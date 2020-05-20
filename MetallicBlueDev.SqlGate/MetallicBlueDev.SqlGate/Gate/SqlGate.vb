Imports System.Data.Common
Imports System.Data.SqlClient
Imports System.Threading
Imports log4net
Imports MetallicBlueDev.SqlGate.Configuration
Imports MetallicBlueDev.SqlGate.Extensions
Imports MetallicBlueDev.SqlGate.GateException
Imports MetallicBlueDev.SqlGate.Helpers
Imports MetallicBlueDev.SqlGate.Provider

Namespace Gate

    <Serializable()>
    Public NotInheritable Class SqlGate
        Implements ISqlGate

        Protected Shared ReadOnly Logger As ILog = LogManager.GetLogger(Reflection.MethodBase.GetCurrentMethod().DeclaringType)

        Private mProvider As SqlGateProvider = Nothing
        Private mCanceled As Boolean = False
        Private mDisposed As Boolean = False
        Private mNumberOfAttempts As Integer = 0
        Private mNumberOfRows As Integer = -1
        Private mSqlStatement As String = Nothing
        Private mCanUseLogging As Boolean = False

        Private Const ConnectorTypeName As String = "Sql"
        Private Const ClientName As String = "System.Data.SqlClient"

        Private mParameters As IList(Of IDataParameter) = Nothing
        Private mResult As DataTable = Nothing

        Public ReadOnly Property Configuration As ClientConfiguration Implements ISqlGate.Configuration

        Public ReadOnly Property NumberOfAttempts As Integer Implements ISqlGate.NumberOfAttempts
            Get
                Return mNumberOfAttempts
            End Get
        End Property

        Public ReadOnly Property NumberOfRows As Integer Implements ISqlGate.NumberOfRows
            Get
                Return mNumberOfRows
            End Get
        End Property

        Public Property SqlStatement As String Implements ISqlGate.SqlStatement
            Get
                Dim query As String = mSqlStatement

                If query Is Nothing Then
                    query = String.Empty
                End If

                Return query
            End Get
            Set(value As String)
                mSqlStatement = value
            End Set
        End Property

        Public Property AllowedSaving As Boolean Implements ISqlGate.AllowedSaving

        Public Property CanThrowException As Boolean = True Implements ISqlGate.CanThrowException

        Public Property CanUseLogging As Boolean Implements ISqlGate.CanUseLogging
            Get
                Return mCanUseLogging OrElse Logger.IsDebugEnabled
            End Get
            Set(value As Boolean)
                mCanUseLogging = value
            End Set
        End Property

        Public Property CanUseNotification As Boolean = True Implements ISqlGate.CanUseNotification

        Public Property WithKeyInfo As Boolean = False

        Public Property IsStoredProcedure As Boolean = False Implements ISqlGate.IsStoredProcedure

        Public ReadOnly Property Result As DataTable
            Get
                Dim dataResult As DataTable = mResult

                If dataResult Is Nothing Then
                    dataResult = New DataTable()
                End If

                Return dataResult
            End Get
        End Property

        Public ReadOnly Property Parameters As IList(Of IDataParameter) Implements ISqlGate.Parameters
            Get
                If mParameters Is Nothing Then
                    mParameters = New List(Of IDataParameter)()
                End If

                Return mParameters
            End Get
        End Property

        Public Sub New(Optional ByVal pConnectionName As String = Nothing)
            Configuration = New ClientConfiguration(Me)

            CanUseLogging = True

            If pConnectionName.IsNotNullOrEmpty() Then
                Configuration.ChangeConnectionString(pConnectionName)
            End If
        End Sub

        Public Function BeginNewTransaction(Optional ByVal pTransactionIsolation As IsolationLevel = IsolationLevel.ReadCommitted) As IDbTransaction
            CreateProvider()
            Return mProvider.BeginNewTransaction(pTransactionIsolation)
        End Function

        Public Sub ClearParameters()
            If Not mParameters Is Nothing Then
                mParameters.Clear()
                mParameters = Nothing
            End If
        End Sub

        Public Function GetNewParameter() As IDataParameter
            CreateProvider()
            Return mProvider.GetNewParameter()
        End Function

        Public Sub CancelLastCommand() Implements ISqlGate.CancelLastCommand
            If Not mCanceled Then
                If CanUseLogging Then
                    Logger.Warn("Canceling last command...")
                End If

                mProvider.CancelLastCommand()
                mCanceled = True
            End If
        End Sub

        Public Sub Dispose() Implements IDisposable.Dispose
            If Not mDisposed Then
                DestroyProvider()
                mDisposed = True
            End If
        End Sub

        Public Sub Execute()
            ExecutionStart()
            Dim sqlDataReader As IDataReader = Nothing

            Try
                While ExecutionAllowed(Nothing)
                    If ExecuteSql(sqlDataReader) Then
                        Exit While
                    End If
                End While
            Catch ex As System.Exception
                If CanThrowException Then
                    Throw
                End If
            Finally
                FinalizeTransaction(sqlDataReader)
            End Try
        End Sub

        Public Sub Execute(pRawDataSet As DataSet, Optional ByVal pTableName As String = Nothing)
            ExecutionStart()
            Dim sqlDataAdapter As DbDataAdapter = Nothing

            Try
                While ExecutionAllowed(Nothing)
                    sqlDataAdapter = DirectCast(mProvider.GetDataAdapter(), DbDataAdapter)

                    If Not sqlDataAdapter Is Nothing _
                      AndAlso ExecuteDataSet(pTableName, pRawDataSet, sqlDataAdapter) Then
                        Exit While
                    End If
                End While
            Catch ex As System.Exception
                If CanThrowException Then
                    Throw
                End If
            Finally
                FinalizeTransaction(sqlDataAdapter)
            End Try

            AffectDataSetResult(pTableName, pRawDataSet)
        End Sub

        Public Sub Execute(pRawDataTable As DataTable)
            Execute(pRawDataTable.DataSet, pRawDataTable.TableName)
        End Sub

        Public Sub AddNewParameter(pParameterName As String, pTypeParameter As DbType, pParameterValue As Object, Optional ByVal pIsOutParameter As Boolean = False)
            Dim sqlParameter As IDataParameter = GetNewParameter()
            sqlParameter.ParameterName = pParameterName.Replace("@", String.Empty)
            sqlParameter.DbType = pTypeParameter
            sqlParameter.Value = pParameterValue

            If pIsOutParameter Then
                sqlParameter.Direction = ParameterDirection.Output
            End If

            Parameters.Add(sqlParameter)
        End Sub

        Public Function GetParameterValue(pParameterName As String) As Object
            Dim parameterValue As Object = Nothing
            Dim parameter As IDataParameter = Parameters.Where(Function(pPData) pPData.ParameterName = pParameterName).DefaultIfEmpty(Nothing).FirstOrDefault()

            If Not parameter Is Nothing Then
                parameterValue = parameter.Value
            End If

            Return parameterValue
        End Function

        Private Sub SetNumberOfRows(pNumberOfRows As Integer)
            mNumberOfRows = pNumberOfRows
        End Sub

        Private Function CanUseProvider() As Boolean
            Return Not mProvider Is Nothing
        End Function

        Private Sub CreateProvider()
            mDisposed = False

            CheckProvider()
        End Sub

        Private Function ExecutionAllowed(pEx As System.Exception, Optional ByVal pForRetry As Boolean = True) As Boolean
            If IsInvalidQuery(pEx) Then
                GateHelper.ExceptionMarker(pEx, Me)
                Throw pEx
            End If

            Dim isAllowed As Boolean = mNumberOfAttempts < Configuration.MaximumNumberOfAttempts

            If Not isAllowed _
              OrElse mCanceled Then
                ThrowStopExecution(pEx)
            Else
                If pForRetry Then
                    PreparingExecution()
                End If
            End If

            Return isAllowed
        End Function

        Private Function IsInvalidQuery(pEx As Exception) As Boolean
            Dim rslt As Boolean = False

            If pEx IsNot Nothing _
              AndAlso TypeOf pEx Is SqlException Then
                Select Case DirectCast(pEx, SqlException).Number
                    Case 102, 107, 170, 207, 208, 242, 547, 2705, 2812, 3621, 8152
                        rslt = True
                End Select
            End If

            Return rslt
        End Function

        Private Sub PreparingNextAttempt()
            If NumberOfAttempts > 0 _
              AndAlso CanUseLogging Then
                Logger.WarnFormat("Retrying execution of the command ({0}/{1}: {2} seconds).", NumberOfAttempts, Configuration.MaximumNumberOfAttempts, Configuration.Timeout)
                Logger.InfoFormat("Retry {0}", GetQueryInfo())
            End If

            mProvider.PreparingNextAttempt()
        End Sub

        Private Sub ManageNotification()
            If CanPublishEvent() Then
                PublishEvent()
            End If
        End Sub

        Private Sub ExecutionStart()
            mNumberOfAttempts = 0
            mCanceled = False
            mNumberOfRows = -1
            mResult = Nothing

            CreateProvider()
        End Sub

        Private Sub ExecutionEnd()
            mProvider.EndAttempt()

            ClearReadOnlyColumn()
            ManageNotification()

            DestroyProvider()
        End Sub

        Private Function GetNewProvider() As SqlGateProvider
            Return New SqlGateProvider(Me)
        End Function

        Private Sub DestroyProvider()
            ClearInputParameters()
            If Not mProvider Is Nothing Then
                mProvider.FreeMemory()
                mProvider = Nothing
            End If
        End Sub

        Private Function GetQueryInfo() As String
            Return $"{If(IsStoredProcedure,
                         "stored procedure",
                         "query")}: {SqlStatement}."
        End Function

        Private Sub PublishEvent()
        End Sub

        Private Function CanPublishEvent() As Boolean
            Return CanUseNotification _
                   AndAlso AllowedSaving _
                   AndAlso NumberOfRows > 0
        End Function

        Private Sub MakeProvider()
            mProvider = GetNewProvider()
            mProvider.Initialize()
        End Sub

        Private Sub CheckProvider()
            If Not CanUseProvider() Then
                MakeProvider()
            Else
                mProvider.Initialize()
            End If
        End Sub

        Private Sub ThrowStopExecution(pEx As System.Exception)
            If mCanceled Then
                If Not pEx Is Nothing Then
                    Throw pEx
                Else
                    Throw New TransactionCanceledException("Cancellation of the transaction.", pEx)
                End If
            Else
                GateHelper.ExceptionMarker(pEx, Me)

                Throw New TransactionCanceledException("Unable to execute " & Me.GetType.Name & " command after " & mNumberOfAttempts & " retry.", pEx)
            End If
        End Sub

        Private Sub PreparingExecution()
            If mNumberOfAttempts > 0 Then
                Thread.Sleep(Configuration.AttemptDelay)
            End If

            PreparingNextAttempt()

            mNumberOfAttempts += 1
        End Sub

        Private Sub ClearInputParameters()
            If Not mParameters Is Nothing Then
                For i As Integer = mParameters.Count - 1 To 0 Step -1
                    If mParameters(i).Direction <> ParameterDirection.Input Then
                        Continue For
                    End If

                    mParameters.RemoveAt(i)
                Next

                If mParameters.Count < 1 Then
                    mParameters = Nothing
                End If
            End If
        End Sub

        Private Function ExecuteSql(ByRef pSqlDataReader As IDataReader) As Boolean
            Dim success As Boolean = False
            Dim command As IDbCommand = mProvider.GetCommand()

            If Not command Is Nothing Then
                Try
                    pSqlDataReader = command.ExecuteReader(If(WithKeyInfo, CommandBehavior.KeyInfo, CommandBehavior.Default))

                    AffectDataTableResult(pSqlDataReader)

                    success = True
                Catch ex As System.Exception
                    ExecutionAllowed(ex, False)
                Finally
                    If Not pSqlDataReader Is Nothing Then
                        Try
                            pSqlDataReader.Close()
                        Catch ex As System.Exception
                            If CanUseLogging Then
                                Logger.Error("Failed to close data reader.", ex)
                            End If
                        End Try
                    End If
                End Try
            End If

            Return success
        End Function

        Private Function ExecuteDataSet(pTableName As String, pRawDataSet As DataSet, pSqlDataAdapter As DbDataAdapter) As Boolean
            Dim success As Boolean = False

            Try
                If AllowedSaving Then
                    If pTableName.IsNotNullOrEmpty() Then
                        SetNumberOfRows(pSqlDataAdapter.Update(pRawDataSet, pTableName))
                    Else
                        SetNumberOfRows(pSqlDataAdapter.Update(pRawDataSet))
                    End If
                Else
                    If pTableName.IsNotNullOrEmpty() Then
                        SetNumberOfRows(pSqlDataAdapter.Fill(pRawDataSet, pTableName))
                    Else
                        SetNumberOfRows(pSqlDataAdapter.Fill(pRawDataSet))
                    End If
                End If

                success = True
            Catch ex As System.Exception
                ExecutionAllowed(ex, False)
            End Try

            Return success
        End Function

        Private Sub FinalizeTransaction(pReader As IDisposable)
            mProvider.ValidTransaction()

            If Not pReader Is Nothing Then
                Try
                    pReader.Dispose()
                Catch ex As System.Exception
                    If CanUseLogging Then
                        Logger.Error("Failed to dispose reader.", ex)
                    End If
                End Try
            End If

            ExecutionEnd()
        End Sub

        Private Sub AffectDataTableResult(pSqlDataReader As IDataReader)
            If Not pSqlDataReader Is Nothing Then
                mResult = New DataTable()
                mResult.Load(pSqlDataReader)

                If pSqlDataReader.RecordsAffected > 0 _
                  AndAlso Not mResult.Rows.Count > 0 Then
                    SetNumberOfRows(pSqlDataReader.RecordsAffected)
                Else
                    SetNumberOfRows(mResult.Rows.Count)
                End If
            End If
        End Sub

        Private Sub AffectDataSetResult(pTableName As String, pRawDataSet As DataSet)
            If NumberOfRows >= 0 Then
                If pTableName.IsNotNullOrEmpty() _
                  AndAlso pRawDataSet.Tables.Contains(pTableName) Then
                    mResult = pRawDataSet.Tables(pTableName)
                ElseIf pRawDataSet.Tables.Count > 0 Then
                    mResult = pRawDataSet.Tables(pRawDataSet.Tables.Count - 1)
                End If
            End If
        End Sub

        Private Sub ClearReadOnlyColumn()
            If Not mResult Is Nothing Then
                For Each currentColumn As DataColumn In mResult.Columns
                    If currentColumn.ReadOnly Then
                        currentColumn.ReadOnly = False
                    End If
                Next
            End If
        End Sub

    End Class

End Namespace
