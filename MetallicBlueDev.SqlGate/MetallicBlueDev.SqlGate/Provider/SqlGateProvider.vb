Imports System.Data.Common
Imports System.Threading
Imports log4net
Imports MetallicBlueDev.SqlGate.Gate
Imports MetallicBlueDev.SqlGate.GateException

Namespace Provider

    <Serializable()>
    Friend NotInheritable Class SqlGateProvider
        Implements IDisposable

        Protected Shared ReadOnly Logger As ILog = LogManager.GetLogger(Reflection.MethodBase.GetCurrentMethod().DeclaringType)


        <NonSerialized()>
        Private mConnection As IDbConnection = Nothing

        <NonSerialized()>
        Private mCommand As IDbCommand = Nothing

        <NonSerialized()>
        Private mLastCommand As IDbCommand = Nothing

        Private mDisposed As Boolean = False

        Private ReadOnly mConnector As ISqlGate

        Private mProviderFactory As DbProviderFactory = Nothing
        Private mDataAdapter As IDbDataAdapter = Nothing
        Private mTransaction As IDbTransaction = Nothing

        Friend Sub New(pConnector As ISqlGate)
            mConnector = pConnector
        End Sub

        Public Sub Dispose() Implements IDisposable.Dispose
            If Not mDisposed Then
                FreeMemory()
                mDisposed = True
            End If
        End Sub

        Friend Sub SetConnection(pConnection As IDbConnection)
            mConnection = pConnection

            CheckConnectionConfiguration()
        End Sub

        Friend Sub CheckConnectionConfiguration()
            If mConnector.Configuration.Updated Then
                mConnector.Configuration.Update(mConnection)
            End If
        End Sub

        Friend Function CanUseConnection() As Boolean
            Return Not mConnection Is Nothing
        End Function

        Friend Function GetConnection() As IDbConnection
            Return mConnection
        End Function

        Friend Function GetCommand() As IDbCommand
            If mCommand Is Nothing Then
                mCommand = CreateCommand()
            End If

            If Not mCommand Is Nothing Then
                If mCommand.CommandText <> mConnector.SqlStatement Then
                    mCommand.CommandText = mConnector.SqlStatement
                End If

                If mCommand.Parameters.Count > 0 Then
                    mCommand.Parameters.Clear()
                End If

                mCommand.CommandTimeout = mConnector.Configuration.Timeout

                OnPrepareCommand(mCommand)
            End If

            mLastCommand = mCommand

            Return mCommand
        End Function




        Friend Sub CancelLastCommand()
            If Not mLastCommand Is Nothing Then
                mLastCommand.Cancel()
            End If
        End Sub

        Friend Sub Initialize()
            If mProviderFactory Is Nothing Then
                mProviderFactory = DbProviderFactories.GetFactory("System.Data.SqlClient")
            End If

            If Not CanUseConnection() Then
                SetConnection(mProviderFactory.CreateConnection())
            End If

            CheckConnectionConfiguration()
        End Sub

        Friend Sub PreparingNextAttempt()
            mDisposed = False

            If mConnector.NumberOfAttempts > 0 _
               AndAlso Not mTransaction Is Nothing _
               AndAlso Connected() Then
                If Not mConnector.AllowedSaving Then
                    BeginNewTransaction(mConnector.Configuration.Isolation)
                End If
            End If

            Reconnect()
        End Sub

        Friend Sub EndAttempt()
            Close()
            mLastCommand = Nothing
            mTransaction = Nothing
        End Sub

        Friend Sub FreeMemory()
            If Not mProviderFactory Is Nothing Then
                mProviderFactory = Nothing
            End If

            SafeDispose(mTransaction)
            mTransaction = Nothing

            Close()

            SafeDispose(mCommand)
            SafeDispose(mLastCommand)

            mConnection = Nothing
            mCommand = Nothing
            mLastCommand = Nothing
        End Sub

        Friend Function BeginNewTransaction(pTransactionIsolation As IsolationLevel) As IDbTransaction
            Reconnect()

            If Connected() Then
                ValidTransaction()

                If CanUseConnection() Then
                    mTransaction = GetConnection().BeginTransaction(pTransactionIsolation)
                End If
            Else
                Logger.Warn("Failed to create new transaction.")

                mTransaction = Nothing
            End If

            Return mTransaction
        End Function

        Friend Sub ValidTransaction()
            If Not mTransaction Is Nothing Then
                Try
                    mTransaction.Commit()
                Catch ex As System.Exception
                    Logger.Error("Failed to commit transaction.", ex)

                    Try
                        mTransaction.Rollback()

                        Logger.Info("The transaction was rolled back successfully.")
                    Catch e As System.Exception
                        Logger.Warn("Failed to roll back the transaction.", e)
                    End Try
                End Try

                SafeDispose(mTransaction)
                mTransaction = Nothing
            End If
        End Sub

        Friend Function GetNewParameter() As IDataParameter
            Return CreateParameter()
        End Function

        Friend Function GetDataAdapter() As IDbDataAdapter
            Dim currentSqlCommand As IDbCommand = GetCommand()

            If mDataAdapter Is Nothing Then
                mDataAdapter = CreateDataAdapter(currentSqlCommand)
            Else
                UpdateDataAdapter(mDataAdapter, currentSqlCommand)
            End If

            SetLastCommand(mDataAdapter.SelectCommand)

            Return mDataAdapter
        End Function

        Protected Sub OnPrepareCommand(pDbCommand As IDbCommand)
            If mTransaction Is Nothing Then
                If Not mConnector.AllowedSaving Then
                    BeginNewTransaction(mConnector.Configuration.Isolation)
                End If
            End If

            If Not mTransaction Is Nothing Then
                pDbCommand.Transaction = mTransaction
            End If

            If mConnector.IsStoredProcedure Then
                If pDbCommand.CommandType <> CommandType.StoredProcedure Then
                    pDbCommand.CommandType = CommandType.StoredProcedure
                End If
            Else
                If pDbCommand.CommandType <> CommandType.Text Then
                    pDbCommand.CommandType = CommandType.Text
                End If
            End If

            If mConnector.Parameters IsNot Nothing Then
                For Each parameter As IDataParameter In mConnector.Parameters
                    pDbCommand.Parameters.Add(parameter)
                Next
            End If
        End Sub


        Protected Sub SetLastCommand(pLastCommand As IDbCommand)
            mLastCommand = pLastCommand
        End Sub

        Protected Function CreateCommand() As IDbCommand
            Dim newCommand As IDbCommand = Nothing

            If Not mConnection Is Nothing Then
                newCommand = mConnection.CreateCommand()
                newCommand.CommandType = CommandType.Text
            End If

            Return newCommand
        End Function

        Protected Sub Close()
            If Not mConnection Is Nothing Then
                Try
                    mConnection.Close()
                Catch ex As System.Exception
                    If mConnector.CanUseLogging Then
                        Logger.Error("Error on closing connection.", ex)
                    End If
                End Try

                SafeDispose(mConnection)

                mConnector.Configuration.ConfigurationUpdated()
            End If
        End Sub

        Protected Function Connected() As Boolean
            Dim isConnected As Boolean = False

            If Not mConnection Is Nothing Then
                Select Case mConnection.State
                    Case ConnectionState.Executing,
                      ConnectionState.Fetching,
                      ConnectionState.Open
                        isConnected = True
                End Select
            End If

            Return isConnected
        End Function

        Protected Sub SafeDispose(pObject As IDisposable)
            If Not pObject Is Nothing Then
                Try
                    pObject.Dispose()
                Catch ex As System.Exception
                    If mConnector.CanUseLogging Then
                        Logger.Error("Dispose error " & pObject.ToString() & ".", ex)
                    End If
                End Try
            End If
        End Sub

        Private Sub Open()
            If Not Connected() Then
                Try
                    Initialize()

                    For attemptNumber As Integer = 0 To mConnector.Configuration.MaximumNumberOfAttempts
                        If GetConnection().State <> ConnectionState.Open Then
                            GetConnection().Open()
                        End If

                        If Connected() _
                           OrElse GetConnection().State <> ConnectionState.Broken Then
                            Exit For
                        Else
                            Close()
                        End If

                        Logger.WarnFormat("Connection lost...{0}", attemptNumber.ToString())

                        Thread.Sleep(mConnector.Configuration.AttemptDelay)
                    Next
                Catch ex As System.Exception
                    Dim exp As New SqlGateException("Connection error.", ex)
                    Logger.Fatal("Unable to connect to the database.", exp)
                    Throw exp
                End Try
            End If
        End Sub

        Private Sub Reconnect()
            If Not Connected() Then
                Close()

                Try
                    Open()
                Catch ex As System.Exception
                    If mConnector.CanUseLogging Then
                        Logger.Error("Open error.", ex)
                    End If
                End Try
            End If
        End Sub

        Private Function CreateParameter() As IDataParameter
            Dim newParameter As IDataParameter = Nothing

            If Not mProviderFactory Is Nothing Then
                newParameter = mProviderFactory.CreateParameter()
            End If

            Return newParameter
        End Function

        Private Function CreateDataAdapter(pCurrentSqlCommand As IDbCommand) As IDbDataAdapter
            Dim newDataAdapter As IDbDataAdapter = Nothing

            If Not mProviderFactory Is Nothing Then
                newDataAdapter = mProviderFactory.CreateDataAdapter()
            End If

            UpdateDataAdapter(newDataAdapter, pCurrentSqlCommand)

            Return newDataAdapter
        End Function

        Private Sub UpdateDataAdapter(pCurrentDataAdapter As IDbDataAdapter, pCurrentSqlCommand As IDbCommand)
            If Not pCurrentDataAdapter Is Nothing _
               AndAlso Not pCurrentSqlCommand Is Nothing _
               AndAlso Not pCurrentDataAdapter.SelectCommand Is pCurrentSqlCommand Then
                If String.IsNullOrEmpty(pCurrentSqlCommand.CommandText) _
                   OrElse (Not mConnector.IsStoredProcedure AndAlso Not pCurrentSqlCommand.CommandText.ToUpper().Contains("SELECT")) Then
                    Dim exp As New SqlGateException("You must specify a SELECT query before")
                    Logger.Fatal("You must specify a SELECT query before.", exp)
                    Throw exp
                End If

                pCurrentDataAdapter.SelectCommand = CopyCommand(pCurrentSqlCommand)

                If Not mProviderFactory Is Nothing _
                   AndAlso mConnector.AllowedSaving Then
                    Dim sqlCommandBuilder As DbCommandBuilder = mProviderFactory.CreateCommandBuilder()
                    sqlCommandBuilder.DataAdapter = DirectCast(pCurrentDataAdapter, DbDataAdapter)

                    pCurrentDataAdapter.InsertCommand = sqlCommandBuilder.GetInsertCommand()
                    pCurrentDataAdapter.UpdateCommand = sqlCommandBuilder.GetUpdateCommand()
                    pCurrentDataAdapter.DeleteCommand = sqlCommandBuilder.GetDeleteCommand()
                End If
            End If
        End Sub

        Private Function CopyCommand(pModelCommand As IDbCommand) As IDbCommand
            Dim newCommand As IDbCommand = CreateCommand()

            newCommand.CommandType = pModelCommand.CommandType
            newCommand.CommandText = pModelCommand.CommandText
            newCommand.Transaction = pModelCommand.Transaction
            newCommand.CommandTimeout = pModelCommand.CommandTimeout

            If pModelCommand.Parameters.Count > 0 Then
                For Each parameter As DbParameter In pModelCommand.Parameters
                    newCommand.Parameters.Add(CopyParameter(parameter))
                Next
            End If

            Return newCommand
        End Function

        Private Function CopyParameter(pParameter As IDataParameter) As IDataParameter
            Dim newParameter As IDataParameter = CreateParameter()
            newParameter.ParameterName = pParameter.ParameterName
            newParameter.Value = pParameter.Value
            newParameter.DbType = pParameter.DbType

            Return newParameter
        End Function






    End Class

End Namespace
