Imports System.Reflection
Imports log4net
Imports MetallicBlueDev.SqlGate.Extensions
Imports MetallicBlueDev.SqlGate.Gate

Namespace Configuration

    <Serializable()>
    Public NotInheritable Class ClientConfiguration

        Private Shared ReadOnly Logger As ILog = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType)

        Private ReadOnly mGate As ISqlGate

        Private mUpdated As Boolean = False
        Private mMaximumNumberOfAttempts As Integer = 0
        Private mAttemptDelay As Integer = 0
        Private mConnectionString As String = Nothing
        Private mTimeout As Integer = -1
        Private mIsolation As IsolationLevel = IsolationLevel.Unspecified

        Public ReadOnly Property Updated As Boolean
            Get
                Return mUpdated
            End Get
        End Property

        Public Property MaximumNumberOfAttempts As Integer
            Get
                Return mMaximumNumberOfAttempts
            End Get
            Set(value As Integer)
                If value >= 0 Then
                    mMaximumNumberOfAttempts = value
                    ConfigurationUpdated()
                End If
            End Set
        End Property

        Public Property AttemptDelay As Integer
            Get
                Return mAttemptDelay
            End Get
            Set(value As Integer)
                If value >= 0 Then
                    mAttemptDelay = value
                    ConfigurationUpdated()
                End If
            End Set
        End Property

        Public Property Isolation As IsolationLevel
            Get
                Return mIsolation
            End Get
            Set(value As IsolationLevel)
                mIsolation = value
                ConfigurationUpdated()
            End Set
        End Property

        Public Property ConnectionString As String
            Get
                Return mConnectionString
            End Get
            Set(value As String)
                If value.IsNotNullOrEmpty() Then
                    mConnectionString = value
                    ConfigurationUpdated()
                End If
            End Set
        End Property

        Public Property Timeout As Integer
            Get
                Return GetCurrentTimeout()
            End Get
            Set(value As Integer)
                If value >= 0 Then
                    mTimeout = value
                    ConfigurationUpdated()
                End If
            End Set
        End Property

        Friend Sub New(pConnector As ISqlGate)
            mGate = pConnector

            Dim defaultConfig As DataSetConfiguration.SqlGateConfigRow = GlobalConfiguration.GetFirstConfig()

            If defaultConfig IsNot Nothing Then
                ChangeConnectionString(defaultConfig.ConnectionName)
            End If
        End Sub


        Public Sub ChangeConnectionString(pConnectionName As String)
            Dim currentConfig As DataSetConfiguration.SqlGateConfigRow = GlobalConfiguration.GetConfig(pConnectionName)

            If currentConfig IsNot Nothing Then
                MaximumNumberOfAttempts = currentConfig.MaximumNumberOfAttempts
                AttemptDelay = currentConfig.AttemptDelay
                Timeout = currentConfig.Timeout
                Isolation = CType(currentConfig.IsolationLevel, IsolationLevel)
                ConnectionString = GlobalConfiguration.GetConnectionString(currentConfig.ConnectionName)
            ElseIf mGate.CanUseLogging Then
                Logger.WarnFormat("Unable to find connectionString '{0}'.", pConnectionName)
            End If
        End Sub

        Friend Sub Update(pConnection As IDbConnection)
            pConnection.ConnectionString = ConnectionString

            ConfigurationUpToDate()
        End Sub

        Friend Sub ConfigurationUpdated()
            mUpdated = True
        End Sub

        Private Sub ConfigurationUpToDate()
            mUpdated = False
        End Sub

        Private Function GetCurrentTimeout() As Integer
            Dim connectorTimeOut As Integer = mTimeout

            If Not (connectorTimeOut > 1) Then
                If mGate.NumberOfAttempts > 0 Then
                    connectorTimeOut *= mGate.NumberOfAttempts
                End If

                If connectorTimeOut > 120 Then
                    connectorTimeOut = 120
                End If
            End If

            Return connectorTimeOut
        End Function

    End Class

End Namespace
