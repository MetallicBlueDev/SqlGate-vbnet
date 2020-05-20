Imports MetallicBlueDev.SqlGate.Configuration

Namespace Gate

  Public Interface ISqlGate
    Inherits IDisposable

    Property IsStoredProcedure As Boolean

    ReadOnly Property Parameters As IList(Of IDataParameter)

    Property CanThrowException As Boolean

    Property CanUseLogging As Boolean

    Property CanUseNotification As Boolean

    ReadOnly Property Configuration As ClientConfiguration

    ReadOnly Property NumberOfAttempts As Integer

    ReadOnly Property NumberOfRows As Integer

    Property SqlStatement As String

    Property AllowedSaving As Boolean

    Sub CancelLastCommand()

  End Interface

End Namespace
