Imports System.Runtime.Serialization

Namespace GateException





    <Serializable()>
    Public Class SqlGateException
        Inherits Exception







        Public Sub New(pMessage As String, Optional ByVal pInner As Exception = Nothing)
            MyBase.New(pMessage, pInner)
        End Sub

        Protected Sub New(pInfo As SerializationInfo, pContext As StreamingContext)
            MyBase.New(pInfo, pContext)
        End Sub

    End Class

End Namespace
