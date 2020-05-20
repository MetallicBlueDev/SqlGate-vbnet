Imports System.Data.SqlClient
Imports log4net
Imports MetallicBlueDev.SqlGate.Extensions
Imports MetallicBlueDev.SqlGate.Gate

Namespace Helpers

    Friend Class GateHelper

        Protected Shared ReadOnly Logger As ILog = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType)

        Friend Shared Sub ExceptionMarker(pEx As System.Exception, pConnector As ISqlGate)
            Dim sqlEx As SqlException = GetSqlException(pEx)
            Dim lastQuery As String

            If pConnector.SqlStatement.IsNotNullOrEmpty() Then
                lastQuery = pConnector.SqlStatement
            Else
                lastQuery = "sorry, unknown query"
            End If

            If Not sqlEx Is Nothing Then
                For Each currentError As SqlError In sqlEx.Errors
                    Logger.FatalFormat(
                      "Sql message: {0}." &
                        Environment.NewLine & "LineNumber: {1}" &
                        Environment.NewLine & "Source: {2}" &
                        Environment.NewLine & "Procedure: {3}" &
                        Environment.NewLine & "Sql server error class: {4}" &
                        Environment.NewLine & "Sql server error number: {5}" &
                        Environment.NewLine & "Query: {6}",
                      currentError.Message,
                      currentError.LineNumber,
                      currentError.Source,
                      currentError.Procedure,
                      currentError.Class,
                      currentError.Number,
                      lastQuery
                      )
                Next
            Else
                Logger.FatalFormat("Failed to execute last token: {0}.", lastQuery)
            End If
        End Sub


        Private Shared Function GetSqlException(pEx As System.Exception) As SqlException
            Dim sqlEx As SqlException = Nothing

            If Not pEx Is Nothing Then
                If Not (TypeOf pEx Is SqlException) Then
                    While Not pEx.InnerException Is Nothing
                        If TypeOf pEx Is SqlException Then
                            Exit While
                        End If

                        pEx = pEx.InnerException
                    End While
                End If

                If TypeOf pEx Is SqlException Then
                    sqlEx = DirectCast(pEx, SqlException)
                End If
            End If

            Return sqlEx
        End Function

    End Class

End Namespace
