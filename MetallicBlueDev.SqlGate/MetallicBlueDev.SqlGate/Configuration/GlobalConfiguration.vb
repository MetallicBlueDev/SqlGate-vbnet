Imports System.Configuration
Imports System.Xml
Imports log4net
Imports MetallicBlueDev.SqlGate.Extensions

Namespace Configuration

    Friend Class GlobalConfiguration

        Private Shared ReadOnly Logger As ILog = LogManager.GetLogger(Reflection.MethodBase.GetCurrentMethod().DeclaringType)

        Private Shared ReadOnly Locker As Object = New Object()
        Private Shared mConfigs As DataSetConfiguration.SqlGateConfigDataTable = Nothing

        Friend Shared Sub Create(section As XmlNode)
            SyncLock Locker
                Try
                    mConfigs = New DataSetConfiguration.SqlGateConfigDataTable()

                    LoadConfigs(section)
                Catch ex As Exception
                    Logger.Error("Invalid configuration.", ex)
                End Try
            End SyncLock
        End Sub





        Friend Shared Function Initialized() As Boolean
            SyncLock Locker
                Return mConfigs IsNot Nothing
            End SyncLock
        End Function






        Friend Shared Function GetConfigs() As DataSetConfiguration.SqlGateConfigDataTable
            SyncLock Locker
                Return mConfigs
            End SyncLock
        End Function






        Friend Shared Function GetFirstConfig() As DataSetConfiguration.SqlGateConfigRow
            SyncLock Locker
                Dim rslt As DataSetConfiguration.SqlGateConfigRow = Nothing

                If mConfigs IsNot Nothing Then
                    rslt = mConfigs.FirstOrDefault()
                End If

                Return rslt
            End SyncLock
        End Function






        Friend Shared Function GetConfig(connectionName As String) As DataSetConfiguration.SqlGateConfigRow
            SyncLock Locker
                Dim rslt As DataSetConfiguration.SqlGateConfigRow = Nothing

                If mConfigs IsNot Nothing Then
                    rslt = mConfigs.FirstOrDefault(Function(row) row.ConnectionName.EqualsIgnoreCase(connectionName))
                End If

                Return rslt
            End SyncLock
        End Function







        Friend Shared Function GetConnectionString(pConnectionName As String) As String
            Dim result As String = Nothing

            Try
                If ConfigurationManager.ConnectionStrings.Count > 0 Then
                    Dim config As ConnectionStringSettings = ConfigurationManager.ConnectionStrings.Item(pConnectionName)

                    If config IsNot Nothing Then
                        result = config.ConnectionString
                    End If
                End If
            Catch ex As Exception
                Logger.Error("Invalid connection name ('" & pConnectionName & "') or 'ConnectionStrings' is corrupt.", ex)
            End Try

            If String.IsNullOrEmpty(result) Then
                Logger.ErrorFormat("No data found for connection name '{0}'. Failed to configure default connection string.", pConnectionName)
            End If

            Return result
        End Function





        Private Shared Sub LoadConfigs(section As XmlNode)
            For Each childNode As XmlNode In section.ChildNodes.Cast(Of XmlNode)().Where(Function(node) node.Name.EqualsIgnoreCase("EntityGateConfig"))
                Dim configRow As DataSetConfiguration.SqlGateConfigRow = mConfigs.NewSqlGateConfigRow()

                LoadConfig(childNode, configRow)

                mConfigs.AddSqlGateConfigRow(configRow)
            Next
        End Sub






        Private Shared Sub LoadConfig(childNode As XmlNode, configRow As DataSetConfiguration.SqlGateConfigRow)
            For Each col As DataColumn In mConfigs.Columns
                Dim colNode As XmlNode = childNode.SelectSingleNode(col.ColumnName)

                If colNode IsNot Nothing Then
                    configRow(col) = colNode.InnerText
                Else
                    Logger.ErrorFormat("Column not found: ", col.ColumnName)
                End If
            Next
        End Sub

    End Class

End Namespace
