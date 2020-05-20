Imports System.Configuration
Imports System.Xml

Namespace Configuration

    Public Class EntityGateSectionHandler
        Implements IConfigurationSectionHandler

        Public Function Create(parent As Object, configContext As Object, section As XmlNode) As Object Implements IConfigurationSectionHandler.Create
            If Not GlobalConfiguration.Initialized() Then
                GlobalConfiguration.Create(section)
            End If

            Return GlobalConfiguration.GetConfigs()
        End Function

    End Class

End Namespace
