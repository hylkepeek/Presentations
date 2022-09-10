param (
        $pipelineName, 
        $stageOrder, 
        $datasetName, 
        $reportName, 
        $dashboardName
      )
#Fill in these values before starting the scirpt
    $tenantId = "[your tenant id]"
    $appId = "[app id of your service principal]"
    $appPassword = "[password of your service principal]"
    $workspaceTest = "[test workspace name]"
    $workspaceProd = "[production workspace name]"


#You need this module!!
    Install-Module MicrosoftPowerBIMgmt -force

# Determine workspace name based on parameter
    $DeployToWorkspace = ""
    if ($stageOrder -eq 0)
    {
        $DeployToWorkspace = $workspaceTest
    }
    elseif ($stageOrder -eq 1)
    {
        $DeployToWorkspace = $workspaceProd
    }

    "DeployToWorkspace: {0}" -f $DeployToWorkspace

#Login to Power BI
    Write-Host "Start login"
    $applicationId = $appId;
    $securePassword = $appPassword | ConvertTo-SecureString -AsPlainText -Force
    $credential = New-Object -TypeName System.Management.Automation.PSCredential -ArgumentList $applicationId, $securePassword
    Connect-PowerBIServiceAccount -ServicePrincipal -Credential $credential -TenantId $tenantId
    Write-Host "Login succes"

try {
    # Get pipelines
    $pipelines = (Invoke-PowerBIRestMethod -Url "pipelines"  -Method Get | ConvertFrom-Json).value
    $pipeline = $pipelines | Where-Object {$_.DisplayName -eq $pipelineName}

    "pipelineName: {0}" -f $pipelineName

    # Get pipeline stage artifacts
    $artifactsUrl = "pipelines/{0}/stages/{1}/artifacts" -f $pipeline.Id,$stageOrder
    $artifacts = Invoke-PowerBIRestMethod -Url $artifactsUrl  -Method Get | ConvertFrom-Json

    $dataset = $artifacts.datasets | Where-Object {$_.artifactDisplayName -eq $datasetName}
    $report = $artifacts.reports | Where-Object {$_.artifactDisplayName -eq $reportName}
    $dashboard = $artifacts.dashboards | Where-Object {$_.artifactDisplayName -eq $dashboardName}

    # Construct the request url and body
    $url = "pipelines/{0}/Deploy" -f $pipeline.Id

    $body = @{ 
        sourceStageOrder = $stageOrder

        datasets = @(
            @{sourceId = $dataset.artifactId }
        )      
        reports = @(
            @{sourceId = $report.artifactId }
        )        
        dashboards = @(
            @{sourceId = $dashboard.artifactId }
        )

        options = @{
            # Allows creating new artifact if needed on the Test stage workspace
            allowCreateArtifact = $TRUE

            # Allows overwriting existing artifact if needed on the Test stage workspace
            allowOverwriteArtifact = $TRUE
        }
    } | ConvertTo-Json

    "url: {0}" -f $url
    "body: {0}" -f $body

    # Send the request
    $deployResult = Invoke-PowerBIRestMethod -Url $url  -Method Post -Body $body | ConvertFrom-Json
    "Operation ID: {0}" -f $deployResult.id

    #Get WorkspaceID
    $groups = (Invoke-PowerBIRestMethod -Url "groups" -Method Get -Body $body | ConvertFrom-Json).value
    $group = $groups | Where-Object {$_.name -eq $DeployToWorkspace}

    #Refresh dataset
    $body = @{"notifyOption"="MailOnFailure"}
    $url = "groups/{0}/datasets/{1}/refreshes" -f $group.Id, $dataset.targetArtifactId

    "dataset: {0}" -f $dataset
    "url: {0}" -f $url

    # Send the request
    $processResult = Invoke-PowerBIRestMethod -Url $url -Method Post -Body $body | ConvertFrom-Json
    "Refressprocess ID: {0}" -f $processResult.id


} catch {
    $errmsg = Resolve-PowerBIError -Last
    $errmsg.Message
}
