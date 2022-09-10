param (
        $pipelineName, 
        $stageOrder
      )

#Fill in these values before starting the scirpt
    $tenantId = "[your tenant id]"
    $appId = "[app id of your service principal]"
    $appPassword = "[password of your service principal]"
    $workspaceTest = "[test workspace name]"
    $workspaceProd = "[production workspace name]"


#You need this module!!
    Install-Module MicrosoftPowerBIMgmt -force

    $DeployToWorkspace = ""
    if ($stageOrder -eq 0)
    {
        $DeployToWorkspace = "[test workspace name]"
    }
    elseif ($stageOrder -eq 1)
    {
        $DeployToWorkspace = "[production workspace name]"
    }

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

    if(!$pipeline) {
        Write-Host "A pipeline with the requested name was not found"
        return
    }

    # Construct the request url and body
    $url = "pipelines/{0}/DeployAll" -f $pipeline.Id
    
    $body = @{ 
        sourceStageOrder = $stageOrder

        options = @{
            # Allows creating new artifact if needed on the Test stage workspace
            allowCreateArtifact = $TRUE

            # Allows overwriting existing artifact if needed on the Test stage workspace
            allowOverwriteArtifact = $TRUE
        }
    } | ConvertTo-Json

# Send the request
    $deployResult = Invoke-PowerBIRestMethod -Url $url  -Method Post -Body $body | ConvertFrom-Json

    "Deploymentprocess ID: {0}" -f $deployResult.id


# Refresh datasets

    #Get WorkspaceID
    $groups = (Invoke-PowerBIRestMethod -Url "groups" -Method Get -Body $body | ConvertFrom-Json).value
    $group = $groups | Where-Object {$_.name -eq $DeployToWorkspace}
    
    #Get all datasets
    $url = "groups/{0}/datasets" -f $group.Id

    $datasets = (Invoke-PowerBIRestMethod -Url $url -Method Get | ConvertFrom-Json).value
    foreach ($ds in $datasets)
    {
        $url = "groups/{0}/datasets/{1}/refreshes" -f $group.Id, $ds.id
        $body = @{"notifyOption"="MailOnFailure"}

        #$url
        $processResult = Invoke-PowerBIRestMethod -Url $url -Method Post -Body $body | ConvertFrom-Json
        "Refressprocess ID: {0}" -f $processResult.id
        Write-Host $body
    }

} catch {
    $errmsg = Resolve-PowerBIError -Last
    $errmsg.Message
}

