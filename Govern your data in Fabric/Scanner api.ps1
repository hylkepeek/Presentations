
#Login to Power BI/Fabric
    Write-Host "Start login"
    $applicationId = "{service principal name}";
    $securePassword = "{service principal secret}" | ConvertTo-SecureString -AsPlainText -Force
    $credential = New-Object -TypeName System.Management.Automation.PSCredential -ArgumentList $applicationId, $securePassword
    Connect-PowerBIServiceAccount -ServicePrincipal -Credential $credential -TenantId "{Tenant ID}" 
    Write-Host "End Login"

#Get list of Workspace IDs
    $url = ”https://api.powerbi.com/v1.0/myorg/admin/workspaces/modified“
    $workspaceids = Invoke-PowerBIRestMethod -Url $url -Method Get | ConvertFrom-Json
#    $workspaceids 
    $workspaces_as_list = $workspaceids.id -join """,
    """
# Make API metadata Scan call
    $body1 =

    ‘{
     workspaces: [
      "'+ "$workspaces_as_list"

    $body = $body1 + '"
    ]
}’

    $url= ” https://api.powerbi.com/v1.0/myorg/admin/workspaces/getInfo?lineage=True&datasourceDetails=True&datasetSchema=True&datasetExpressions=True“

    $getinfo_response = Invoke-PowerBIRestMethod -Url $url -Method Post -Body $body | ConvertFrom-Json

    $scanID = $getinfo_response.id

# $scanID
    $url = ”https://api.powerbi.com/v1.0/myorg/admin/workspaces/scanstatus/” + $scanID
    
#Wait for scan status to be succeeeded
    Start-Sleep -Seconds 5

# Check the scan Status
    $scanstatus_response = Invoke-PowerBIRestMethod -Url $url -Method Get | ConvertFrom-Json

    $scanstatus = $scanstatus_response.status

    if($scanstatus -eq "Succeeded")
    {
        #Get the scan result for the specified scan
        $outputfilepath = "{local file path}/scanresult.json"
        $url = ” https://api.powerbi.com/v1.0/myorg/admin/workspaces/scanResult/” + $scanID
        $t = Invoke-PowerBIRestMethod -Url $url -Method Get
        $t | Out-File -FilePath $outputfilepath
        $return = "Request succeeded. Output is written to $outputfilepath."
    }
    else
    {
        $return = "Request cancelled. It takes to long before the scan is ready to read."
    }

     $return

#Distconnect from Power BI
    Disconnect-PowerBIServiceAccount