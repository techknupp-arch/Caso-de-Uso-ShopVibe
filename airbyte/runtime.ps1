function Set-AirbyteRuntimeProfile {
    # The same JSON is a valid Helm values file and the source of existing-install adjustments.
    $profile = Get-Content -LiteralPath (Join-Path $PSScriptRoot 'values.json') -Raw | ConvertFrom-Json
    foreach ($component in $profile.PSObject.Properties) {
        $deploymentName = 'airbyte-abctl-' + $component.Name
        $text = docker exec airbyte-abctl-control-plane kubectl get deployment $deploymentName -n airbyte-abctl -o json
        if ($LASTEXITCODE -ne 0) { throw "Deployment Airbyte ausente: $deploymentName" }
        $deployment = ($text -join "`n") | ConvertFrom-Json
        $container = $deployment.spec.template.spec.containers[0]
        $desired = [ordered]@{name=$container.name}
        $needsPatch = $false
        if ($component.Value.env_vars) {
            $desired.env = @()
            foreach ($variable in $component.Value.env_vars.PSObject.Properties) {
                $desired.env += @{name=$variable.Name; value=$variable.Value}
                $existing = @($container.env | Where-Object { $_.name -eq $variable.Name })
                if ($existing.Count -ne 1 -or $existing[0].value -ne $variable.Value) { $needsPatch = $true }
            }
        }
        foreach ($probeName in @('livenessProbe', 'readinessProbe')) {
            if ($component.Value.$probeName) {
                $desired[$probeName] = $component.Value.$probeName
                foreach ($field in $component.Value.$probeName.PSObject.Properties) {
                    if ($container.$probeName.($field.Name) -ne $field.Value) { $needsPatch = $true }
                }
            }
        }
        if ($needsPatch) {
            Write-Host "Ajustando perfil local: $deploymentName"
            $patch = @{spec=@{template=@{spec=@{containers=@($desired)}}}} | ConvertTo-Json -Depth 20 -Compress
            $patch | docker exec -i airbyte-abctl-control-plane kubectl patch deployment $deploymentName -n airbyte-abctl --type strategic --patch-file /dev/stdin
            if ($LASTEXITCODE -ne 0) { throw "Falha ao ajustar $deploymentName" }
        }
        # Sequential rollouts avoid starting all JVMs at once on a local Docker installation.
        docker exec airbyte-abctl-control-plane kubectl rollout status deployment/$deploymentName -n airbyte-abctl --timeout=300s
        if ($LASTEXITCODE -ne 0) { throw "Airbyte nao ficou saudavel: $deploymentName" }
    }
}
