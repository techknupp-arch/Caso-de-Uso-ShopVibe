param([switch]$SkipSync)

$ErrorActionPreference = 'Stop'
Push-Location $PSScriptRoot
try {
    . (Join-Path $PSScriptRoot 'airbyte\runtime.ps1')
    $env:DO_NOT_TRACK = '1'
    $abctlVersion = '0.30.4'
    $chartVersion = '1.8.5'
    $architecture = if ($env:PROCESSOR_ARCHITECTURE -eq 'ARM64') { 'arm64' } else { 'amd64' }
    $toolsPath = Join-Path $PSScriptRoot '.tools\airbyte'
    $runtimePath = Join-Path $PSScriptRoot '.runtime'
    New-Item -ItemType Directory -Path $toolsPath, $runtimePath -Force | Out-Null
    $packageName = "abctl-v$abctlVersion-windows-$architecture"
    $abctlPath = Join-Path $toolsPath "$packageName\abctl.exe"
    if (-not (Test-Path -LiteralPath $abctlPath)) {
        Write-Host "Baixando abctl $abctlVersion com verificacao SHA256..."
        $baseUrl = "https://github.com/airbytehq/abctl/releases/download/v$abctlVersion"
        $archivePath = Join-Path $toolsPath "$packageName.zip"
        $checksumPath = Join-Path $toolsPath 'checksums.txt'
        Invoke-WebRequest -UseBasicParsing -Uri "$baseUrl/$packageName.zip" -OutFile $archivePath
        Invoke-WebRequest -UseBasicParsing -Uri "$baseUrl/abctl_${abctlVersion}_checksums.txt" -OutFile $checksumPath
        $checksumLine = @(Get-Content -LiteralPath $checksumPath | Where-Object { $_.Trim().EndsWith("$packageName.zip") })
        if ($checksumLine.Count -ne 1) { throw 'Checksum oficial ausente ou ambiguo.' }
        $expected = ($checksumLine[0] -split '\s+')[0]
        if ((Get-FileHash -LiteralPath $archivePath -Algorithm SHA256).Hash -ne $expected) { throw 'Checksum do abctl divergente.' }
        Expand-Archive -LiteralPath $archivePath -DestinationPath $toolsPath -Force
    }
    docker info --format 'Docker: {{.NCPU}} CPUs, {{.MemTotal}} bytes de memoria'
    if ($LASTEXITCODE -ne 0) { throw 'Abra o Docker Desktop e aguarde o mecanismo iniciar.' }
    docker compose config --quiet
    if ($LASTEXITCODE -ne 0) { throw 'Docker Compose invalido.' }
    docker compose up -d --build
    if ($LASTEXITCODE -ne 0) { throw 'Falha ao iniciar os servicos do projeto.' }

    $airbyteReady = $false
    try {
        $ErrorActionPreference = 'Continue'
        $nodeRunning = docker inspect airbyte-abctl-control-plane --format '{{.State.Running}}' 2>$null
        $nodeStarted = $false
        if ($LASTEXITCODE -eq 0 -and $nodeRunning -eq 'false') {
            docker start airbyte-abctl-control-plane | Out-Null
            if ($LASTEXITCODE -ne 0) { throw 'Falha ao retomar o container Airbyte existente.' }
            $nodeStarted = $true
        }
        $attempts = if ($nodeStarted) { 12 } else { 1 }
        for ($attempt = 0; $attempt -lt $attempts; $attempt++) {
            $deploymentText = docker exec airbyte-abctl-control-plane kubectl get deployments airbyte-abctl-server airbyte-abctl-connector-builder-server -n airbyte-abctl -o json --request-timeout=5s 2>$null
            if ($LASTEXITCODE -eq 0) { break }
            if ($nodeStarted) { Start-Sleep -Seconds 5 }
        }
        if ($LASTEXITCODE -eq 0) {
            $deployments = (($deploymentText -join "`n") | ConvertFrom-Json).items
            $ready = @($deployments | Where-Object { $_.spec.template.spec.containers[0].image.EndsWith(":$chartVersion") })
            if ($ready.Count -eq 2) {
                $ErrorActionPreference = 'Stop'
                Set-AirbyteRuntimeProfile
                $page = Invoke-WebRequest -UseBasicParsing -Uri 'http://localhost:8001/' -TimeoutSec 10
                $airbyteReady = $page.StatusCode -eq 200
            } else {
                throw 'Ja existe Airbyte com outra versao neste computador. A instalacao existente foi preservada.'
            }
        }
    } finally { $ErrorActionPreference = 'Stop' }
    if ($airbyteReady) {
        Write-Host 'Airbyte 1.8.5 e Connector Builder ja estao ativos; reutilizando a instalacao.'
        'Airbyte 1.8.5 e Connector Builder saudaveis; instalacao existente reutilizada.' | Out-File -FilePath (Join-Path $runtimePath 'airbyte-install.log') -Encoding utf8
    } else {
        Write-Host 'Instalando Airbyte 1.8.5 em http://localhost:8001. A primeira instalacao pode demorar.'
        Write-Host 'Detalhes em .runtime/airbyte-install.log'
        # Windows PowerShell 5 treats native stderr as errors when redirected, even on exit 0.
        try {
            $ErrorActionPreference = 'Continue'
            & $abctlPath local install --chart-version $chartVersion --values (Join-Path $PSScriptRoot 'airbyte\values.json') --port 8001 --low-resource-mode --no-browser --insecure-cookies 2>&1 | Out-File -FilePath (Join-Path $runtimePath 'airbyte-install.log') -Encoding utf8
            $installExit = $LASTEXITCODE
        } finally { $ErrorActionPreference = 'Stop' }
        if ($installExit -ne 0) { throw 'Falha no Airbyte. Consulte .runtime/airbyte-install.log; os dados existentes foram preservados.' }
    }

    # Read the generated Kubernetes secret; abctl credentials is human-readable, not a JSON API.
    $secretText = docker exec airbyte-abctl-control-plane kubectl get secret airbyte-auth-secrets -n airbyte-abctl -o json
    if ($LASTEXITCODE -ne 0) { throw 'Nao foi possivel obter credenciais locais do Airbyte.' }
    $secret = ($secretText -join "`n") | ConvertFrom-Json
    $clientId = [System.Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($secret.data.'instance-admin-client-id'))
    $clientSecret = [System.Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($secret.data.'instance-admin-client-secret'))
    $configPath = Join-Path $runtimePath 'airbyte.json'
    $config = if (Test-Path -LiteralPath $configPath) { Get-Content -LiteralPath $configPath -Raw | ConvertFrom-Json } else { [pscustomobject]@{} }
    $config | Add-Member -NotePropertyName client_id -NotePropertyValue $clientId -Force
    $config | Add-Member -NotePropertyName client_secret -NotePropertyValue $clientSecret -Force
    [System.IO.File]::WriteAllText($configPath, ($config | ConvertTo-Json -Depth 20), [System.Text.UTF8Encoding]::new($false))

    docker compose run --rm --no-deps airbyte-init
    if ($LASTEXITCODE -ne 0) { throw 'Falha ao configurar conectores Airbyte.' }
    $configured = Get-Content -LiteralPath $configPath -Raw | ConvertFrom-Json
    $loginPassword = [System.Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($secret.data.'instance-admin-password'))
    $loginPath = Join-Path $runtimePath 'airbyte-login.txt'
    [System.IO.File]::WriteAllText($loginPath, ("URL: http://localhost:8001`r`nEmail: " + $configured.login_email + "`r`nSenha: " + $loginPassword + "`r`n"), [System.Text.UTF8Encoding]::new($false))
    if (-not $SkipSync) {
        docker compose run --rm --no-deps airbyte-init python airbyte/bootstrap.py sync
        if ($LASTEXITCODE -ne 0) { throw 'Falha na sincronizacao Airbyte; consulte a interface.' }
        docker compose exec -T airflow python -c 'from pipelines.airbyte import verify_bronze; print(verify_bronze())'
        if ($LASTEXITCODE -ne 0) { throw 'Dados Airbyte nao confirmados na Bronze.' }
        docker compose exec -T airflow python -m pipelines.cli demo
        if ($LASTEXITCODE -ne 0) { throw 'Falha ao preparar a demonstracao Silver/Gold.' }
        docker compose exec -T airflow python -m pipelines.cli verify
        if ($LASTEXITCODE -ne 0) { throw 'Validacao Silver falhou.' }
        docker compose exec -T airflow python -m pipelines.cli verify-gold
        if ($LASTEXITCODE -ne 0) { throw 'Validacao Gold falhou.' }
    }
    Write-Host 'ShopVibe pronto: Airbyte http://localhost:8001 | Airflow http://localhost:8080 | MinIO http://localhost:9002'
    Write-Host 'Login Airbyte salvo localmente em .runtime/airbyte-login.txt (nao enviar ao Git).'
} finally {
    Pop-Location
}
