param(
    [Parameter(Mandatory)][ValidateSet("postgres", "kafka", "redis", "prometheus", "otel-collector")][string]$Service,
    [Parameter(Mandatory)][ValidateSet("pause", "flush")][string]$Operation,
    [Parameter(Mandatory)][ValidateRange(1, 30)][int]$DelaySeconds,
    [Parameter(Mandatory)][ValidateRange(0, 30)][int]$DurationSeconds,
    [Parameter(Mandatory)][string]$RepositoryRoot,
    [Parameter(Mandatory)][string]$EventPath,
    [string]$CoordinationNamespace
)

$ErrorActionPreference = "Stop"
$result = [ordered]@{
    schemaVersion = 1
    service = $Service
    operation = $Operation
    configuredDelaySeconds = $DelaySeconds
    configuredDurationSeconds = $DurationSeconds
    scheduledAt = (Get-Date).ToUniversalTime().ToString("o")
    faultStartedAt = $null
    restoredAt = $null
    pauseExitCode = $null
    restoreExitCode = $null
    redisKeysBefore = $null
    redisKeysAfterFlush = $null
    mirrorReconstructedAt = $null
    reconstructedKeyCount = $null
    passed = $false
    error = $null
}
$pauseAttempted = $false
$exitCode = 1

try {
    $resolvedRoot = (Resolve-Path -LiteralPath $RepositoryRoot).Path
    if (-not (Test-Path -LiteralPath (Join-Path $resolvedRoot "compose.yaml") -PathType Leaf)) {
        throw "Repository root does not contain compose.yaml"
    }
    Push-Location $resolvedRoot
    try {
        Start-Sleep -Seconds $DelaySeconds
        if ($Operation -eq "pause") {
            $pauseAttempted = $true
            docker compose pause $Service | Out-Null
            $result.pauseExitCode = $LASTEXITCODE
            if ($LASTEXITCODE -ne 0) { throw "Failed to pause Compose service $Service" }
            $result.faultStartedAt = (Get-Date).ToUniversalTime().ToString("o")
            Start-Sleep -Seconds $DurationSeconds
        } else {
            if ($Service -ne "redis") { throw "flush is supported only for Redis" }
            if ([string]::IsNullOrWhiteSpace($CoordinationNamespace)) { throw "Redis flush requires a coordination namespace" }
            $pattern = "flowforge:${CoordinationNamespace}:coordination:*"
            $result.redisKeysBefore = @(docker compose exec -T redis redis-cli --scan --pattern $pattern).Count
            $result.faultStartedAt = (Get-Date).ToUniversalTime().ToString("o")
            docker compose exec -T redis redis-cli FLUSHALL | Out-Null
            $result.pauseExitCode = $LASTEXITCODE
            if ($LASTEXITCODE -ne 0) { throw "Failed to flush Redis state" }
            $result.redisKeysAfterFlush = @(docker compose exec -T redis redis-cli --scan --pattern $pattern).Count
            $result.restoredAt = (Get-Date).ToUniversalTime().ToString("o")
            $result.restoreExitCode = 0
            if ($result.redisKeysAfterFlush -gt 0) {
                $result.reconstructedKeyCount = $result.redisKeysAfterFlush
                $result.mirrorReconstructedAt = $result.restoredAt
            }
            $deadline = (Get-Date).AddSeconds(15)
            while (-not $result.mirrorReconstructedAt -and (Get-Date) -lt $deadline) {
                $keyCount = @(docker compose exec -T redis redis-cli --scan --pattern $pattern).Count
                if ($keyCount -gt 0) {
                    $result.reconstructedKeyCount = $keyCount
                    $result.mirrorReconstructedAt = (Get-Date).ToUniversalTime().ToString("o")
                    break
                }
                Start-Sleep -Milliseconds 500
            }
            if (-not $result.mirrorReconstructedAt) { throw "Redis coordination mirror was not reconstructed within 15 seconds" }
        }
    } finally {
        if ($pauseAttempted) {
            docker compose unpause $Service | Out-Null
            $result.restoreExitCode = $LASTEXITCODE
            $result.restoredAt = (Get-Date).ToUniversalTime().ToString("o")
        }
        Pop-Location
    }
    if ($result.restoreExitCode -ne 0) { throw "Failed to restore Compose service $Service" }
    $result.passed = $true
    $exitCode = 0
} catch {
    $result.error = $_.Exception.Message
} finally {
    $eventDirectory = Split-Path -Parent ([System.IO.Path]::GetFullPath($EventPath))
    [System.IO.Directory]::CreateDirectory($eventDirectory) | Out-Null
    $json = $result | ConvertTo-Json -Depth 10
    [System.IO.File]::WriteAllText($EventPath, $json, [System.Text.UTF8Encoding]::new($false))
}

exit $exitCode
