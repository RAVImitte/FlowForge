param(
    [string[]]$Scenario,
    [string]$OutputDirectory,
    [switch]$SkipBuild,
    [switch]$NoCuration,
    [switch]$SummarizeOnly
)

$ErrorActionPreference = "Stop"
$repositoryRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$manifestPath = Join-Path $repositoryRoot "load-testing/resilience-matrix.json"
$manifest = Get-Content -Raw -LiteralPath $manifestPath | ConvertFrom-Json
$runKey = "resilience-matrix-" + (Get-Date -Format "yyyyMMdd-HHmmss")
if (-not $OutputDirectory) {
    $OutputDirectory = Join-Path $repositoryRoot "load-testing/results/$runKey"
}
$OutputDirectory = [System.IO.Path]::GetFullPath($OutputDirectory)
$curatedDirectory = Join-Path $repositoryRoot "docs/resilience/reports"
New-Item -ItemType Directory -Force -Path $OutputDirectory | Out-Null
if (-not $NoCuration) { New-Item -ItemType Directory -Force -Path $curatedDirectory | Out-Null }

function Write-Utf8Json {
    param([Parameter(Mandatory)]$Value, [Parameter(Mandatory)][string]$Path, [int]$Depth = 40)
    $json = $Value | ConvertTo-Json -Depth $Depth
    [System.IO.File]::WriteAllText($Path, $json, [System.Text.UTF8Encoding]::new($false))
}

function Get-ScenarioSummary {
    param($Definition, $Report, [string]$Failure)
    if (-not $Report) {
        return [ordered]@{
            id=$Definition.id; dependency=$Definition.dependency; topology=$Definition.topology
            profile=$Definition.profile; faultPlan=$Definition.faultPlan
            requiredPass=[bool]$Definition.requiredPass; passed=$false; failure=$Failure
        }
    }
    $fault = $Report.faultInjection
    $postgresProbeErrors = @($Report.probes.samples | Where-Object { $_.postgres.probeError }).Count
    $kafkaProbeErrors = @($Report.probes.samples | Where-Object { $_.kafkaProbeError }).Count
    $summary = [ordered]@{
        id = $Definition.id
        dependency = $Definition.dependency
        topology = $Definition.topology
        profile = $Definition.profile
        faultPlan = $Definition.faultPlan
        requiredPass = [bool]$Definition.requiredPass
        passed = [bool]$Report.passed
        failure = $Failure
        counts = $Report.workload.counts
        latencyMs = $Report.workload.latencyMs
        recoverySeconds = [double]$fault.recoverySeconds
        suppressedCommandDuplicates = [long]$fault.suppressedCommandDuplicates
        durableFailures = [long]$fault.durableFailures
        dependencyProbeErrors = $postgresProbeErrors + $kafkaProbeErrors
        checks = $fault.checks
    }
    if ($Definition.id -eq "redis-loss") {
        $flush = @($fault.events | Where-Object operation -eq "flush" | Select-Object -First 1)[0]
        $summary.redis = [ordered]@{
            coordinationFailures = [long]$fault.redisCoordinationFailures
            reconciliations = [long]$fault.coordinationReconciliations
            permitLimitViolations = [long]$fault.permitLimitViolations
            keysBeforeFlush = [long]$flush.redisKeysBefore
            reconstructedKeyCount = [long]$flush.reconstructedKeyCount
            reconstructedAt = $flush.mirrorReconstructedAt
        }
    }
    if ($Definition.id -eq "telemetry-pause") {
        $summary.telemetry = $fault.telemetry.after
    }
    return $summary
}

if ($manifest.schemaVersion -ne 1) { throw "Unsupported resilience matrix schemaVersion" }
if ($manifest.name -ne "phase-6-resilience-matrix") { throw "Unexpected resilience matrix name" }
$definitions = @($manifest.scenarios)
if ($Scenario -and $Scenario.Count -gt 0) {
    $unknown = @($Scenario | Where-Object { $_ -notin @($definitions.id) })
    if ($unknown.Count -gt 0) { throw "Unknown resilience scenario(s): $($unknown -join ', ')" }
    $definitions = @($definitions | Where-Object id -in $Scenario)
}
if ($definitions.Count -eq 0) { throw "No resilience scenarios selected" }
foreach ($definition in $definitions) {
    foreach ($asset in @(
        "load-testing/topologies/$($definition.topology).json",
        "load-testing/profiles/$($definition.profile).json",
        "load-testing/faults/$($definition.faultPlan).json"
    )) {
        if (-not (Test-Path -LiteralPath (Join-Path $repositoryRoot $asset) -PathType Leaf)) {
            throw "Resilience scenario $($definition.id) references missing asset $asset"
        }
    }
}

if (-not $SkipBuild -and -not $SummarizeOnly) {
    Push-Location $repositoryRoot
    try {
        $env:DEBUG = $null
        & ".\mvnw.cmd" -q -pl flowforge-control-plane,flowforge-worker,flowforge-load-test -am package '-DskipTests'
        if ($LASTEXITCODE -ne 0) { throw "Resilience-matrix build failed" }
    } finally { Pop-Location }
}

$failures = @{}
foreach ($definition in $definitions) {
    $scenarioDirectory = Join-Path $OutputDirectory ([string]$definition.id)
    if (-not $SummarizeOnly) {
        Write-Host "Running resilience scenario $($definition.id): $($definition.faultPlan)"
        try {
            & (Join-Path $PSScriptRoot "run-load-topology.ps1") `
                -Topology ([string]$definition.topology) -Profile ([string]$definition.profile) `
                -FaultPlan ([string]$definition.faultPlan) -OutputDirectory $scenarioDirectory `
                -SkipBuild -ResetData
        } catch {
            $failures[[string]$definition.id] = $_.Exception.Message
            Write-Warning "Scenario $($definition.id) reported failure: $($_.Exception.Message)"
        }
    }
}

$summaries = @()
foreach ($definition in $definitions) {
    $reportPath = Join-Path (Join-Path $OutputDirectory ([string]$definition.id)) "report.json"
    $failure = if ($failures.ContainsKey([string]$definition.id)) { $failures[[string]$definition.id] } else { "" }
    $report = if (Test-Path -LiteralPath $reportPath) {
        Get-Content -Raw -LiteralPath $reportPath | ConvertFrom-Json
    } else {
        if (-not $failure) { $failure = "No report exists for this scenario" }
        $null
    }
    if ($report -and -not $NoCuration) {
        $report.environment.machine = "redacted-local-rancher-host"
        $report | Add-Member -NotePropertyName curation -NotePropertyValue ([ordered]@{
            matrix = $manifest.name
            scenario = $definition.id
            curatedAt = (Get-Date).ToUniversalTime().ToString("o")
            caveat = "Local Rancher Desktop resilience evidence; not a universal recovery guarantee."
        }) -Force
        Write-Utf8Json $report (Join-Path $curatedDirectory "$($definition.id).json")
    }
    $summaries += Get-ScenarioSummary $definition $report $failure
}

$requiredFailures = @($summaries | Where-Object { $_.requiredPass -and -not $_.passed })
$matrixSummary = [ordered]@{
    schemaVersion = 1
    matrix = $manifest.name
    generatedAt = (Get-Date).ToUniversalTime().ToString("o")
    scenarioCount = $summaries.Count
    requiredFailureCount = $requiredFailures.Count
    passed = $requiredFailures.Count -eq 0
    caveat = "Results describe this local Rancher Desktop host and configured workload only."
    scenarios = $summaries
}
$summaryPath = Join-Path $OutputDirectory "summary.json"
Write-Utf8Json $matrixSummary $summaryPath
if (-not $NoCuration) {
    Write-Utf8Json $matrixSummary (Join-Path $repositoryRoot "docs/resilience/resilience-summary.json")
}
Write-Host "Resilience matrix summary: $summaryPath"
if ($requiredFailures.Count -gt 0) { throw "$($requiredFailures.Count) required resilience scenario(s) failed" }
