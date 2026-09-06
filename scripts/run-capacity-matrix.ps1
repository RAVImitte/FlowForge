param(
    [string[]]$Scenario,
    [string]$OutputDirectory,
    [switch]$SkipBuild,
    [switch]$NoCuration,
    [switch]$SummarizeOnly
)

$ErrorActionPreference = "Stop"
$repositoryRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$manifestPath = Join-Path $repositoryRoot "load-testing/capacity-matrix.json"
$manifest = Get-Content -Raw -LiteralPath $manifestPath | ConvertFrom-Json
$runKey = "capacity-matrix-" + (Get-Date -Format "yyyyMMdd-HHmmss")
if (-not $OutputDirectory) {
    $OutputDirectory = Join-Path $repositoryRoot "load-testing/results/$runKey"
}
$OutputDirectory = [System.IO.Path]::GetFullPath($OutputDirectory)
$curatedDirectory = Join-Path $repositoryRoot "docs/performance/reports"
New-Item -ItemType Directory -Force -Path $OutputDirectory | Out-Null
if (-not $NoCuration) { New-Item -ItemType Directory -Force -Path $curatedDirectory | Out-Null }

function Write-Utf8Json {
    param([Parameter(Mandatory)]$Value, [Parameter(Mandatory)][string]$Path, [int]$Depth = 40)
    $json = $Value | ConvertTo-Json -Depth $Depth
    [System.IO.File]::WriteAllText($Path, $json, [System.Text.UTF8Encoding]::new($false))
}

function Get-UsefulWorkStats {
    param([array]$Rows)
    $deltas = @($Rows | ForEach-Object { [long]$_.delta })
    if ($deltas.Count -eq 0) {
        return [ordered]@{ instances=0; total=0; minimum=0; maximum=0; imbalanceRatio=0.0 }
    }
    $measure = $deltas | Measure-Object -Minimum -Maximum -Sum -Average
    $imbalance = if ([double]$measure.Average -gt 0) {
        ([double]$measure.Maximum - [double]$measure.Minimum) / [double]$measure.Average
    } else { 0.0 }
    return [ordered]@{
        instances = $deltas.Count
        total = [long]$measure.Sum
        minimum = [long]$measure.Minimum
        maximum = [long]$measure.Maximum
        imbalanceRatio = [Math]::Round($imbalance, 6)
    }
}

function Get-ScenarioSummary {
    param($Definition, $Report, [string]$Failure)
    if (-not $Report) {
        return [ordered]@{
            id=$Definition.id; category=$Definition.category; topology=$Definition.topology
            profile=$Definition.profile; requiredPass=[bool]$Definition.requiredPass
            passed=$false; failure=$Failure
        }
    }
    $finalSample = @($Report.probes.samples | Select-Object -Last 1)[0]
    $commandAssignments = @($finalSample.kafkaAssignments | Where-Object group -eq "flowforge-workers-v1")
    $activeConsumers = @($commandAssignments | Where-Object consumerId -ne "-" | Select-Object -ExpandProperty consumerId -Unique)
    return [ordered]@{
        id = $Definition.id
        category = $Definition.category
        topology = $Definition.topology
        profile = $Definition.profile
        requiredPass = [bool]$Definition.requiredPass
        passed = [bool]$Report.passed
        failure = $Failure
        counts = $Report.workload.counts
        rates = $Report.workload.rates
        latencyMs = $Report.workload.latencyMs
        peaks = $Report.probes.peaks
        durableExecutionCounts = $Report.durableExecutionCounts
        durableFailureCauses = @($Report.durableFailureCauses)
        workerUsefulWork = Get-UsefulWorkStats @($Report.distribution.workerCommandsCompleted)
        controlPlaneUsefulWork = Get-UsefulWorkStats @($Report.distribution.controlPlaneResultsConsumed)
        kafka = [ordered]@{
            configuredPartitions = [int]$Report.topology.kafkaPartitions
            configuredWorkerConsumers = @($Report.topology.worker.ports).Count * [int]$Report.topology.worker.concurrency
            activeCommandConsumers = $activeConsumers.Count
            assignedCommandPartitions = $commandAssignments.Count
        }
    }
}

function Wait-Postgres {
    $deadline = (Get-Date).AddSeconds(90)
    do {
        $item = docker compose ps postgres --format json 2>$null | Select-Object -First 1
        if ($item) {
            $state = $item | ConvertFrom-Json
            if ($state.State -eq "running" -and $state.Health -eq "healthy") { return }
        }
        Start-Sleep -Seconds 1
    } while ((Get-Date) -lt $deadline)
    throw "PostgreSQL did not become healthy while enriching capacity reports"
}

function Get-DurableFailureCauses {
    param([string]$RunId)
    $parsedRunId = [Guid]::Empty
    if (-not [Guid]::TryParse($RunId, [ref]$parsedRunId)) { throw "Invalid load-test run id: $RunId" }
    $workflowName = "Load test $($parsedRunId.ToString())"
    $sql = @"
SELECT json_build_object(
    'status', ta.status,
    'errorCode', COALESCE(ta.error_code, 'UNCLASSIFIED'),
    'errorMessage', COALESCE(ta.error_message, ''),
    'count', count(*)
)
FROM task_attempt ta
JOIN task_execution te ON te.id=ta.task_execution_id
JOIN workflow_execution we ON we.id=te.workflow_execution_id
JOIN workflow_version wv ON wv.id=we.workflow_version_id
WHERE wv.name='$workflowName' AND ta.status IN ('FAILED', 'TIMED_OUT')
GROUP BY ta.status, ta.error_code, ta.error_message
ORDER BY count(*) DESC, ta.status, ta.error_code;
"@
    $causes = @()
    docker compose exec -T postgres psql -U flowforge -d flowforge -At -c $sql 2>$null | ForEach-Object {
        if (-not [string]::IsNullOrWhiteSpace($_)) { $causes += ($_ | ConvertFrom-Json) }
    }
    return $causes
}

function Add-MissingFailureEvidence {
    param([array]$Definitions)
    $missing = @($Definitions | Where-Object {
        $path = Join-Path (Join-Path $OutputDirectory ([string]$_.id)) "report.json"
        if (-not (Test-Path -LiteralPath $path)) { return $false }
        $candidate = Get-Content -Raw -LiteralPath $path | ConvertFrom-Json
        return $candidate.PSObject.Properties.Name -notcontains "durableFailureCauses"
    })
    if ($missing.Count -eq 0) { return }

    $postgresWasRunning = @(docker compose ps --status running --services 2>$null) -contains "postgres"
    try {
        if (-not $postgresWasRunning) {
            docker compose up -d postgres | Out-Null
            if ($LASTEXITCODE -ne 0) { throw "Failed to start PostgreSQL for report enrichment" }
        }
        Wait-Postgres
        foreach ($definition in $missing) {
            $reportPath = Join-Path (Join-Path $OutputDirectory ([string]$definition.id)) "report.json"
            $report = Get-Content -Raw -LiteralPath $reportPath | ConvertFrom-Json
            $causes = @(Get-DurableFailureCauses ([string]$report.workload.runId))
            $report | Add-Member -NotePropertyName durableFailureCauses -NotePropertyValue $causes -Force
            Write-Utf8Json $report $reportPath 40
        }
    } finally {
        if (-not $postgresWasRunning) { docker compose stop postgres | Out-Null }
    }
}

if ($manifest.schemaVersion -ne 1) { throw "Unsupported capacity matrix schemaVersion" }
$definitions = @($manifest.scenarios)
if ($Scenario -and $Scenario.Count -gt 0) {
    $unknown = @($Scenario | Where-Object { $_ -notin @($definitions.id) })
    if ($unknown.Count -gt 0) { throw "Unknown matrix scenario(s): $($unknown -join ', ')" }
    $definitions = @($definitions | Where-Object id -in $Scenario)
}
if ($definitions.Count -eq 0) { throw "No capacity scenarios selected" }

if (-not $SkipBuild -and -not $SummarizeOnly) {
    Push-Location $repositoryRoot
    try {
        $env:DEBUG = $null
        & ".\mvnw.cmd" -q -pl flowforge-control-plane,flowforge-worker,flowforge-load-test -am package '-DskipTests'
        if ($LASTEXITCODE -ne 0) { throw "Capacity-matrix build failed" }
    } finally {
        Pop-Location
    }
}

$failures = @{}
foreach ($definition in $definitions) {
    $scenarioDirectory = Join-Path $OutputDirectory ([string]$definition.id)
    if (-not $SummarizeOnly) {
        Write-Host "Running capacity scenario $($definition.id): $($definition.topology) / $($definition.profile)"
        try {
            & (Join-Path $PSScriptRoot "run-load-topology.ps1") `
                -Topology ([string]$definition.topology) -Profile ([string]$definition.profile) `
                -OutputDirectory $scenarioDirectory -SkipBuild -ResetData
        } catch {
            $failures[[string]$definition.id] = $_.Exception.Message
            Write-Warning "Scenario $($definition.id) reported failure: $($_.Exception.Message)"
        }
    }
}

Add-MissingFailureEvidence $definitions

$summaries = @()
foreach ($definition in $definitions) {
    $scenarioDirectory = Join-Path $OutputDirectory ([string]$definition.id)
    $reportPath = Join-Path $scenarioDirectory "report.json"
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
            category = $definition.category
            curatedAt = (Get-Date).ToUniversalTime().ToString("o")
            caveat = "Local Rancher Desktop evidence; not a universal production capacity claim."
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
if (-not $NoCuration) { Write-Utf8Json $matrixSummary (Join-Path $repositoryRoot "docs/performance/capacity-matrix-summary.json") }
Write-Host "Capacity matrix summary: $summaryPath"
if ($requiredFailures.Count -gt 0) { throw "$($requiredFailures.Count) required capacity scenario(s) failed" }
