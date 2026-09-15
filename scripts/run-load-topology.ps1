param(
    [string]$Topology = "single-6p",
    [string]$Profile = "smoke",
    [string]$FaultPlan,
    [string]$OutputDirectory,
    [switch]$SkipBuild,
    [switch]$ResetData,
    [switch]$KeepInfrastructure
)

$ErrorActionPreference = "Stop"
$repositoryRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$isWindowsHost = [Environment]::OSVersion.Platform -eq [PlatformID]::Win32NT
$topologyPath = Join-Path $repositoryRoot "load-testing/topologies/$Topology.json"
$profilePath = Join-Path $repositoryRoot "load-testing/profiles/$Profile.json"
$faultPlanPath = if ($FaultPlan) { Join-Path $repositoryRoot "load-testing/faults/$FaultPlan.json" } else { $null }
if (-not (Test-Path -LiteralPath $topologyPath -PathType Leaf)) { throw "Unknown topology: $Topology" }
if (-not (Test-Path -LiteralPath $profilePath -PathType Leaf)) { throw "Unknown workload profile: $Profile" }
if ($faultPlanPath -and -not (Test-Path -LiteralPath $faultPlanPath -PathType Leaf)) { throw "Unknown fault plan: $FaultPlan" }
$topologyConfig = Get-Content -Raw -LiteralPath $topologyPath | ConvertFrom-Json
$profileConfig = Get-Content -Raw -LiteralPath $profilePath | ConvertFrom-Json
$faultConfig = if ($faultPlanPath) { Get-Content -Raw -LiteralPath $faultPlanPath | ConvertFrom-Json } else { $null }
$requiresTelemetry = $faultConfig -and @($faultConfig.actions | Where-Object { $_.service -in @("prometheus", "otel-collector") }).Count -gt 0
$runKey = "{0}-{1}-{2}" -f $Topology, $Profile, (Get-Date -Format "yyyyMMdd-HHmmss")
$coordinationNamespace = "load-" + [Guid]::NewGuid().ToString("N").Substring(0, 12)
if (-not $OutputDirectory) {
    $OutputDirectory = Join-Path $repositoryRoot "load-testing/results/$runKey"
}
$OutputDirectory = [System.IO.Path]::GetFullPath($OutputDirectory)
New-Item -ItemType Directory -Force -Path $OutputDirectory | Out-Null

function Write-Utf8Json {
    param([Parameter(Mandatory)]$Value, [Parameter(Mandatory)][string]$Path, [int]$Depth = 20)
    $json = $Value | ConvertTo-Json -Depth $Depth
    [System.IO.File]::WriteAllText($Path, $json, [System.Text.UTF8Encoding]::new($false))
}

function Assert-Topology {
    if ($topologyConfig.schemaVersion -ne 1) { throw "Unsupported topology schemaVersion" }
    if ($topologyConfig.name -ne $Topology) { throw "Topology name must match its file name" }
    $ports = @($topologyConfig.controlPlane.ports) + @($topologyConfig.worker.ports)
    if (@($topologyConfig.controlPlane.ports).Count -lt 1) { throw "At least one control-plane port is required" }
    if (@($topologyConfig.worker.ports).Count -lt 1) { throw "At least one worker port is required" }
    if (($ports | Sort-Object -Unique).Count -ne $ports.Count) { throw "Topology ports must be unique" }
    if ($ports | Where-Object { $_ -lt 1024 -or $_ -gt 65535 }) { throw "Topology ports must be between 1024 and 65535" }
    if ($topologyConfig.kafkaPartitions -lt 1) { throw "kafkaPartitions must be positive" }
    if ($topologyConfig.probeIntervalSeconds -lt 1) { throw "probeIntervalSeconds must be positive" }
    if ($topologyConfig.startupTimeoutSeconds -lt 1) { throw "startupTimeoutSeconds must be positive" }
    if ($topologyConfig.postRunDrainSeconds -lt 0) { throw "postRunDrainSeconds cannot be negative" }
    if ($topologyConfig.controlPlane.databasePoolSize -lt 1 -or $topologyConfig.controlPlane.maxHeapMb -lt 1) {
        throw "Control-plane pool and heap settings must be positive"
    }
    if ($topologyConfig.worker.databasePoolSize -lt 1 -or $topologyConfig.worker.maxHeapMb -lt 1 -or $topologyConfig.worker.concurrency -lt 1) {
        throw "Worker pool, heap, and concurrency settings must be positive"
    }
}

function Assert-FaultPlan {
    if (-not $faultConfig) { return }
    if ($faultConfig.schemaVersion -ne 1) { throw "Unsupported fault-plan schemaVersion" }
    if ($faultConfig.name -ne $FaultPlan) { throw "Fault-plan name must match its file name" }
    $actions = @($faultConfig.actions)
    if ($actions.Count -lt 1 -or $actions.Count -gt 4) { throw "Fault plans require between one and four actions" }
    foreach ($action in $actions) {
        if ($action.service -notin @("postgres", "kafka", "redis", "prometheus", "otel-collector")) { throw "Unsupported fault service: $($action.service)" }
        if ($action.operation -notin @("pause", "flush")) { throw "Unsupported fault operation: $($action.operation)" }
        if ($action.operation -eq "flush" -and $action.service -ne "redis") { throw "flush is supported only for Redis" }
        if ($action.delaySeconds -lt 1 -or $action.delaySeconds -gt 30) { throw "Fault delaySeconds must be between 1 and 30" }
        if ($action.operation -eq "pause" -and ($action.durationSeconds -lt 1 -or $action.durationSeconds -gt 30)) { throw "Pause durationSeconds must be between 1 and 30" }
        if ($action.operation -eq "flush" -and $action.durationSeconds -ne 0) { throw "Flush durationSeconds must be zero" }
    }
    if ($faultConfig.thresholds.maximumRecoverySeconds -lt 1 -or $faultConfig.thresholds.maximumRecoverySeconds -gt 300) {
        throw "maximumRecoverySeconds must be between 1 and 300"
    }
    if ($faultConfig.thresholds.maximumSuppressedCommandDuplicates -lt 0 -or $faultConfig.thresholds.maximumDurableFailures -lt 0) {
        throw "Fault duplicate and durable-failure thresholds cannot be negative"
    }
}

function Invoke-Build {
    if ($SkipBuild) { return }
    Push-Location $repositoryRoot
    try {
        $env:DEBUG = $null
        $mavenWrapper = if ($isWindowsHost) { ".\mvnw.cmd" } else { "./mvnw" }
        & $mavenWrapper -q -pl flowforge-control-plane,flowforge-worker,flowforge-load-test -am package '-DskipTests'
        if ($LASTEXITCODE -ne 0) { throw "Application/load-test build failed" }
    } finally {
        Pop-Location
    }
}

function Start-BackgroundProcess {
    param(
        [Parameter(Mandatory)][string]$FilePath,
        [Parameter(Mandatory)][string[]]$ArgumentList,
        [string]$RedirectStandardOutput,
        [string]$RedirectStandardError
    )
    $parameters = @{
        FilePath = $FilePath
        ArgumentList = $ArgumentList
        PassThru = $true
    }
    if ($RedirectStandardOutput) { $parameters.RedirectStandardOutput = $RedirectStandardOutput }
    if ($RedirectStandardError) { $parameters.RedirectStandardError = $RedirectStandardError }
    if ($isWindowsHost) { $parameters.WindowStyle = "Hidden" }
    Start-Process @parameters
}

function Wait-Infrastructure {
    $deadline = (Get-Date).AddSeconds(90)
    do {
        $items = @(docker compose ps --format json | ForEach-Object { $_ | ConvertFrom-Json })
        $required = @($items | Where-Object { $_.Service -in @("postgres", "kafka", "redis") })
        if ($required.Count -eq 3 -and @($required | Where-Object { $_.Health -ne "healthy" }).Count -eq 0) { return }
        Start-Sleep -Seconds 1
    } while ((Get-Date) -lt $deadline)
    throw "PostgreSQL, Kafka, and Redis did not become healthy"
}

function Get-TelemetryProbe {
    param([string]$Since = "")
    $prometheusReady = $false
    $targets = @()
    try {
        $ready = Invoke-WebRequest "http://localhost:9090/-/ready" -UseBasicParsing -TimeoutSec 3
        $prometheusReady = $ready.StatusCode -eq 200
        $response = Invoke-RestMethod "http://localhost:9090/api/v1/targets" -TimeoutSec 3
        $targets = @($response.data.activeTargets | Where-Object { $_.labels.job -in @("flowforge-control-plane", "flowforge-worker") } | ForEach-Object {
            [ordered]@{ job=$_.labels.job; health=$_.health; lastError=$_.lastError }
        })
    } catch {}
    $collectorRunning = $false
    $collectorPortOpen = $false
    $traceBatches = 0
    try {
        $collector = @(docker compose --profile observability ps --format json otel-collector | ForEach-Object { $_ | ConvertFrom-Json }) | Select-Object -First 1
        $collectorRunning = $collector -and $collector.State -eq "running"
        $tcp = [Net.Sockets.TcpClient]::new()
        try {
            $connect = $tcp.ConnectAsync("localhost", 4318)
            $collectorPortOpen = $connect.Wait(2000) -and $tcp.Connected
        } finally { $tcp.Dispose() }
        if ($Since) {
            $logs = docker compose --profile observability logs --since $Since otel-collector 2>$null | Out-String
            $traceBatches = [regex]::Matches($logs, '(?m)\bTraces\b').Count
        }
    } catch {}
    return [ordered]@{
        prometheusReady = $prometheusReady
        targets = $targets
        healthyApplicationTargets = @($targets | Where-Object health -eq "up").Count
        collectorRunning = [bool]$collectorRunning
        collectorPortOpen = $collectorPortOpen
        traceBatchesAfterRestore = $traceBatches
    }
}

function Wait-TelemetryRecovery {
    $deadline = (Get-Date).AddSeconds(45)
    do {
        $probe = Get-TelemetryProbe
        if ($probe.prometheusReady -and $probe.collectorRunning -and $probe.collectorPortOpen -and $probe.healthyApplicationTargets -ge 2) {
            return $probe
        }
        Start-Sleep -Seconds 2
    } while ((Get-Date) -lt $deadline)
    throw "Prometheus targets and OpenTelemetry Collector did not recover before the deadline"
}

function Start-FlowForgeProcess {
    param([string]$Service, [int]$Index, [int]$Port, [string]$Jar, [int]$PoolSize, [int]$MaxHeapMb)
    $instance = "$Service-$Index"
    $stdout = Join-Path $OutputDirectory "$instance.log"
    $stderr = Join-Path $OutputDirectory "$instance.err.log"
    $env:DEBUG = $null
    $env:SPRING_PROFILES_ACTIVE = "production"
    $env:FLOWFORGE_SECURITY_ENABLED = "false"
    $env:FLOWFORGE_ENVIRONMENT = $coordinationNamespace
    $env:FLOWFORGE_OTLP_ENABLED = if ($requiresTelemetry) { "true" } else { "false" }
    $env:FLOWFORGE_TRACING_SAMPLING_PROBABILITY = if ($requiresTelemetry) { "1" } else { "0" }
    $env:FLOWFORGE_KAFKA_TOPIC_PARTITIONS = [string]$topologyConfig.kafkaPartitions
    $env:FLOWFORGE_CONCURRENCY_RECONCILIATION_INTERVAL_MS = "2000"
    if ($Service -eq "control-plane") {
        $env:PORT = [string]$Port
        $env:FLOWFORGE_DB_POOL_SIZE = [string]$PoolSize
        $env:FLOWFORGE_INSTANCE_ID = $instance
        $env:FLOWFORGE_SCHEDULER_INSTANCE_ID = $instance
        $env:FLOWFORGE_RESULT_CONSUMER_CONCURRENCY = [string]$topologyConfig.kafkaPartitions
        $env:FLOWFORGE_RESULT_INLINE_DISPATCH_ENABLED = "false"
        $env:FLOWFORGE_HEARTBEAT_CONSUMER_CONCURRENCY = [string][Math]::Min(2, [int]$topologyConfig.kafkaPartitions)
        $env:FLOWFORGE_OUTBOX_BATCH_SIZE = "500"
        $env:FLOWFORGE_OUTBOX_PUBLISH_CONCURRENCY = [string][Math]::Min(8, [int]$PoolSize)
        $env:FLOWFORGE_OUTBOX_POLL_INTERVAL_MS = "25"
        $env:FLOWFORGE_OUTBOX_COMMAND_DISPATCH_INTERVAL_MS = "25"
    } else {
        $env:WORKER_PORT = [string]$Port
        $env:FLOWFORGE_WORKER_DB_POOL_SIZE = [string]$PoolSize
        $env:FLOWFORGE_WORKER_ID = $instance
        $env:FLOWFORGE_WORKER_CONCURRENCY = [string]$topologyConfig.worker.concurrency
        $env:FLOWFORGE_WORKER_RESULT_BATCH_SIZE = "500"
        $env:FLOWFORGE_WORKER_RESULT_POLL_INTERVAL_MS = "25"
    }
    $quotedJar = '"' + $Jar + '"'
    $process = Start-BackgroundProcess -FilePath "java" `
        -ArgumentList @("-Xms128m", "-Xmx${MaxHeapMb}m", "-jar", $quotedJar) `
        -RedirectStandardOutput $stdout -RedirectStandardError $stderr
    return [pscustomobject]@{
        service = $Service
        instance = $instance
        port = $Port
        process = $process
        stdout = $stdout
        stderr = $stderr
    }
}

function Wait-Applications {
    param([array]$Applications)
    $deadline = (Get-Date).AddSeconds([int]$topologyConfig.startupTimeoutSeconds)
    do {
        $healthy = 0
        foreach ($application in $Applications) {
            $application.process.Refresh()
            if ($application.process.HasExited) {
                throw "$($application.instance) exited during startup; inspect $($application.stdout)"
            }
            try {
                $health = Invoke-RestMethod "http://localhost:$($application.port)/actuator/health/readiness" -TimeoutSec 2
                if ($health.status -eq "UP") { $healthy++ }
            } catch {}
        }
        if ($healthy -eq $Applications.Count) { return }
        Start-Sleep -Seconds 1
    } while ((Get-Date) -lt $deadline)
    throw "Applications did not become healthy before the startup deadline"
}

function Get-MetricSum {
    param([string]$Text, [string]$Name, [string]$RequiredLabel = "")
    $sum = 0.0
    $pattern = "(?m)^" + [regex]::Escape($Name) + "(?<labels>\{[^}]*\})?\s+(?<value>[-+0-9.eE]+)$"
    foreach ($match in [regex]::Matches($Text, $pattern)) {
        if ($RequiredLabel -and -not $match.Groups["labels"].Value.Contains($RequiredLabel)) { continue }
        $sum += [double]::Parse($match.Groups["value"].Value, [Globalization.CultureInfo]::InvariantCulture)
    }
    return $sum
}

function Get-ApplicationMetrics {
    param([array]$Applications)
    $rows = @()
    foreach ($application in $Applications) {
        try {
            $text = (Invoke-WebRequest "http://localhost:$($application.port)/actuator/prometheus" -UseBasicParsing -TimeoutSec 4).Content
            $rows += [pscustomobject]@{
                service = $application.service
                instance = $application.instance
                port = $application.port
                processCpu = Get-MetricSum $text "process_cpu_usage"
                heapBytes = Get-MetricSum $text "jvm_memory_used_bytes" 'area="heap"'
                activeExecutions = Get-MetricSum $text "flowforge_executions_active"
                readyTasks = Get-MetricSum $text "flowforge_tasks_ready"
                readyOldestAgeSeconds = Get-MetricSum $text "flowforge_tasks_ready_oldest_age_seconds"
                outboxPending = Get-MetricSum $text "flowforge_outbox_pending"
                outboxOldestAgeSeconds = Get-MetricSum $text "flowforge_outbox_oldest_age_seconds"
                workerProcessing = Get-MetricSum $text "flowforge_worker_commands_processing"
                workerResultsPending = Get-MetricSum $text "flowforge_worker_results_pending"
                commandsCompleted = Get-MetricSum $text "flowforge_worker_commands_completed_total"
                commandDuplicates = Get-MetricSum $text "flowforge_worker_commands_duplicates_total"
                resultsConsumed = Get-MetricSum $text "flowforge_results_consumed_total"
                retriesScheduled = Get-MetricSum $text "flowforge_retries_scheduled_total"
                taskTimeouts = Get-MetricSum $text "flowforge_timeouts_tasks_total"
                redisCoordinationFailures = Get-MetricSum $text "flowforge_coordination_redis_failures_total"
                coordinationReconciliations = Get-MetricSum $text "flowforge_coordination_reconciliations_total"
                coordinationPermitsReconciled = Get-MetricSum $text "flowforge_coordination_reconciled_permits_sum"
            }
        } catch {
            $rows += [pscustomobject]@{ service=$application.service; instance=$application.instance; port=$application.port; scrapeError=$_.Exception.Message }
        }
    }
    return @($rows)
}

function Get-PostgresProbe {
    $sql = @"
WITH permit_violations AS (
    SELECT cp.resource_key
      FROM coordination_permit cp
      JOIN workflow_execution we ON we.id::text = cp.holder_id
      JOIN workflow_version wv ON wv.id = we.workflow_version_id
     WHERE cp.status = 'ACTIVE' AND cp.resource_key LIKE 'concurrency:workflow-version:%'
     GROUP BY cp.resource_key, wv.max_concurrent_executions
    HAVING count(*) > wv.max_concurrent_executions
    UNION ALL
    SELECT cp.resource_key
      FROM coordination_permit cp
      JOIN task_attempt ta ON ta.id::text = cp.holder_id
      JOIN task_execution te ON te.id = ta.task_execution_id
      JOIN workflow_execution we ON we.id = te.workflow_execution_id
      JOIN workflow_task wt ON wt.workflow_version_id = we.workflow_version_id AND wt.task_key = te.task_key
     WHERE cp.status = 'ACTIVE' AND cp.resource_key LIKE 'concurrency:task:%'
     GROUP BY cp.resource_key, wt.max_concurrency
    HAVING count(*) > wt.max_concurrency
)
SELECT count(*) FILTER (WHERE state='active'), count(*),
       (SELECT count(*) FROM pg_locks WHERE NOT granted),
       (SELECT count(*) FROM coordination_permit WHERE status='ACTIVE'),
       (SELECT count(*) FROM permit_violations)
  FROM pg_stat_activity WHERE datname='flowforge';
"@
    $previousErrorPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = "SilentlyContinue"
        $output = @(docker compose exec -T postgres psql -U flowforge -d flowforge -At -F '|' -c $sql 2>&1)
        $probeExitCode = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $previousErrorPreference
    }
    if ($probeExitCode -ne 0) {
        return [pscustomobject]@{ activeConnections=0; totalConnections=0; blockedLocks=0; activePermits=0; permitLimitViolations=0; probeError="PostgreSQL probe unavailable (exit $probeExitCode)" }
    }
    $line = $output | Select-Object -Last 1
    if ($line -match '^(\d+)\|(\d+)\|(\d+)\|(\d+)\|(\d+)$') {
        return [pscustomobject]@{ activeConnections=[int]$Matches[1]; totalConnections=[int]$Matches[2]; blockedLocks=[int]$Matches[3]; activePermits=[int]$Matches[4]; permitLimitViolations=[int]$Matches[5] }
    }
    return [pscustomobject]@{ activeConnections=0; totalConnections=0; blockedLocks=0; activePermits=0; permitLimitViolations=0; probeError="Unexpected psql output" }
}

function Get-KafkaLags {
    $lags = [ordered]@{
        commands = 0L
        results = 0L
        heartbeats = 0L
    }
    $previousErrorPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = "SilentlyContinue"
        $output = @(docker compose exec -T kafka timeout 8s /opt/kafka/bin/kafka-consumer-groups.sh `
            --bootstrap-server localhost:9092 --describe --all-groups 2>&1)
        $probeExitCode = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $previousErrorPreference
    }
    if ($probeExitCode -ne 0) {
        return [pscustomobject]@{
            lag = $lags
            assignments = @()
            probeError = "Kafka probe unavailable (exit $probeExitCode)"
        }
    }
    $groups = @{
        "flowforge-workers-v1" = "commands"
        "flowforge-control-plane-results-v1" = "results"
        "flowforge-control-plane-heartbeats-v1" = "heartbeats"
    }
    $assignments = @()
    foreach ($line in $output) {
        $parts = @($line.Trim() -split '\s+')
        if ($parts.Count -ge 6 -and $groups.ContainsKey($parts[0]) -and $parts[1].StartsWith("flowforge.") -and $parts[5] -match '^\d+$') {
            $key = $groups[$parts[0]]
            $lags[$key] += [long]$parts[5]
            $assignments += [ordered]@{
                group = $parts[0]
                topic = $parts[1]
                partition = [int]$parts[2]
                lag = [long]$parts[5]
                consumerId = if ($parts.Count -gt 6) { $parts[6] } else { "" }
                host = if ($parts.Count -gt 7) { $parts[7] } else { "" }
                clientId = if ($parts.Count -gt 8) { $parts[8] } else { "" }
            }
        }
    }
    return [pscustomobject]@{ lag=$lags; assignments=@($assignments) }
}

function Get-ProbeSample {
    param([array]$Applications)
    $metrics = @(Get-ApplicationMetrics $Applications)
    $valid = @($metrics | Where-Object { -not $_.scrapeError })
    $control = @($valid | Where-Object service -eq "control-plane")
    $workers = @($valid | Where-Object service -eq "worker")
    $postgres = Get-PostgresProbe
    $kafkaLags = Get-KafkaLags
    return [ordered]@{
        timestamp = (Get-Date).ToUniversalTime().ToString("o")
        applicationScrapeErrors = @($metrics | Where-Object scrapeError).Count
        totalProcessCpu = [double](($valid | Measure-Object processCpu -Sum).Sum)
        totalHeapBytes = [long](($valid | Measure-Object heapBytes -Sum).Sum)
        activeExecutions = [long](($control | Measure-Object activeExecutions -Maximum).Maximum)
        readyTasks = [long](($control | Measure-Object readyTasks -Maximum).Maximum)
        readyOldestAgeSeconds = [double](($control | Measure-Object readyOldestAgeSeconds -Maximum).Maximum)
        outboxPending = [long](($control | Measure-Object outboxPending -Maximum).Maximum)
        outboxOldestAgeSeconds = [double](($control | Measure-Object outboxOldestAgeSeconds -Maximum).Maximum)
        workerProcessing = [long](($workers | Measure-Object workerProcessing -Sum).Sum)
        workerResultsPending = [long](($workers | Measure-Object workerResultsPending -Sum).Sum)
        postgres = $postgres
        commandsCompleted = [long](($workers | Measure-Object commandsCompleted -Sum).Sum)
        commandDuplicates = [long](($workers | Measure-Object commandDuplicates -Sum).Sum)
        resultsConsumed = [long](($control | Measure-Object resultsConsumed -Sum).Sum)
        retriesScheduled = [long](($control | Measure-Object retriesScheduled -Sum).Sum)
        taskTimeouts = [long](($control | Measure-Object taskTimeouts -Sum).Sum)
        kafkaLag = $kafkaLags.lag
        kafkaAssignments = @($kafkaLags.assignments)
        kafkaProbeError = $kafkaLags.probeError
    }
}

function Update-Peaks {
    param([hashtable]$Peaks, $Sample)
    $values = [ordered]@{
        totalProcessCpu = [double]$Sample.totalProcessCpu
        totalHeapBytes = [long]$Sample.totalHeapBytes
        activeExecutions = [long]$Sample.activeExecutions
        readyTasks = [long]$Sample.readyTasks
        readyOldestAgeSeconds = [double]$Sample.readyOldestAgeSeconds
        outboxPending = [long]$Sample.outboxPending
        outboxOldestAgeSeconds = [double]$Sample.outboxOldestAgeSeconds
        workerProcessing = [long]$Sample.workerProcessing
        workerResultsPending = [long]$Sample.workerResultsPending
        postgresActiveConnections = [long]$Sample.postgres.activeConnections
        postgresTotalConnections = [long]$Sample.postgres.totalConnections
        postgresBlockedLocks = [long]$Sample.postgres.blockedLocks
        postgresActivePermits = [long]$Sample.postgres.activePermits
        postgresPermitLimitViolations = [long]$Sample.postgres.permitLimitViolations
        kafkaCommandLag = [long]$Sample.kafkaLag.commands
        kafkaResultLag = [long]$Sample.kafkaLag.results
        kafkaHeartbeatLag = [long]$Sample.kafkaLag.heartbeats
    }
    foreach ($entry in $values.GetEnumerator()) {
        if (-not $Peaks.ContainsKey($entry.Key) -or $entry.Value -gt $Peaks[$entry.Key]) { $Peaks[$entry.Key] = $entry.Value }
    }
}

function Get-DurableCounts {
    param([string]$RunId)
    $sql = "SELECT we.status, count(*) FROM workflow_execution we JOIN workflow_version wv ON wv.id=we.workflow_version_id WHERE wv.name='Load test $RunId' GROUP BY we.status ORDER BY we.status;"
    $output = docker compose exec -T postgres psql -U flowforge -d flowforge -At -F '|' -c $sql 2>$null
    $counts = [ordered]@{}
    foreach ($line in $output) {
        if ($line -match '^([A-Z_]+)\|(\d+)$') { $counts[$Matches[1]] = [long]$Matches[2] }
    }
    return $counts
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
    $output = docker compose exec -T postgres psql -U flowforge -d flowforge -At -c $sql 2>$null
    foreach ($line in $output) {
        if (-not [string]::IsNullOrWhiteSpace($line)) { $causes += ($line | ConvertFrom-Json) }
    }
    return $causes
}

function Get-CounterDistribution {
    param([array]$Before, [array]$After, [string]$Service, [string]$Counter)
    $rows = @()
    foreach ($current in @($After | Where-Object service -eq $Service)) {
        $previous = $Before | Where-Object instance -eq $current.instance | Select-Object -First 1
        $start = if ($previous) { [double]$previous.$Counter } else { 0 }
        $rows += [ordered]@{ instance=$current.instance; port=$current.port; delta=[long]([double]$current.$Counter - $start) }
    }
    return $rows
}

function Assert-KafkaPartitions {
    $topics = @(
        "flowforge.task.commands.v1",
        "flowforge.task.results.v1",
        "flowforge.task.heartbeats.v1"
    )
    foreach ($topic in $topics) {
        $description = docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh `
            --bootstrap-server localhost:9092 --describe --topic $topic 2>$null
        $partitionMatch = [regex]::Match(($description -join "`n"), 'PartitionCount:\s*(\d+)')
        if (-not $partitionMatch.Success -or [int]$partitionMatch.Groups[1].Value -ne [int]$topologyConfig.kafkaPartitions) {
            throw "Kafka topic $topic partition count does not match topology; use -ResetData when reducing partitions"
        }
    }
}

function Stop-Applications {
    param([array]$Applications)
    foreach ($application in $Applications) {
        if (-not $application.process.HasExited) {
            $application.process.CloseMainWindow() | Out-Null
        }
    }
    Start-Sleep -Seconds 2
    foreach ($application in $Applications) {
        $application.process.Refresh()
        if (-not $application.process.HasExited) { Stop-Process -Id $application.process.Id -Force -ErrorAction SilentlyContinue }
    }
}

Assert-Topology
Assert-FaultPlan
$applications = @()
$loadProcess = $null
$faultProcesses = @()
$faultEventPaths = @()
$locationPushed = $false
$combinedReportPath = Join-Path $OutputDirectory "report.json"
try {
    Invoke-Build
    Push-Location $repositoryRoot
    $locationPushed = $true
    if ($ResetData) {
        if ($requiresTelemetry) {
            docker compose --profile observability down --volumes --remove-orphans
        } else {
            docker compose down --volumes --remove-orphans
        }
        if ($LASTEXITCODE -ne 0) { throw "Failed to reset Compose data" }
    }
    if ($requiresTelemetry) {
        docker compose --profile observability up -d postgres kafka redis prometheus otel-collector
    } else {
        docker compose up -d postgres kafka redis
    }
    if ($LASTEXITCODE -ne 0) { throw "Failed to start infrastructure" }
    Wait-Infrastructure

    $controlJar = (Resolve-Path "flowforge-control-plane/target/flowforge-control-plane-0.1.0-SNAPSHOT-exec.jar").Path
    $workerJar = (Resolve-Path "flowforge-worker/target/flowforge-worker-0.1.0-SNAPSHOT.jar").Path
    $loadJar = (Resolve-Path "flowforge-load-test/target/flowforge-load-test.jar").Path
    $controlApplications = @()
    $index = 0
    foreach ($port in @($topologyConfig.controlPlane.ports)) {
        $application = Start-FlowForgeProcess "control-plane" $index ([int]$port) $controlJar `
            ([int]$topologyConfig.controlPlane.databasePoolSize) ([int]$topologyConfig.controlPlane.maxHeapMb)
        $controlApplications += $application
        $applications += $application
        $index++
    }
    Wait-Applications $controlApplications
    Assert-KafkaPartitions

    $workerApplications = @()
    $index = 0
    foreach ($port in @($topologyConfig.worker.ports)) {
        $application = Start-FlowForgeProcess "worker" $index ([int]$port) $workerJar `
            ([int]$topologyConfig.worker.databasePoolSize) ([int]$topologyConfig.worker.maxHeapMb)
        $workerApplications += $application
        $applications += $application
        $index++
    }
    Wait-Applications $workerApplications
    $telemetryBefore = if ($requiresTelemetry) { Wait-TelemetryRecovery } else { $null }

    $profileConfig | Add-Member -NotePropertyName baseUrls -NotePropertyValue @(
        $topologyConfig.controlPlane.ports | ForEach-Object { "http://localhost:$_" }
    ) -Force
    $profileConfig.PSObject.Properties.Remove("baseUrl")
    $derivedProfilePath = Join-Path $OutputDirectory "profile.json"
    Write-Utf8Json $profileConfig $derivedProfilePath
    $workloadReportPath = Join-Path $OutputDirectory "workload.json"
    $beforeMetrics = @(Get-ApplicationMetrics $applications)
    $loadStdout = Join-Path $OutputDirectory "load-generator.log"
    $loadStderr = Join-Path $OutputDirectory "load-generator.err.log"
    $loadExitCodePath = Join-Path $OutputDirectory "load-generator.exit-code"
    $loadProcessHost = (Get-Process -Id $PID).Path
    $loadProcess = Start-BackgroundProcess -FilePath $loadProcessHost `
        -ArgumentList @(
            "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass",
            "-File", ('"' + (Join-Path $repositoryRoot "scripts/invoke-load-generator.ps1") + '"'),
            "-Jar", ('"' + $loadJar + '"'),
            "-Profile", ('"' + $derivedProfilePath + '"'),
            "-Report", ('"' + $workloadReportPath + '"'),
            "-ExitCodeFile", ('"' + $loadExitCodePath + '"')
        ) `
        -RedirectStandardOutput $loadStdout -RedirectStandardError $loadStderr

    if ($faultConfig) {
        $faultIndex = 0
        foreach ($action in @($faultConfig.actions)) {
            $eventPath = Join-Path $OutputDirectory ("fault-{0}-{1}.json" -f $faultIndex, $action.service)
            $faultEventPaths += $eventPath
            $faultProcess = Start-BackgroundProcess -FilePath $loadProcessHost `
                -ArgumentList @(
                    "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass",
                    "-File", ('"' + (Join-Path $repositoryRoot "scripts/invoke-dependency-fault.ps1") + '"'),
                    "-Service", [string]$action.service,
                    "-Operation", [string]$action.operation,
                    "-DelaySeconds", [string]$action.delaySeconds,
                    "-DurationSeconds", [string]$action.durationSeconds,
                    "-RepositoryRoot", ('"' + $repositoryRoot + '"'),
                    "-EventPath", ('"' + $eventPath + '"'),
                    "-CoordinationNamespace", $coordinationNamespace
                )
            $faultProcesses += $faultProcess
            $faultIndex++
        }
    }

    $samples = @()
    $peaks = @{}
    do {
        $sample = Get-ProbeSample $applications
        $samples += $sample
        Update-Peaks $peaks $sample
        Start-Sleep -Seconds ([int]$topologyConfig.probeIntervalSeconds)
        $loadProcess.Refresh()
    } while (-not $loadProcess.HasExited)
    $loadProcess.WaitForExit()
    if (-not (Test-Path -LiteralPath $loadExitCodePath)) {
        throw "Load generator did not persist its exit code; inspect $loadStderr"
    }
    $loadExitCode = [int](Get-Content -Raw -LiteralPath $loadExitCodePath)
    foreach ($faultProcess in $faultProcesses) {
        if (-not $faultProcess.WaitForExit(65000)) { throw "Fault injector did not finish within its bounded deadline" }
    }
    $telemetryRecovered = if ($requiresTelemetry) { Wait-TelemetryRecovery } else { $null }
    Start-Sleep -Seconds ([int]$topologyConfig.postRunDrainSeconds)
    $finalSample = Get-ProbeSample $applications
    $samples += $finalSample
    Update-Peaks $peaks $finalSample
    $afterMetrics = @(Get-ApplicationMetrics $applications)
    if (-not (Test-Path -LiteralPath $workloadReportPath)) {
        throw "Load generator did not produce a report; inspect $loadStderr"
    }
    $workload = Get-Content -Raw -LiteralPath $workloadReportPath | ConvertFrom-Json
    $faultEvents = @($faultEventPaths | ForEach-Object {
        if (-not (Test-Path -LiteralPath $_ -PathType Leaf)) { throw "Fault injector did not produce event evidence: $_" }
        Get-Content -Raw -LiteralPath $_ | ConvertFrom-Json
    })
    $telemetryAfter = $null
    if ($requiresTelemetry) {
        $telemetryRestore = @($faultEvents | Where-Object { $_.service -in @("prometheus", "otel-collector") } | ForEach-Object { $_.restoredAt } | Sort-Object | Select-Object -Last 1)[0]
        Start-Sleep -Seconds 6
        $telemetryAfter = Get-TelemetryProbe $telemetryRestore
    }
    $durableCounts = Get-DurableCounts ([string]$workload.runId)
    $durableFailureCauses = @(Get-DurableFailureCauses ([string]$workload.runId))
    $workerDistribution = @(Get-CounterDistribution $beforeMetrics $afterMetrics "worker" "commandsCompleted")
    $controlDistribution = @(Get-CounterDistribution $beforeMetrics $afterMetrics "control-plane" "resultsConsumed")
    $duplicateDistribution = @(Get-CounterDistribution $beforeMetrics $afterMetrics "worker" "commandDuplicates")
    $redisFailureDistribution = @(Get-CounterDistribution $beforeMetrics $afterMetrics "control-plane" "redisCoordinationFailures")
    $reconciliationDistribution = @(Get-CounterDistribution $beforeMetrics $afterMetrics "control-plane" "coordinationReconciliations")
    $executionsObserved = [long](($durableCounts.Values | Measure-Object -Sum).Sum)
    $correctnessPassed = if ($workload.mode -eq "API") {
        $executionsObserved -eq [long]$workload.counts.accepted -and [long]$durableCounts.SUCCEEDED -eq [long]$workload.counts.succeeded
    } else {
        $executionsObserved -ge [long]$workload.counts.succeeded
    }
    $hostMemory = try { [long](Get-CimInstance Win32_ComputerSystem).TotalPhysicalMemory } catch { 0L }
    $javaVersion = (& java --version | Select-Object -First 1).ToString()
    $dockerVersion = (& docker version --format '{{.Server.Version}}').ToString()
    $gitCommit = (& git rev-parse HEAD).ToString()
    & git diff --quiet
    $gitStatus = & git status --porcelain | Out-String
    $gitDirty = $LASTEXITCODE -ne 0 -or -not [string]::IsNullOrWhiteSpace($gitStatus)
    $faultEvaluation = $null
    if ($faultConfig) {
        if (@($faultEvents | Where-Object { -not $_.passed }).Count -gt 0) {
            $failedFaults = @($faultEvents | Where-Object { -not $_.passed } | ForEach-Object { "$($_.service)/$($_.operation): $($_.error)" }) -join "; "
            throw "Fault injection failed: $failedFaults"
        }
        $latestRestore = @($faultEvents | ForEach-Object { [DateTimeOffset]::Parse($_.restoredAt) } | Sort-Object | Select-Object -Last 1)[0]
        $workloadStarted = [DateTimeOffset]::Parse($workload.startedAt)
        $workloadFinished = [DateTimeOffset]::Parse($workload.finishedAt)
        $faultStarted = @($faultEvents | ForEach-Object { [DateTimeOffset]::Parse($_.faultStartedAt) } | Sort-Object | Select-Object -First 1)[0]
        $coveredByWorkload = $workloadStarted -le $faultStarted -and $workloadFinished -ge $latestRestore
        $recoverySeconds = [Math]::Max(0, ($workloadFinished - $latestRestore).TotalSeconds)
        $suppressedCommandDuplicates = [long](($duplicateDistribution | ForEach-Object { [long]$_.delta } | Measure-Object -Sum).Sum)
        $durableSucceeded = if ($durableCounts.Contains("SUCCEEDED")) { [long]$durableCounts.SUCCEEDED } else { 0L }
        $durableFailures = [Math]::Max(0, $executionsObserved - $durableSucceeded)
        $redisCoordinationFailures = [long](($redisFailureDistribution | ForEach-Object { [long]$_.delta } | Measure-Object -Sum).Sum)
        $coordinationReconciliations = [long](($reconciliationDistribution | ForEach-Object { [long]$_.delta } | Measure-Object -Sum).Sum)
        $permitLimitViolations = [long](($samples | ForEach-Object { [long]$_.postgres.permitLimitViolations } | Measure-Object -Maximum).Maximum)
        $checks = @(
            [ordered]@{ metric="injectorsRestored"; passed=@($faultEvents | Where-Object { -not $_.passed }).Count -eq 0 },
            [ordered]@{ metric="coveredByWorkload"; passed=$coveredByWorkload },
            [ordered]@{ metric="recoverySeconds"; operator="<="; limit=[double]$faultConfig.thresholds.maximumRecoverySeconds; actual=[Math]::Round($recoverySeconds, 6); passed=$recoverySeconds -le [double]$faultConfig.thresholds.maximumRecoverySeconds },
            [ordered]@{ metric="suppressedCommandDuplicates"; operator="<="; limit=[long]$faultConfig.thresholds.maximumSuppressedCommandDuplicates; actual=$suppressedCommandDuplicates; passed=$suppressedCommandDuplicates -le [long]$faultConfig.thresholds.maximumSuppressedCommandDuplicates },
            [ordered]@{ metric="durableFailures"; operator="<="; limit=[long]$faultConfig.thresholds.maximumDurableFailures; actual=$durableFailures; passed=$durableFailures -le [long]$faultConfig.thresholds.maximumDurableFailures }
        )
        if ($null -ne $faultConfig.thresholds.minimumRedisCoordinationFailures) {
            $checks += [ordered]@{ metric="redisCoordinationFailures"; operator=">="; limit=[long]$faultConfig.thresholds.minimumRedisCoordinationFailures; actual=$redisCoordinationFailures; passed=$redisCoordinationFailures -ge [long]$faultConfig.thresholds.minimumRedisCoordinationFailures }
        }
        if ($null -ne $faultConfig.thresholds.minimumCoordinationReconciliations) {
            $checks += [ordered]@{ metric="coordinationReconciliations"; operator=">="; limit=[long]$faultConfig.thresholds.minimumCoordinationReconciliations; actual=$coordinationReconciliations; passed=$coordinationReconciliations -ge [long]$faultConfig.thresholds.minimumCoordinationReconciliations }
        }
        if ($null -ne $faultConfig.thresholds.maximumPermitLimitViolations) {
            $checks += [ordered]@{ metric="permitLimitViolations"; operator="<="; limit=[long]$faultConfig.thresholds.maximumPermitLimitViolations; actual=$permitLimitViolations; passed=$permitLimitViolations -le [long]$faultConfig.thresholds.maximumPermitLimitViolations }
        }
        if ($faultConfig.thresholds.requireTelemetryRecovery) {
            $telemetryPassed = $telemetryAfter.prometheusReady -and $telemetryAfter.healthyApplicationTargets -ge 2 -and $telemetryAfter.collectorRunning -and $telemetryAfter.collectorPortOpen -and $telemetryAfter.traceBatchesAfterRestore -gt 0
            $checks += [ordered]@{ metric="telemetryRecovered"; expected=$true; actual=[bool]$telemetryPassed; passed=[bool]$telemetryPassed }
        }
        $faultEvaluation = [ordered]@{
            plan = $faultConfig
            events = $faultEvents
            coveredByWorkload = $coveredByWorkload
            recoverySeconds = [Math]::Round($recoverySeconds, 6)
            suppressedCommandDuplicates = $suppressedCommandDuplicates
            durableFailures = $durableFailures
            redisCoordinationFailures = $redisCoordinationFailures
            coordinationReconciliations = $coordinationReconciliations
            permitLimitViolations = $permitLimitViolations
            checks = $checks
            telemetry = if ($requiresTelemetry) { [ordered]@{ before=$telemetryBefore; recovered=$telemetryRecovered; after=$telemetryAfter } } else { $null }
        }
        $faultEvaluation.passed = @($faultEvaluation.checks | Where-Object { -not $_.passed }).Count -eq 0
    }
    $combined = [ordered]@{
        schemaVersion = 2
        generatedAt = (Get-Date).ToUniversalTime().ToString("o")
        topology = $topologyConfig
        environment = [ordered]@{
            machine = $env:COMPUTERNAME
            operatingSystem = [Environment]::OSVersion.VersionString
            processors = [Environment]::ProcessorCount
            physicalMemoryBytes = $hostMemory
            java = $javaVersion
            docker = $dockerVersion
            gitCommit = $gitCommit
            gitDirty = $gitDirty
        }
        workload = $workload
        probes = [ordered]@{
            intervalSeconds = [int]$topologyConfig.probeIntervalSeconds
            sampleCount = $samples.Count
            peaks = $peaks
            samples = $samples
        }
        distribution = [ordered]@{
            workerCommandsCompleted = $workerDistribution
            workerCommandDuplicates = $duplicateDistribution
            controlPlaneResultsConsumed = $controlDistribution
        }
        durableExecutionCounts = $durableCounts
        durableFailureCauses = $durableFailureCauses
        faultInjection = $faultEvaluation
        correctnessPassed = $correctnessPassed
        loadGeneratorExitCode = $loadExitCode
        passed = ([bool]$workload.passed -and $correctnessPassed -and $loadExitCode -eq 0 -and (-not $faultEvaluation -or $faultEvaluation.passed))
    }
    Write-Utf8Json $combined $combinedReportPath 30
    if (-not $combined.passed) { throw "Topology load thresholds or durable correctness failed; inspect $combinedReportPath" }
    Write-Host "Topology report: $combinedReportPath"
} finally {
    if ($loadProcess -and -not $loadProcess.HasExited) { Stop-Process -Id $loadProcess.Id -Force -ErrorAction SilentlyContinue }
    foreach ($faultProcess in $faultProcesses) {
        if (-not $faultProcess.HasExited) { Stop-Process -Id $faultProcess.Id -Force -ErrorAction SilentlyContinue }
    }
    if ($faultConfig) {
        foreach ($service in @($faultConfig.actions.service | Sort-Object -Unique)) {
            $previousErrorPreference = $ErrorActionPreference
            try {
                $ErrorActionPreference = "SilentlyContinue"
                docker compose unpause $service 2>&1 | Out-Null
            } finally {
                $ErrorActionPreference = $previousErrorPreference
            }
        }
    }
    Stop-Applications $applications
    if (-not $KeepInfrastructure) {
        if ($requiresTelemetry) {
            docker compose --profile observability stop postgres kafka redis prometheus otel-collector | Out-Null
        } else {
            docker compose stop postgres kafka redis | Out-Null
        }
    }
    if ($locationPushed) { Pop-Location }
}
