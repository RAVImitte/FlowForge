param(
    [ValidateSet("smoke", "overload", "soak", "scheduled")]
    [string]$Profile = "smoke",
    [string]$Output,
    [switch]$SkipBuild
)

$ErrorActionPreference = "Stop"
$repositoryRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$profilePath = Join-Path $repositoryRoot "load-testing/profiles/$Profile.json"
$jarPath = Join-Path $repositoryRoot "flowforge-load-test/target/flowforge-load-test.jar"
if (-not $Output) {
    $timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
    $Output = Join-Path $repositoryRoot "load-testing/results/$Profile-$timestamp.json"
}

if (-not $SkipBuild) {
    Push-Location $repositoryRoot
    try {
        & ".\mvnw.cmd" -q -pl flowforge-load-test -am package '-DskipTests'
        if ($LASTEXITCODE -ne 0) { throw "Load-test build failed" }
    } finally {
        Pop-Location
    }
}
if (-not (Test-Path -LiteralPath $jarPath)) {
    throw "Load-test executable is missing: $jarPath"
}

& java -jar $jarPath $profilePath $Output
if ($LASTEXITCODE -ne 0) {
    throw "Load-test thresholds failed; inspect $Output"
}
Write-Host "Load report: $Output"
