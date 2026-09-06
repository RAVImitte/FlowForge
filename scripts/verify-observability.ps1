param(
    [string]$PrometheusImage = "prom/prometheus:v3.13.0"
)

$ErrorActionPreference = "Stop"
$repositoryRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$prometheusAssets = Join-Path $repositoryRoot "observability/prometheus"

docker run --rm --entrypoint /bin/promtool `
    --mount "type=bind,source=$prometheusAssets,target=/prometheus-assets,readonly" `
    $PrometheusImage check rules `
    /prometheus-assets/rules/flowforge-slo-recording.rules.yml `
    /prometheus-assets/rules/flowforge-alerts.rules.yml
if ($LASTEXITCODE -ne 0) { throw "Prometheus rule validation failed" }

docker run --rm --entrypoint /bin/promtool `
    --mount "type=bind,source=$prometheusAssets,target=/prometheus-assets,readonly" `
    $PrometheusImage test rules /prometheus-assets/tests/flowforge-alerts.test.yml
if ($LASTEXITCODE -ne 0) { throw "Prometheus rule tests failed" }

Push-Location $repositoryRoot
try {
    docker compose --profile observability config --quiet
    if ($LASTEXITCODE -ne 0) { throw "Compose observability profile validation failed" }
} finally {
    Pop-Location
}

Write-Host "FlowForge observability assets are valid."
