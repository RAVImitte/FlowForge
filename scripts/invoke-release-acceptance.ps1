param()

$ErrorActionPreference = "Stop"
$repositoryRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$qualityGate = Join-Path $PSScriptRoot "invoke-quality-gate.ps1"
$gates = @("Security", "Upgrade", "Recovery", "Full", "Manifests", "Sbom")
$completed = [System.Collections.Generic.List[string]]::new()

Push-Location $repositoryRoot
try {
    foreach ($gate in $gates) {
        Write-Host "Running release acceptance gate: $gate"
        & $qualityGate -Gate $gate
        if ($LASTEXITCODE -ne 0) {
            throw "Release acceptance gate $gate failed with exit code $LASTEXITCODE"
        }
        $completed.Add($gate)
    }
} finally {
    Pop-Location
}

Write-Host ("Release acceptance passed: " + ($completed -join ", "))
