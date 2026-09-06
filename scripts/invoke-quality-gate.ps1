param(
    [Parameter(Mandatory)]
    [ValidateSet("Fast", "Architecture", "Security", "Migration", "Upgrade", "Recovery", "Full", "Manifests", "Sbom")]
    [string]$Gate
)

$ErrorActionPreference = "Stop"
$repositoryRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$isWindowsHost = [Environment]::OSVersion.Platform -eq [PlatformID]::Win32NT
$mavenWrapper = if ($isWindowsHost) { Join-Path $repositoryRoot "mvnw.cmd" } else { Join-Path $repositoryRoot "mvnw" }

function Invoke-Checked {
    param(
        [Parameter(Mandatory)][string]$Command,
        [Parameter(Mandatory)][string[]]$Arguments,
        [Parameter(Mandatory)][string]$Description
    )
    & $Command @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "$Description failed with exit code $LASTEXITCODE"
    }
}

function Invoke-Maven {
    param([Parameter(Mandatory)][string[]]$Arguments)
    $env:DEBUG = $null
    Invoke-Checked $mavenWrapper (@("--batch-mode", "--no-transfer-progress") + $Arguments) "Maven $Gate gate"
}

function Invoke-ManifestGate {
    foreach ($command in @("helm", "kubectl")) {
        if (-not (Get-Command $command -ErrorAction SilentlyContinue)) {
            throw "$command is required for the manifest gate"
        }
    }

    $chart = Join-Path $repositoryRoot "deploy/helm/flowforge"
    Invoke-Checked "helm" @("lint", $chart, "--strict") "Helm lint"

    $rendered = Join-Path ([IO.Path]::GetTempPath()) ("flowforge-{0}.yaml" -f [Guid]::NewGuid().ToString("N"))
    try {
        & helm template flowforge $chart --namespace flowforge `
            --set "controlPlane.image.digest=sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" `
            --set "worker.image.digest=sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" `
            --set "controlPlane.autoscaling.enabled=true" `
            --set "worker.autoscaling.enabled=true" `
            --set "trustStore.enabled=true" `
            --set "trustStore.secretName=flowforge-truststore" `
            --set-string "global.rotationToken=manifest-validation" | Set-Content -LiteralPath $rendered -Encoding utf8
        if ($LASTEXITCODE -ne 0) { throw "Helm template failed with exit code $LASTEXITCODE" }
        Invoke-Checked "kubectl" @("create", "--dry-run=client", "--validate=false", "-f", $rendered) "Kubernetes client validation"
    } finally {
        if (Test-Path -LiteralPath $rendered) { Remove-Item -LiteralPath $rendered -Force }
    }
}

Push-Location $repositoryRoot
try {
    switch ($Gate) {
        "Fast" {
            Invoke-Maven @("-Pfast", "clean", "verify")
        }
        "Architecture" {
            Invoke-Maven @(
                "-pl", "flowforge-control-plane", "-am",
                "-Dtest=ArchitectureTest", "-Dsurefire.failIfNoSpecifiedTests=false", "test"
            )
        }
        "Security" {
            Invoke-Maven @(
                "-pl", "flowforge-control-plane,flowforge-worker", "-am",
                "-Dtest=SecurityConfigurationTest,FlowForgeJwtAuthenticationConverterTest,TenantContextFilterTest,ProductionProfileTest,SecurityAuditIntegrationTest,TenantQuotaPersistenceIntegrationTest,DeadLetterReplayIntegrationTest,WorkerSecretRedactionTest,WorkerProductionProfileTest",
                "-Dsurefire.failIfNoSpecifiedTests=false", "test"
            )
        }
        "Migration" {
            Invoke-Maven @(
                "-pl", "flowforge-control-plane", "-am",
                "-Dtest=TenantMigrationIntegrationTest", "-Dsurefire.failIfNoSpecifiedTests=false", "test"
            )
        }
        "Upgrade" {
            Invoke-Maven @(
                "-pl", "flowforge-control-plane", "-am",
                "-Dtest=TenantMigrationIntegrationTest,SchemaRollbackCompatibilityIntegrationTest",
                "-Dsurefire.failIfNoSpecifiedTests=false", "test"
            )
        }
        "Recovery" {
            Invoke-Maven @(
                "-pl", "flowforge-control-plane", "-am",
                "-Dtest=PostgresBackupRestoreIntegrationTest,PostgresPointInTimeRecoveryIntegrationTest,SchemaRollbackCompatibilityIntegrationTest,CoordinationIntegrationTest,DeadLetterReplayIntegrationTest",
                "-Dsurefire.failIfNoSpecifiedTests=false", "test"
            )
        }
        "Full" {
            Invoke-Maven @("clean", "verify")
        }
        "Manifests" {
            Invoke-ManifestGate
        }
        "Sbom" {
            Invoke-Maven @("-Psbom", "-DskipTests", "verify")
            $sbom = Join-Path $repositoryRoot "target/sbom/flowforge.cdx.json"
            if (-not (Test-Path -LiteralPath $sbom -PathType Leaf) -or (Get-Item -LiteralPath $sbom).Length -eq 0) {
                throw "CycloneDX aggregate SBOM was not generated at $sbom"
            }
        }
    }
} finally {
    Pop-Location
}
