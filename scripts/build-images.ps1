param(
    [string]$Registry = "flowforge",
    [string]$Tag = "0.1.0-SNAPSHOT",
    [ValidateSet("linux/amd64", "linux/arm64")]
    [string]$Platform = "linux/amd64",
    [string]$BuildTrustStore = "",
    [switch]$Push
)

$ErrorActionPreference = "Stop"
$repositoryRoot = Split-Path -Parent $PSScriptRoot

function Invoke-Checked {
    param([string[]]$Arguments)
    & docker @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "docker $($Arguments[0]) failed with exit code $LASTEXITCODE"
    }
}

if ([string]::IsNullOrWhiteSpace($Registry) -or [string]::IsNullOrWhiteSpace($Tag)) {
    throw "Registry and Tag must be non-empty."
}

Push-Location $repositoryRoot
try {
    & docker version --format "{{.Server.Version}}" | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "A running Docker-compatible engine is required." }

    $revision = (& git rev-parse HEAD).Trim()
    if ($LASTEXITCODE -ne 0) { throw "Could not resolve the Git revision." }
    $sourceDateEpoch = (& git show -s --format=%ct HEAD).Trim()
    if ($LASTEXITCODE -ne 0) { throw "Could not resolve the commit timestamp." }
    $sourceUrl = (& git remote get-url origin 2>$null)
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($sourceUrl)) { $sourceUrl = "unknown" }
    $sourceUrl = $sourceUrl.Trim()
    $sourceUrl = $sourceUrl -replace '^(https?://)[^/@]+@', '$1'

    if ([string]::IsNullOrWhiteSpace($BuildTrustStore)) {
        $javaCommand = Get-Command java -ErrorAction SilentlyContinue
        if ($null -ne $javaCommand) {
            $javaHome = Split-Path -Parent (Split-Path -Parent $javaCommand.Source)
            $detectedTrustStore = Join-Path $javaHome "lib/security/cacerts"
            if (Test-Path -LiteralPath $detectedTrustStore) { $BuildTrustStore = $detectedTrustStore }
        }
    }
    $buildSecretArguments = @()
    if (-not [string]::IsNullOrWhiteSpace($BuildTrustStore)) {
        $resolvedTrustStore = (Resolve-Path -LiteralPath $BuildTrustStore).Path
        $buildSecretArguments = @("--secret", "id=build_truststore,src=$resolvedTrustStore")
    }
    $outputMode = if ($Push) { "--push" } else { "--load" }
    foreach ($component in @("control-plane", "worker")) {
        $image = "$($Registry.TrimEnd('/'))/$component`:$Tag"
        $arguments = @(
            "buildx", "build",
            "--platform", $Platform,
            "--file", "docker/$component.Dockerfile",
            "--tag", $image,
            "--build-arg", "SOURCE_DATE_EPOCH=$sourceDateEpoch",
            "--build-arg", "VERSION=$Tag",
            "--build-arg", "REVISION=$revision",
            "--build-arg", "SOURCE_URL=$sourceUrl",
            "--provenance=true"
        ) + $buildSecretArguments + @(
            $outputMode,
            "."
        )
        Invoke-Checked $arguments

        if (-not $Push) {
            $user = (& docker image inspect $image --format "{{.Config.User}}").Trim()
            if ($LASTEXITCODE -ne 0 -or $user -ne "10001:10001") {
                throw "Image $image did not retain the required non-root user."
            }
            Write-Host "Validated $image (user $user)."
        }
    }
}
finally {
    Pop-Location
}
