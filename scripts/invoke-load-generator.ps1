param(
    [Parameter(Mandatory)][string]$Jar,
    [Parameter(Mandatory)][string]$Profile,
    [Parameter(Mandatory)][string]$Report,
    [Parameter(Mandatory)][string]$ExitCodeFile
)

$ErrorActionPreference = "Stop"
try {
    & java -jar $Jar $Profile $Report
    $code = $LASTEXITCODE
} catch {
    Write-Error $_
    $code = 1
} finally {
    [System.IO.File]::WriteAllText(
        $ExitCodeFile,
        [string]$code,
        [System.Text.UTF8Encoding]::new($false)
    )
}
exit $code
