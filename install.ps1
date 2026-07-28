#Requires -Version 5.0
<#
ee-database installer for Windows.

Source:   https://github.com/TOT-Concept/ee-database
License:  MIT
Releases: https://github.com/TOT-Concept/ee-database/releases

This installer is also open source. The URL below is an alias of the
canonical script in the repo:
    https://raw.githubusercontent.com/TOT-Concept/ee-database/main/install.ps1

Usage:
    iwr -useb https://entityenricher.ai/install-eedatabase.ps1 | iex

Pin a version:
    $env:EE_DATABASE_VERSION = "v1.0.0"; iwr -useb https://entityenricher.ai/install-eedatabase.ps1 | iex

Skip the prompt (CI):
    $env:EE_DATABASE_YES = "1"; iwr -useb https://entityenricher.ai/install-eedatabase.ps1 | iex
#>

$ErrorActionPreference = "Stop"

$repo    = "TOT-Concept/ee-database"
$version = if ($env:EE_DATABASE_VERSION) { $env:EE_DATABASE_VERSION } else { "latest" }

# Determine architecture (Windows ARM64 not yet released; fall back to x86_64).
$arch = if ([Environment]::Is64BitOperatingSystem) { "x86_64" } else {
    Write-Error "ee-database requires a 64-bit Windows OS."
}

$asset = "ee-database-windows-$arch.exe"
$baseUrl = if ($version -eq "latest") {
    "https://github.com/$repo/releases/latest/download"
} else {
    "https://github.com/$repo/releases/download/$version"
}
$url     = "$baseUrl/$asset"
$sigUrl  = "$baseUrl/$asset.sig"
$certUrl = "$baseUrl/$asset.pem"

# Sigstore keyless verification: releases are signed in CI by the release
# workflow's GitHub OIDC identity and logged in the public Rekor transparency
# log. There is no long-lived signing key anywhere — verification pins the
# exact workflow identity below.
$certIdentityRegexp = "^https://github\.com/TOT-Concept/ee-database/\.github/workflows/release\.yml@refs/tags/ee-database-v"
$certOidcIssuer     = "https://token.actions.githubusercontent.com"

# Install location: prefer %LOCALAPPDATA%\Programs\ee-database; fall back to USERPROFILE.
$destDir = Join-Path $env:LOCALAPPDATA "Programs\ee-database"
if (-not (Test-Path $destDir)) {
    New-Item -ItemType Directory -Force -Path $destDir | Out-Null
}
$dest = Join-Path $destDir "ee-database.exe"

Write-Host ""
Write-Host "ee-database installer"
Write-Host ""
Write-Host "  Source     https://github.com/$repo  (MIT)"
Write-Host "  Download   $url"
Write-Host "  Signature  Sigstore keyless (cosign) - signed by the release workflow's"
Write-Host "             GitHub OIDC identity, logged in the public Rekor transparency log"
Write-Host "  Install to $dest"
Write-Host "  Version    $version"
Write-Host ""
Write-Host "You can audit this script first:     iwr -useb https://entityenricher.ai/install-eedatabase.ps1"
Write-Host "You can pin a version:               `$env:EE_DATABASE_VERSION = 'v1.0.0'"
Write-Host "You can skip the prompt in CI:       `$env:EE_DATABASE_YES = '1'"
Write-Host "You can skip PATH setup:             `$env:EE_DATABASE_NO_PATH = '1'"
Write-Host "You can skip signature verification: `$env:EE_DATABASE_SKIP_VERIFY = '1'  (not recommended)"
Write-Host ""

if (-not $env:EE_DATABASE_YES -and [Environment]::UserInteractive) {
    for ($i = 5; $i -ge 1; $i--) {
        Write-Host -NoNewline "Continuing in $i seconds (Ctrl+C to abort)... `r"
        Start-Sleep -Seconds 1
    }
    Write-Host ""
    Write-Host ""
}

Write-Host "Downloading..."
$tmpBin = New-TemporaryFile
$tmpSig = $null
$tmpCert = $null
try {
    try {
        Invoke-WebRequest -Uri $url -OutFile $tmpBin -UseBasicParsing
    } catch {
        Write-Error "Download failed: $_"
    }

    # Signature verification — Sigstore keyless (skip with EE_DATABASE_SKIP_VERIFY=1).
    # The binary's .sig + the release workflow's Fulcio certificate are verified
    # against this repo's release.yml identity, with the entry logged in Rekor.
    if (-not $env:EE_DATABASE_SKIP_VERIFY) {
        if (-not (Get-Command cosign -ErrorAction SilentlyContinue)) {
            Write-Error @"
cosign is required to verify the binary signature.
  Install: winget install sigstore.cosign
  Docs:    https://docs.sigstore.dev/cosign/installation/
  Bypass:  `$env:EE_DATABASE_SKIP_VERIFY = '1'  (not recommended)
"@
        }
        $tmpSig = New-TemporaryFile
        $tmpCert = New-TemporaryFile
        try {
            Invoke-WebRequest -Uri $sigUrl -OutFile $tmpSig -UseBasicParsing
            Invoke-WebRequest -Uri $certUrl -OutFile $tmpCert -UseBasicParsing
        } catch {
            Write-Error "Could not download signature or signing certificate: $_"
        }
        # Capture cosign's exit code reliably: relax ErrorActionPreference around
        # the native call (stderr output under 'Stop' would otherwise abort or
        # mask $LASTEXITCODE) and collect output instead of piping it away.
        $prevEap = $ErrorActionPreference
        $ErrorActionPreference = "Continue"
        $cosignOutput = & cosign verify-blob `
            --certificate $tmpCert `
            --certificate-identity-regexp $certIdentityRegexp `
            --certificate-oidc-issuer $certOidcIssuer `
            --signature $tmpSig $tmpBin 2>&1
        $cosignExit = $LASTEXITCODE
        $ErrorActionPreference = $prevEap
        if ($cosignExit -ne 0) {
            Write-Host ($cosignOutput | Out-String)
            Write-Error @"
Signature verification FAILED for $asset
  The downloaded binary was not signed by the ee-database release workflow
  (or the download is partial/tampered). Aborting.
"@
        }
        Write-Host "OK Signature verified (Sigstore keyless, Rekor-logged)." -ForegroundColor Green
    } else {
        Write-Host "WARN EE_DATABASE_SKIP_VERIFY=1 set - proceeding without signature check." -ForegroundColor Yellow
    }

    Move-Item -Force $tmpBin $dest
} finally {
    Remove-Item -Force $tmpSig, $tmpCert -ErrorAction SilentlyContinue
    if (Test-Path $tmpBin) { Remove-Item -Force $tmpBin -ErrorAction SilentlyContinue }
}

Write-Host "OK Installed ee-database to $dest" -ForegroundColor Green
Write-Host ""

# === PATH setup ============================================================
# Add the install dir to the user PATH so `ee-database` resolves in new shells.
# Exact-match dedup against existing user + machine PATH entries (so we never
# add a duplicate). Also updates the current session's PATH so the binary is
# usable immediately without restarting the terminal. Opt out with
# $env:EE_DATABASE_NO_PATH = '1'.
$needsShellReload = $false
if (-not $env:EE_DATABASE_NO_PATH) {
    $userPath = [Environment]::GetEnvironmentVariable("Path", "User")
    $machinePath = [Environment]::GetEnvironmentVariable("Path", "Machine")
    $splitOpts = [StringSplitOptions]::RemoveEmptyEntries
    $userEntries = if ($userPath) { $userPath.Split(';', $splitOpts) } else { @() }
    $machineEntries = if ($machinePath) { $machinePath.Split(';', $splitOpts) } else { @() }
    $allEntries = @($userEntries + $machineEntries) | ForEach-Object { $_.TrimEnd('\') }
    $destDirNormalized = $destDir.TrimEnd('\')

    if ($allEntries -contains $destDirNormalized) {
        Write-Host "i  $destDir already on PATH (skipping)."
    } else {
        $newUserPath = if ($userPath) { "$userPath;$destDir" } else { $destDir }
        [Environment]::SetEnvironmentVariable("Path", $newUserPath, "User")
        # Update current session so `ee-database` works without a terminal restart.
        $env:Path = "$env:Path;$destDir"
        Write-Host "OK Added $destDir to user PATH" -ForegroundColor Green
        $needsShellReload = $true
    }
}

Write-Host ""
Write-Host "Next steps:"
Write-Host "  1. Open https://entityenricher.ai → Databases → Sync client"
Write-Host "  2. Pair a client and copy the pairing command, OR run:"
Write-Host "       ee-database pair --server https://entityenricher.ai"
Write-Host "  3. Then:"
Write-Host "       ee-database run --dsn <target-database-dsn> --save-dsn --create-missing"
if ($needsShellReload) {
    Write-Host ""
    Write-Host "WARN Open a new terminal so the updated PATH applies to other apps." -ForegroundColor Yellow
}
