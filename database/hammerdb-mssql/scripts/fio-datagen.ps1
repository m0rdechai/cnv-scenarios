# FIO-based data generator for CNV extra disks.
# Standalone reference script. check.sh uses equivalent inline PowerShell
# snippets at runtime (EncodedCommand). Run this script manually for debugging.
#
# Modes:
#   preflight  - Check/install FIO; exit 0 if ready, 1 if failed
#   generate   - Build FIO job file, run FIO on target drives
#   validate   - Audit generated data per drive, emit DATAGEN_RESULT lines
#
# Parameters (set below or override before running):
#   $Mode             preflight | generate | validate
#   $DirectoryCount   numjobs (directories per disk)
#   $FilesPerDir      nrfiles (files per directory)
#   $FileSize         filesize (e.g. 1G, 100M)
#   $DepthCount       directory nesting depth
#   $FioUrl           MSI download URL for runtime install
#   $FioSha256        Expected SHA-256 of the MSI at $FioUrl. Defaults to the known-good
#                      hash for the default fio-3.38-x64.msi release asset. Update this if
#                      you override $FioUrl. If blank, install is blocked unless
#                      $AllowUnpinnedFioInstall is set to $true (fail-closed by default).
#   $AllowUnpinnedFioInstall  Allow installing an unverified MSI when $FioSha256 is blank.
#                      Defaults to $false.
#   $ExcludeDrives    Array of drive letters to skip (e.g. C,D)

$ErrorActionPreference = 'Stop'

$Mode                     = '__MODE__'
$DirectoryCount           = [int]'__DIR_COUNT__'
$FilesPerDir              = [int]'__FILES_PER_DIR__'
$FileSize                 = '__FILE_SIZE__'
$DepthCount               = [int]'__DEPTH_COUNT__'
$FioUrl                   = '__FIO_URL__'
$FioSha256                = '1D450FD538E5EF90A05AAF5BD88E457970CB009832B344AD069D3B3C48BF2C1C'
$AllowUnpinnedFioInstall  = $false
$ExcludeDrives            = @('__EXCLUDE_DRIVES__' -split ',')

$DirPrefix  = 'fio_data_dir_'
$FilePrefix = 'bench_file_'

function Get-TargetVolumes {
    $vols = Get-Volume | Where-Object {
        $_.DriveLetter -and
        $_.DriveType -eq 'Fixed' -and
        $_.DriveLetter -notin $ExcludeDrives
    }
    return $vols
}

function Build-FilenameFormat {
    if ($DepthCount -le 1) {
        return "${DirPrefix}`$jobnum\${FilePrefix}`$filenum.dat"
    }
    $parts = @()
    for ($i = 0; $i -lt $DepthCount; $i++) {
        $parts += "${DirPrefix}`$jobnum"
    }
    $dirPath = $parts -join '\'
    return "${dirPath}\${FilePrefix}`$filenum.dat"
}

# ─── PREFLIGHT ───────────────────────────────────────────────────────────────
if ($Mode -eq 'preflight') {
    $fio = Get-Command fio.exe -ErrorAction SilentlyContinue
    if ($fio) {
        $ver = & fio.exe --version 2>&1
        Write-Output "FIO_FOUND=$($fio.Source) version=$ver"
        exit 0
    }

    Write-Output "FIO_NOT_FOUND - deploying from $FioUrl"
    try {
        $installer = "$env:TEMP\fio-install.msi"
        [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
        Invoke-WebRequest -Uri $FioUrl -OutFile $installer -UseBasicParsing -TimeoutSec 120

        $actualHash = (Get-FileHash -Path $installer -Algorithm SHA256).Hash
        if ($FioSha256) {
            if ($actualHash -ne $FioSha256.ToUpper()) {
                Write-Output "FIO_DEPLOY_FAILED integrity_check_failed expected=$FioSha256 actual=$actualHash"
                Remove-Item $installer -Force -ErrorAction SilentlyContinue
                exit 1
            }
            Write-Output "FIO_INTEGRITY_OK sha256=$actualHash"
        } elseif ($AllowUnpinnedFioInstall) {
            Write-Output "FIO_INTEGRITY_UNPINNED sha256=$actualHash - pin this as `$FioSha256 if this URL is trusted"
        } else {
            Write-Output "FIO_DEPLOY_FAILED unpinned_install_blocked sha256=$actualHash - set `$FioSha256 or `$AllowUnpinnedFioInstall = `$true"
            Remove-Item $installer -Force -ErrorAction SilentlyContinue
            exit 1
        }

        $proc = Start-Process msiexec.exe -ArgumentList "/i `"$installer`" /qn /norestart" -Wait -PassThru -NoNewWindow
        if ($proc.ExitCode -ne 0) {
            Write-Output "FIO_DEPLOY_FAILED msiexec_exit=$($proc.ExitCode)"
            exit 1
        }
        $env:PATH += ";C:\Program Files\fio"
        $fio = Get-Command fio.exe -ErrorAction SilentlyContinue
        if ($fio) {
            $ver = & fio.exe --version 2>&1
            Write-Output "FIO_DEPLOYED=$($fio.Source) version=$ver"
            exit 0
        }
        Write-Output "FIO_DEPLOY_FAILED fio.exe not found after install"
        exit 1
    } catch {
        Write-Output "FIO_DEPLOY_FAILED error=$($_.Exception.Message)"
        exit 1
    }
}

# ─── GENERATE ────────────────────────────────────────────────────────────────
if ($Mode -eq 'generate') {
    $TargetVolumes = Get-TargetVolumes
    if ($TargetVolumes.Count -eq 0) {
        Write-Output "DATAGEN_NO_TARGET_DRIVES"
        exit 1
    }

    # Skip if data already exists with correct dir count on all target drives
    $allExist = $true
    foreach ($vol in $TargetVolumes) {
        $root = "$($vol.DriveLetter):\"
        $existing = @(Get-ChildItem -Path $root -Directory -Filter "${DirPrefix}*" -ErrorAction SilentlyContinue)
        if ($existing.Count -ne $DirectoryCount) {
            $allExist = $false
            break
        }
    }
    if ($allExist) {
        Write-Output "DATAGEN_SKIPPED data already exists with correct dir count on all drives"
        exit 0
    }

    Write-Output "DATAGEN_STARTING drives=$(($TargetVolumes.DriveLetter) -join ',') dirs=$DirectoryCount files=$FilesPerDir size=$FileSize"

    $fnFormat = Build-FilenameFormat

    $fioExe = (Get-Command fio.exe -ErrorAction SilentlyContinue).Source
    if (-not $fioExe) {
        $fioExe = "C:\Program Files\fio\fio.exe"
    }

    # FIO 3.38 on Windows has a bug where the directory= option in job files
    # fails with "lstat: No such file or directory". Workaround: Set-Location
    # to each drive root and run FIO without a directory= option per drive.
    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    $anyFail = $false
    foreach ($vol in $TargetVolumes) {
        Set-Location "$($vol.DriveLetter):\"
        $FioJobPath = "$env:TEMP\cnv_fio_$($vol.DriveLetter).fio"
        @"
[global]
rw=write
bs=1M
refill_buffers
scramble_buffers=1
numjobs=$DirectoryCount
nrfiles=$FilesPerDir
filesize=$FileSize
filename_format=$fnFormat

[fill_$($vol.DriveLetter)]
"@ | Out-File -FilePath $FioJobPath -Encoding ascii

        & $fioExe $FioJobPath 2>&1 | ForEach-Object { Write-Output $_ }
        if ($LASTEXITCODE -ne 0) {
            $anyFail = $true
            Write-Output "DATAGEN_DRIVE_FAILED drive=$($vol.DriveLetter) exit=$LASTEXITCODE"
        }
        Remove-Item $FioJobPath -Force -ErrorAction SilentlyContinue
    }
    $sw.Stop()

    if ($anyFail) {
        Write-Output "DATAGEN_FIO_FAILED elapsed=$([math]::Round($sw.Elapsed.TotalSeconds))s"
        exit 1
    }

    Write-Output "DATAGEN_COMPLETE elapsed=$([math]::Round($sw.Elapsed.TotalSeconds))s"
    exit 0
}

# ─── VALIDATE ────────────────────────────────────────────────────────────────
if ($Mode -eq 'validate') {
    $TargetVolumes = Get-TargetVolumes
    if ($TargetVolumes.Count -eq 0) {
        Write-Output "DATAGEN_NO_TARGET_DRIVES"
        exit 1
    }

    $overallPass = $true
    foreach ($vol in $TargetVolumes) {
        $root = "$($vol.DriveLetter):\"
        $generatedDirs = @(Get-ChildItem -Path $root -Directory -Filter "${DirPrefix}*" -ErrorAction SilentlyContinue)
        $totalFiles = 0
        $totalBytes = [long]0
        foreach ($d in $generatedDirs) {
            $files = @(Get-ChildItem -Path $d.FullName -File -Filter "${FilePrefix}*" -Recurse -ErrorAction SilentlyContinue)
            $totalFiles += $files.Count
            foreach ($f in $files) { $totalBytes += $f.Length }
        }
        $usedGB = [math]::Round($totalBytes / 1GB, 2)
        $expectedDirs = $DirectoryCount
        $expectedFiles = $DirectoryCount * $FilesPerDir
        $status = if ($generatedDirs.Count -eq $expectedDirs -and $totalFiles -eq $expectedFiles) { "PASS" } else { "FAIL" }
        if ($status -eq "FAIL") { $overallPass = $false }
        Write-Output "DATAGEN_RESULT:drive=$($vol.DriveLetter):dirs=$($generatedDirs.Count)/$expectedDirs`:files=$totalFiles/$expectedFiles`:usedGB=$usedGB`:status=$status"
    }

    if ($overallPass) { exit 0 } else { exit 1 }
}

Write-Output "DATAGEN_ERROR unknown mode=$Mode"
exit 1
