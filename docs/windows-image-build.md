# Windows golden images for cnv-scenarios

This repository does **not** ship a default Windows disk URL. Build and publish your own disk, then set `windowsImageUrl` in your vars file or via environment when running tests with `guestOS: windows`. The value may be either a **`docker://...`** container-disk pullspec (CDI `registry` source) or an **`http(s)://...`** URL to a **`.qcow2`** (CDI `http` source).

## What the scenarios expect

### All Windows VM tests

- VM specs use **q35 + EFI** with **`features.smm.enabled: true`** so KubeVirt's validator accepts default EFI Secure Boot (Secure Boot requires SMM).
- **Windows Server 2022** (or compatible) with VirtIO drivers from [kubevirt/virtio-container-disk](https://quay.io/repository/kubevirt/virtio-container-disk).
- **QEMU guest agent** installed and running (required for `virtctl ssh` public-key injection via KubeVirt `accessCredentials` / `qemuGuestAgent` propagation). The VM must declare **`qemuGuestAgent.users`** (KubeVirt webhook): the account name must match **`vmUser`** in your vars (default **Administrator**).
- **OpenSSH Server** installed, `sshd` enabled, and **Administrator** SSH logins allowed. KubeVirt injects your CI key into the guest using the guest agent; the image must ship a supported OpenSSH + agent combination per your OpenShift Virtualization version.
- Firewall: allow **TCP 22** (SSH). For HammerDB/MSSQL, allow **TCP 1433** as needed.

### cpu-limits (Windows)

Validation Phase 4 counts guest processes whose `Win32_Process.CommandLine` contains **`CNV_CPU_BURN=1`** (excluding the validation query itself). The expected count equals `cpuCores x cpuSockets` -- one worker process per visible vCPU.

#### How it works (repo-side bootstrap)

The validator in `check_cpu_limits` (Phase 4) automatically bootstraps CPU burn workers on each Windows VM via SSH before counting them:

1. Checks if workers are already running (idempotent -- skips bootstrap if count is sufficient).
2. Builds a PowerShell script that launches one busy-loop `powershell.exe` per logical CPU using `Invoke-CimMethod Win32_Process.Create` (WMI process creation detaches workers from the SSH session so they survive after disconnect).
3. Encodes the script as base64 UTF-16LE and executes it via `powershell.exe -EncodedCommand` to bypass SSH quoting issues.
4. Waits 10 seconds for workers to initialize, then counts processes matching `CNV_CPU_BURN=1`.

**No image-side setup is required for cpu-limits.** The bootstrap is entirely repo-side.

#### Optional: pre-bake into the golden image

For faster validation or to skip the bootstrap wait, you can optionally pre-install workers in the image. Save this as `C:\Tools\cnv-cpu-burn.ps1`:

```powershell
$cpus = (Get-CimInstance Win32_ComputerSystem).NumberOfLogicalProcessors
1..$cpus | ForEach-Object {
    Start-Process powershell.exe -WindowStyle Hidden -ArgumentList @(
        '-NoProfile', '-Command',
        '$env:CNV_CPU_BURN="1"; [double]$x=1; while($true){ $x=[math]::Sqrt($x+1) }'
    )
}
```

Register a Scheduled Task to run it at boot:

```powershell
$action  = New-ScheduledTaskAction -Execute 'powershell.exe' `
    -Argument '-NoProfile -ExecutionPolicy Bypass -File C:\Tools\cnv-cpu-burn.ps1'
$trigger = New-ScheduledTaskTrigger -AtStartup
$principal = New-ScheduledTaskPrincipal -UserId 'SYSTEM' -RunLevel Highest -LogonType ServiceAccount
Register-ScheduledTask -TaskName 'cnv-cpu-burn' -Action $action -Trigger $trigger `
    -Principal $principal -Description 'CNV CPU burn workers for cpu-limits validation'
```

If workers are already running when validation starts, the bootstrap is skipped automatically.

#### Validation contract

| Requirement | Detail |
|---|---|
| One `powershell.exe` process per vCPU | Worker count must equal `NumberOfLogicalProcessors` |
| Marker in command line | Each worker command line must contain `CNV_CPU_BURN=1` |
| Self-count exclusion | The WMI query used for counting excludes its own `Get-CimInstance Win32_Process` invocation |
| Scales with topology | Reads processor count dynamically; works for any `cpuCores x cpuSockets` combination |

### memory-limits (Windows)

- Guest memory is validated via PowerShell / WMI.
- **Memory workload validation** is fully supported: Phase 4 bootstraps 4 `CNV_MEM_BURN=1` PowerShell workers via `Invoke-CimMethod Win32_Process.Create` (same pattern as cpu-limits). Each worker allocates 90%/4 of expected VM memory as a byte array, fills it with random data, and touches every 4KB page in a continuous loop. **No additional software is required in the image** -- the workload uses only built-in PowerShell capabilities.

### disk-limits / disk-hotplug (Windows)

- Data disks appear as VirtIO disks in the guest. No Linux `cloud-init` or `mount-hotplug-disks.sh` is used. OS-level checks use PowerShell (`Get-Disk`).

### nic-hotplug (Windows)

- Guest OS validation checks for VirtIO network driver presence (`Get-NetAdapter` with `InterfaceDescription` matching `*VirtIO*`), active interface count, and test IP assignment. **VirtIO network drivers must be installed in the image** (from the same [virtio-container-disk](https://quay.io/repository/kubevirt/virtio-container-disk) used for storage drivers). Without VirtIO network drivers, hot-plugged NICs will not appear in the guest.

### high-memory / large-disk (Windows)

- Guest memory and disk validation use the same PowerShell/WMI methods as memory-limits and disk-limits. No additional image requirements beyond the base Windows prerequisites.

### hammerdb-mssql

- **SQL Server 2022** (Express or higher as you prefer), **ODBC Driver 18** for SQL Server, **HammerDB 4.12** (or compatible).
- A **Scheduled Task** (e.g. `run_hammerdb`) that starts HammerDB at boot/logon so that validation Phase 10 can detect its exit and measure post-benchmark disk utilization.
- Validation is performed by `check_windows_vm` in `config/scripts/check.sh` -- a 12-phase modular checker driven entirely by `vars.yml`. The relevant phases for this scenario are:
  - **Phase 3** -- verifies `MSSQLSERVER` service is `Running` (or whichever service name is set in `validateApps`).
  - **Phase 7** -- initializes blank data DataVolumes (GPT + NTFS) so MSSQL can use them.
  - **Phase 8** -- verifies `dataDisks` non-system disks are present and total size matches `dataDisks x diskSize` (5% tolerance).
  - **Phase 9** -- reports disk utilization before HammerDB finishes; set `expectedDiskUtilGB=0` for report-only mode.
  - **Phase 10** -- polls for the `hammerdb` process/scheduled-task to exit (every 30s, up to `waitProcessTimeout` minutes), then asserts disk utilization is within `diskUtilTolerancePct`% of `expectedDiskUtilAfterProcessGB`.
  - **Phase 11** -- FIO data generation on extra disks (E:, F:, ...). Gated by `fillExtraDisks=true`. Checks/deploys FIO, generates high-entropy data via inline PowerShell (logic mirrors `database/hammerdb-mssql/scripts/fio-datagen.ps1`, which serves as a standalone reference), and validates per-drive dir/file/size counts. See [FIO data generation](#fio-data-generation-on-extra-disks) below.
  - **Phase 12** -- Aggregate total disk utilization across all non-C: drives (HammerDB on D: + FIO on E:/F:/...). Asserts against `expectedTotalDiskUtilGB` within `diskUtilTolerancePct`.

**`expectedOS` encoding:** The `expectedOS` value in `vars.yml` (e.g. `"Windows Server 2022"`) contains spaces. The `beforeCleanup` template encodes spaces as underscores before passing to the shell (`Windows_Server_2022`), and `check_windows_vm` decodes them back. This means the OS check performs a case-insensitive substring match for `"Windows Server 2022"` against the guest's `Win32_OperatingSystem.Caption`. Do not use literal underscores in `expectedOS` values unless they are part of the actual OS name.

### FIO data generation on extra disks

After HammerDB finishes writing MSSQL data to D:, the remaining data disks (E:, F:, ...) sit empty. Phase 11 fills them with high-entropy data using [FIO](https://github.com/axboe/fio) to create realistic storage utilization that defeats compression/dedup on Ceph-backed PVCs.

#### FIO deployment

FIO is deployed via one of two mechanisms (automatic fallback):

1. **Pre-installed in the golden image** (recommended). Install the [FIO 3.38 Windows MSI](https://github.com/axboe/fio/releases/download/fio-3.38/fio-3.38-x64.msi) during image build. The MSI installs to `C:\Program Files\fio\` and adds it to PATH.
2. **Runtime download** (fallback). If `fio.exe` is not found in PATH, Phase 11 downloads and installs the MSI from the URL in `fioUrl` (default: GitHub releases). Requires outbound HTTPS from the guest. Override `fioUrl` to an internal HTTP server for air-gapped environments.

To pre-install FIO in the image:

```powershell
Invoke-WebRequest -Uri "https://github.com/axboe/fio/releases/download/fio-3.38/fio-3.38-x64.msi" -OutFile "$env:TEMP\fio.msi" -UseBasicParsing
Start-Process msiexec.exe -ArgumentList '/i', "$env:TEMP\fio.msi", '/qn', '/norestart' -Wait
```

Or via `virt-customize` on the host:

```bash
curl -LO https://github.com/axboe/fio/releases/download/fio-3.38/fio-3.38-x64.msi
virt-customize -a winmssql2022.qcow2 --upload fio-3.38-x64.msi:/fio-install.msi \
  --firstboot-command 'msiexec /i C:\fio-install.msi /qn /norestart'
```

#### Configuration variables

| Variable | Default | Description |
|---|---|---|
| `fillExtraDisks` | `true` | Enable FIO data generation on non-C:/D: drives |
| `fioUrl` | GitHub releases (3.38) | MSI download URL for runtime install |
| `dirCount` | `5` | Directories per disk (FIO `numjobs`) |
| `filesPerDir` | `10` | Files per directory (FIO `nrfiles`) |
| `fileSize` | `"1G"` | Size of each file (FIO `filesize`) |
| `depthCount` | `1` | Directory nesting depth |
| `fioTimeout` | `30` | Maximum minutes for FIO generation |
| `expectedExtraDiskCapacityGB` | `0` | Expected FIO-written total (0 = report-only) |
| `expectedTotalDiskUtilGB` | `0` | Expected total across all non-C: drives (0 = report-only) |

Per-disk data = `dirCount x filesPerDir x fileSize`. Example: `5 x 10 x 1G = 50 GB` per extra disk.

#### Idempotency

If `fio_data_dir_*` directories already exist on a drive with the expected count, FIO generation is skipped for that run. This saves time on re-runs or debugging iterations.

## Building a qcow2 (high level)

1. Install Windows Server 2022 from ISO into a libvirt VM (or OpenShift Virtualization UI), with VirtIO drivers.
2. Install: Guest Agent, OpenSSH Server, (optional) SQL Server + HammerDB + scheduled task, (optional) FIO 3.38 for data generation on extra disks.
3. (Optional) For faster cpu-limits validation: copy `C:\Tools\cnv-cpu-burn.ps1` and register the `cnv-cpu-burn` Scheduled Task (see [cpu-limits (Windows)](#cpu-limits-windows) above). This is optional -- the validator bootstraps workers automatically via SSH if they are not already running.
4. Configure Administrator / policies / firewall as required.
5. Generalize or seal the image per your process (`sysprep /generalize` if you maintain a generalized golden layer; follow Microsoft licensing for evaluation media).
6. Export disk to **qcow2** (e.g. `qemu-img convert`).

## Publishing as a container disk for CDI

CDI `DataVolume` with a **registry** source expects a URL of the form:

`docker://registry.example.com/namespace/windows-cnv-scenarios:tag`

Example build/push steps (adjust registry, tags, and Dockerfile to match your org's standards):

```bash
# Example only -- replace REGISTRY/NAMESPACE/IMAGE:TAG
export IMG="quay.io/myorg/windows-cnv-scenarios:2025.01"

# Place disk.qcow2 beside a Dockerfile that uses kubevirt/containerdisks conventions
# or your internal wrapper image build.
podman build -t "${IMG}" -f Dockerfile.windows-cnv .
podman push "${IMG}"
```

Your `Dockerfile` / build pipeline must produce an image that CDI can import (see OpenShift documentation for **containerized data importer** and **DataVolume** `spec.source.registry`).

## HTTP(S) qcow2 import (CDI)

If you host a **`*.qcow2`** on an HTTP server reachable from the cluster (lab mirror, internal object gateway, etc.), set:

`windowsImageUrl: "http://host:port/path/your-image.qcow2"`

(or `https://...`). Scenario templates emit `spec.source.http` when `windowsImageUrl` does **not** start with `docker://`. The cluster must allow CDI to reach that URL (DNS, firewall, TLS trust for HTTPS).

## Runtime configuration

Use the `--os` flag to select Windows:

- `--os windows`: Run only the Windows variant
- `--os both`: Run both Linux and Windows variants
- `windowsImageUrl`: required for Windows -- either `docker://...` or `http(s)://.../.qcow2` (see above)
- `privateKey` / `publicKey`: same pattern as Linux scenarios (paths to key material).

The runner auto-corrects several variables when targeting Windows (see [Windows Auto-Corrections](../README.md#windows-auto-corrections) in the README):
`vmUser=Administrator`, `maxWaitTimeout=30m`, `windowsRootDiskSize=90Gi`, `max_ssh_retries=20`, `vmMemory=2Gi` (if below).

Minimal command-line example:

```bash
windowsImageUrl='http://host:port/path/image.qcow2' \
./run-workloads.sh cpu-limits memory-limits --mode sanity --os windows
```

Do **not** commit a real `windowsImageUrl` to the public repo if it points at private images; use local overrides or CI secrets.

## References

- [KubeVirt Windows documentation](https://kubevirt.io/user-guide/)
- OpenShift Virtualization: creating Windows VMs and virtio drivers
