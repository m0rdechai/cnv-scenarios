---
description: Pitfalls and patterns for Windows VM templates and validation
globs:
  - "**/vm-*-windows*.yml"
  - "**/vm-windows*.yml"
  - "**/check.sh"
  - "**/windows-image-build.md"
  - "config/templates/vm-windows*.yml"
---

# Windows VM Development Guide

## VM Template Requirements

### CDI DataVolume Import

Windows VMs use CDI DataVolume imports. The template must branch on image URL format:
- `docker://...` uses `spec.source.registry`
- `http://...` or `https://...` uses `spec.source.http`

```yaml
spec:
  source:
    {{- if hasPrefix "docker://" .windowsImageUrl }}
    registry:
      url: {{ .windowsImageUrl }}
    {{- else }}
    http:
      url: {{ .windowsImageUrl }}
    {{- end }}
```

### EFI Secure Boot Requires SMM

The admission webhook rejects VMs with EFI SecureBoot when SMM is absent. Always pair them:

```yaml
domain:
  firmware:
    bootloader:
      efi:
        secureBoot: true
  features:
    smm:
      enabled: true
```

### `qemuGuestAgent.users` Is Required (Not Optional)

The admission webhook rejects VMs where `accessCredentials.sshPublicKey.propagationMethod.qemuGuestAgent` is an empty object. The `users` list must explicitly name the guest OS user:

```yaml
accessCredentials:
  - sshPublicKey:
      source:
        secret:
          secretName: {{ .sshSecretName }}
      propagationMethod:
        qemuGuestAgent:
          users:
            - {{ .vmUser | default "Administrator" }}
```

### Remove Deprecated Fields

- `hyperv.stimer` -- removed in newer KubeVirt APIs; produces unknown field warnings
- Do NOT use Sprig's `required` function -- kube-burner's Go template engine does not include it

### Root Disk Size

Windows images are much larger than Linux cloud images. Use a separate `windowsRootDiskSize` variable (default 90Gi) distinct from any data disk size variable.

## PowerShell Over SSH Patterns

### Single-Quote Escaping in Bash

To embed a literal single quote inside a bash single-quoted string passed to PowerShell:

```bash
# WRONG: '' inside single quotes produces empty string, not a quote
cmd='... Where-Object { $_.DriveType -eq ''Fixed'' }'

# CORRECT: use '"'"' to break out and re-enter single quotes
cmd='... Where-Object { $_.DriveType -eq '"'"'Fixed'"'"' }'
```

The existing `windows_guest_cpu_burn_count_cmd` uses `'"'"'*CNV_CPU_BURN=1*'"'"'` as the reference pattern.

### Dollar Signs in PowerShell via Bash

PowerShell variables start with `$`, which bash tries to expand. Use `[char]36` to construct dollar signs at PowerShell runtime:

```bash
# Bash constructs the command; [char]36 becomes $ only inside PowerShell
cmd='powershell.exe -NoProfile -Command "
  $d = [string][char]36
  $expr = \"${d}_.Status -eq ${d}([char]39)Up${d}([char]39)\"
  ..."'
```

This completely eliminates bash/PowerShell escaping chains.

### CRLF Stripping

PowerShell output contains `\r\n` (CRLF). When embedding string output into JSON, the `\r` becomes an invalid control character:

```bash
# Always strip \r from string output before JSON embedding
guest_os_name=$(remote_command ... | tr -d '\r' | head -1 | xargs)
```

Numeric outputs using `tr -cd '0-9'` naturally strip `\r` already.

### `-EncodedCommand` Size Limit (~8000 characters)

The Windows command line limit is 8,191 characters. A PowerShell script encoded as UTF-16LE + base64 roughly quadruples in size. Keep inline scripts under ~1KB raw (produces ~4000 chars encoded).

For larger scripts, build compact mode-specific inline snippets rather than sending a full multi-mode script. Keep full scripts as standalone `.ps1` files for manual testing/documentation.

### `Invoke-CimMethod` for Detached Process Launch

SSH sessions terminate child processes on disconnect. To launch persistent workers (CPU burn, memory burn), use WMI process creation which detaches from the SSH session:

```powershell
$cmd = "powershell.exe -NoProfile -Command `"<script>`""
Invoke-CimMethod -ClassName Win32_Process -MethodName Create -Arguments @{CommandLine=$cmd}
```

Workers launched this way survive SSH disconnection. Check for existing workers first (idempotent pattern).

## Disk Operations

### Blank DataVolumes Are Offline and Unformatted

Windows does not auto-initialize new disks. Blank DataVolumes arrive as offline, RAW disks. They must be explicitly initialized before any disk validation or application use:

```powershell
Get-Disk | Where-Object { $_.OperationalStatus -eq 'Offline' } | Set-Disk -IsOffline $false
Get-Disk | Where-Object { $_.IsReadOnly } | Set-Disk -IsReadOnly $false
$raw = @(Get-Disk | Where-Object { $_.PartitionStyle -eq 'RAW' })
foreach ($d in $raw) {
    $d | Initialize-Disk -PartitionStyle GPT -PassThru |
        New-Partition -AssignDriveLetter -UseMaximumSize |
        Format-Volume -FileSystem NTFS -Confirm:$false
}
```

This is idempotent -- already-initialized disks are skipped. Make this a togglable phase (`initializeDisks: true`) so it can be skipped when the golden image handles initialization.

### Disk Validation Phase Ordering

Disk utilization checks (`Get-Volume`) only work after initialization. The correct phase order:
1. Disk initialization (online + format)
2. Disk count validation (`Get-Disk` -- works on offline disks too)
3. Disk size validation (`Get-Disk .Size`)
4. Disk utilization (`Get-Volume` -- requires filesystem)

### FIO on Windows: `directory=` Option Is Broken (FIO 3.38)

FIO 3.38's `directory=` job file option fails on Windows with `lstat: No such file or directory` for all path formats. Workaround: `Set-Location` to the target drive root and omit `directory=`:

```powershell
foreach ($vol in $TargetVolumes) {
    Set-Location "$($vol.DriveLetter):\"
    & $fioExe $jobFile  # job file has no directory= line
}
```

## SSH Connectivity

### `virtctl` Requires `vmi/` Prefix

virtctl v1.6+ requires `vmi/<vm-name>` format for SSH targets. Bare VM names fail with `target must contain type and name separated by '/'`.

```bash
# CORRECT
virtctl ssh --username Administrator vmi/my-vm-name -n namespace -c "..."

# WRONG -- fails
virtctl ssh --username Administrator my-vm-name -n namespace -c "..."
```

### QEMU Guest Agent Version Requirement

SSH key injection via `accessCredentials` requires `guest-ssh-add-authorized-keys`, available in QEMU-GA >= v8.0 (Windows build from virtio-win). Older guest agents produce `AccessCredentialsSynchronized: False`. Upgrade via `virtio-win-guest-tools.exe`.

### `virtctl ssh` Fails in Electron-Based IDE Terminals

Running `virtctl ssh` from Cursor's integrated terminal produces `Connection closed by UNKNOWN port 65535`. This does NOT affect `beforeCleanup` runs (kube-burner spawns a clean shell). For interactive testing from the IDE, use `virtctl port-forward` + standard `ssh`.

## Windows Auto-Corrections by the Test Runner

When `guestOS=windows`, `run-workloads.sh` automatically applies safe defaults unless explicitly overridden via environment:

| Variable | Auto-set | Reason |
|----------|----------|--------|
| `vmUser` | `Administrator` | Linux defaults fail SSH to Windows |
| `maxWaitTimeout` | `30m` | CDI import + Windows boot is slow |
| `windowsRootDiskSize` | `90Gi` | Windows images are much larger |
| `max_ssh_retries` | `20` | Windows SSH starts later |
| `vmMemory` | `2Gi` (floor) | Windows Server minimum is 2Gi |
