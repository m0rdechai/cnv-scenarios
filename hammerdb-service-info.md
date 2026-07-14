# HammerDB Service on Windows VMs (hammerdb-mssql flow)

How HammerDB starts on VMs from the **hammerdb-mssql** database flow, and how to disable it **without changing cnv-scenarios code**.

## How it starts

In the **hammerdb-mssql** flow, HammerDB is **not** started by kube-burner or the test scripts. It is baked into the **Windows container disk image** as a **Scheduled Task** that runs at boot/logon. The repo only documents and validates that behavior.

From `docs/windows-image-build.md`:

> A **Scheduled Task** (e.g. `run_hammerdb`) that starts HammerDB at boot/logon …

The reference script lives at `C:\tools\hammerdb-4.12\run-hammerdb.ps1` and is meant to write results to `C:\tools\hammerdb-4.12\results\hammerdb-results.json`.

Validation Phase 10 in `check_windows_vm` polls until the `hammerdb` process (or a scheduled task whose name matches `*hammerdb*`) is no longer running. The configured process name is `waitProcessName: "hammerdb"` in `database/hammerdb-mssql/vars.yml`.

---

## Disable it automatically via vars (no manual SSH steps)

By default (`disableHammerdbSchedTaskAfterValidation: true` in `database/hammerdb-mssql/vars.yml` and `vars-sanity.yml`), after all validation phases complete for a VM, `check_windows_vm` runs Phase 13 that disables any Scheduled Task whose name matches `*<waitProcessName>*` (e.g. `run_hammerdb`) over `virtctl ssh`, so HammerDB will not auto-start on subsequent reboots of that VM.

- Default is `true` — the task is disabled after validation unless you opt out.
- Set `disableHammerdbSchedTaskAfterValidation: false` (or override via env) if you want HammerDB to auto-start again on reboot.
- Reuses `waitProcessName` as the task-name glob pattern; no separate task-name variable is needed.
- Phase 10 already waits for the process/task to finish before Phase 13 runs, so Phase 13 only disables the task — it does not stop anything currently running.
- Requires `ssh_ok` (i.e. `validateSSH` succeeded) and a non-empty `waitProcessName`; otherwise Phase 13 reports `SKIP`.
- See the `validation-windows-vm.json` report's `disable_sched_task` phase entry for the outcome (`PASS`/`FAIL`/`SKIP`).

If you still need to do this by hand (e.g. on a VM from a run that set the toggle to `false`), use the manual steps below.

---

## Disable on a running VM (no code changes)

SSH into the VM (typically `virtctl ssh` as `Administrator`), then:

### 1. Find the scheduled task

```powershell
Get-ScheduledTask | Where-Object { $_.TaskName -like '*hammerdb*' } |
    Format-Table TaskName, State, TaskPath -AutoSize
```

Common names: `run_hammerdb`, `RunHammerDB`, etc.

### 2. Stop anything running now

```powershell
Stop-Process -Name hammerdb -Force -ErrorAction SilentlyContinue

Get-ScheduledTask | Where-Object { $_.TaskName -like '*hammerdb*' } |
    ForEach-Object { Stop-ScheduledTask -TaskName $_.TaskName -TaskPath $_.TaskPath -ErrorAction SilentlyContinue }
```

### 3. Disable it so it does not start on reboot

```powershell
Disable-ScheduledTask -TaskName 'run_hammerdb'   # use the name from step 1
```

Or disable all matching tasks:

```powershell
Get-ScheduledTask | Where-Object { $_.TaskName -like '*hammerdb*' } |
    Disable-ScheduledTask
```

### 4. Confirm

```powershell
Get-Process -Name hammerdb -ErrorAction SilentlyContinue
Get-ScheduledTask | Where-Object { $_.TaskName -like '*hammerdb*' } |
    Select-Object TaskName, State
```

---

## If you are building/customizing the image

Disable or omit the scheduled task during image build so new VMs never auto-start HammerDB. The repo placeholder at `database/hammerdb-mssql/scripts/run-hammerdb.ps1` is only a reference for what goes on the image — kube-burner does not deploy it.

---

## What this does **not** stop

| Component | Still runs? | Notes |
|-----------|-------------|-------|
| **SQL Server (`MSSQLSERVER`)** | Yes | Separate service; Phase 3 checks it is running |
| **FIO on E:/F:/… (Phase 11)** | Yes, if you run the full test | Triggered by `check_windows_vm` over SSH, not the HammerDB task |
| **Disk init (Phase 7)** | Yes | Also driven by the test harness |

To stop MSSQL writing as well:

```powershell
Stop-Service MSSQLSERVER -Force
Set-Service MSSQLSERVER -StartupType Disabled   # optional, persists across reboots
```

---

## If you are mid-test and only want to stop the benchmark

Disabling/killing HammerDB is enough. Phase 10 in `check_windows_vm` waits until the `hammerdb` process (or a `*hammerdb*` scheduled task) is **not** running, then continues. With `expectedDiskUtilAfterProcessGB: 0` in `vars.yml`, that phase is report-only and will not fail on disk size.

Phase 11 (FIO) still runs if `fillExtraDisks=true` unless you skip or abort the test run itself.

---

## Summary

HammerDB auto-start is a **Windows Scheduled Task in the golden image**. Disable that task (and stop any running `hammerdb` process) on the VM; no cnv-scenarios code change is required. If you want new VMs to never run it, remove or disable the task when building the Windows image.
