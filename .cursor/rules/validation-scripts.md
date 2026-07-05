---
description: Patterns and pitfalls for writing validation functions in check.sh
globs:
  - "config/scripts/check.sh"
  - "config/scripts/wrapper.sh"
  - "scale-testing/**/check.sh"
---

# Validation Script Development Guide

## Architecture Overview

Validation uses a two-tier system:

- **Global scripts** (`config/scripts/check.sh`) -- shared library with all standard validation functions. Used by most scenarios.
- **Local scripts** (`scale-testing/*/config/scripts/check.sh`) -- used by per-host-density and virt-capacity-benchmark for percentage-based sampling.

Validation is triggered by kube-burner's `beforeCleanup` hook calling `wrapper.sh`, which resolves `check.sh` path, creates the results directory, pipes output to tee, and preserves the exit code.

## Function Signature Patterns

### Positional Args (Legacy)

Older functions use positional arguments. Adding parameters in the middle breaks all callers. Avoid this pattern for new functions.

### Key=Value Args (Preferred for New Functions)

The extensible pattern uses a fixed positional head followed by `key=value` pairs:

```bash
check_my_scenario() {
    local label_key="$1"; local label_value="$2"
    local namespace="$3"; local private_key="$4"
    local vm_user="$5"; local results_dir="$6"
    shift 6

    local -A cfg
    for arg in "$@"; do
        cfg["${arg%%=*}"]="${arg#*=}"
    done

    # Access config with defaults:
    local cpu_cores="${cfg[cpuCores]:-4}"
    local validate_ssh="${cfg[validateSSH]:-true}"
}
```

New phases can be added without changing existing callers -- they simply do not pass the new key, and the function defaults gracefully.

## `wrapper.sh` Results Directory Handling

`wrapper.sh` reads the **last argument** as the results directory. When using `key=value` pairs after positional args, the last `key=value` string gets misinterpreted as the results path.

**Workaround:** Pass the results directory as **both** the 6th positional arg (for the function) and as the very last arg in the `beforeCleanup` line (for `wrapper.sh`):

```yaml
beforeCleanup: "../../config/scripts/wrapper.sh check_my_scenario \
  {{ $labelKey }} {{ $labelValue }} {{ .testNamespace }} \
  {{ .privateKey }} {{ .vmUser }} {{ .resultsPath }} \
  cpuCores={{ .cpuCores }} validateSSH={{ .validateSSH }} \
  {{ .resultsPath }}"
```

The function creates `mkdir -p "${results_dir}"` using `$6`, and `wrapper.sh` also creates a directory from the last arg. The duplicate is harmless.

## Pitfalls

### 1. `remote_command` Swallows All Output on Non-Zero Exit

The `remote_command()` function suppresses stderr (`2>/dev/null`) and returns 1 without echoing output when the exit code is non-zero:

```bash
output=$(virtctl ssh ... 2>/dev/null)
local ret=$?
if [ $ret -ne 0 ]; then
    return 1    # output is lost
fi
echo "${output}"
```

When a PowerShell command fails, callers using `result=$(remote_command ...) || true` get an empty string. Any parsing of `$result` silently produces wrong values.

**Mitigation:** For phases where diagnostic output on failure matters, check for specific markers in the output before trusting parsed values. Do not assume empty output means "zero."

### 2. `|| echo "{}"` Fallback Masks Failures as Valid JSON

The pattern `output=$(remote_command ... || echo "{}")` combined with `grep -oP '"field"\s*:\s*\K...'` returns empty on both "command succeeded with empty JSON" and "command failed entirely." Downstream `${var:-0}` defaults then silently substitute zero.

**Fix:** Always check for the presence of the expected JSON key before trusting the value:

```bash
if ! echo "${util_json}" | grep -q '"usedGB"'; then
    overall_status="FAILED"
    validations+=("{\"phase\": \"disk_util\", \"status\": \"FAIL\", \
      \"message\": \"Command returned no data (SSH or PowerShell failure)\"}")
fi
```

### 3. PowerShell CRLF in JSON Fields

PowerShell string output contains `\r` (carriage return). Embedding it directly in a JSON string produces invalid JSON:

```
jq: parse error: Invalid string: control characters from U+0000 through U+001F
```

**Fix:** Always strip `\r` from string outputs before JSON embedding:

```bash
value=$(remote_command ... | tr -d '\r' | head -1 | xargs)
```

### 4. `beforeCleanup` Multi-Word Parameters Are Word-Split

kube-burner executes the `beforeCleanup` string via the shell. A value like `expectedOS=Windows Server 2022` becomes three tokens: `expectedOS=Windows`, `Server`, `2022`. The `key=value` parser only catches the first.

**Fix:** Encode spaces as underscores in Go templates, decode in the function:

```yaml
# Template:
expectedOS={{ .expectedOS | default "Windows" | replace " " "_" }}
```

```bash
# Function:
local expected_os="${cfg[expectedOS]:-Windows}"
expected_os="${expected_os//_/ }"
```

### 5. Structured JSON Report Format

All validation functions must produce a JSON report file (`validation-*.json`) in the results directory. Follow this structure:

```json
{
  "testName": "my-scenario",
  "timestamp": "2026-01-01T00:00:00Z",
  "overallStatus": "PASSED",
  "validations": [
    {
      "phase": "vm_discovery",
      "status": "PASS",
      "message": "Found 2 VMs with expected label",
      "details": { "vmCount": 2 }
    }
  ]
}
```

Use `PASSED`/`FAILED` for `overallStatus`, `PASS`/`FAIL`/`SKIP` for individual phase status.

### 6. Phase Gating Pattern

Later phases should be gated on earlier phases succeeding. Do not attempt SSH validation if VM discovery failed:

```bash
if [[ "${discovery_status}" == "FAIL" ]]; then
    # Skip SSH phases, mark as SKIP
    validations+=("{\"phase\": \"guest_os\", \"status\": \"SKIP\", \
      \"message\": \"Skipped: VM discovery failed\"}")
else
    # Proceed with SSH validation
    ...
fi
```

### 7. Retry Logic for SSH Validation

SSH to VMs (especially Windows) is unreliable on first attempt. Use the existing `retry_validation()` wrapper or implement retry with backoff:

```bash
local max_retries="${max_ssh_retries:-10}"
local attempt=0
while [ $attempt -lt $max_retries ]; do
    output=$(remote_command "$vm" "$namespace" "$user" "$key" "$cmd")
    if [ $? -eq 0 ] && [ -n "$output" ]; then
        break
    fi
    ((attempt++))
    sleep 5
done
```

### 8. Tolerance Patterns for Numeric Validation

Guest OS memory and disk sizes will not match spec values exactly. Use percentage tolerances:

```bash
# 15% tolerance for memory
local tolerance=15
local min_expected=$((expected_mb * (100 - tolerance) / 100))
local max_expected=$((expected_mb * (100 + tolerance) / 100))
if [ "$guest_mb" -ge "$min_expected" ] && [ "$guest_mb" -le "$max_expected" ]; then
    status="PASS"
fi
```

Standard tolerances:
- Memory: +/-15%
- Disk size: +/-5% (minimum 1GB absolute)
- Disk utilization: +/-10% (minimum 5GB absolute)
- CPU count: exact match

### 9. Windows vs Linux Command Equivalents

| Check | Linux | Windows |
|-------|-------|---------|
| CPU count | `nproc` | `(Get-CimInstance Win32_Processor).NumberOfLogicalProcessors` |
| Total memory | `free -m \| awk '/Mem:/{print $2}'` | `[math]::Round((Get-CimInstance Win32_ComputerSystem).TotalPhysicalMemory/1MB)` |
| Disk count | `lsblk --json` (exclude vda/sda/zram) | `@(Get-Disk \| Where-Object { -not $_.IsSystem }).Count` |
| Disk size | `lsblk -b` or `/sys/block/*/size` | `(Get-Disk N).Size / 1GB` |
| NIC count | `ip -br link show` | `@(Get-NetAdapter \| Where-Object Status -eq 'Up').Count` |
| SSH test | `hostname && echo SSH_OK` | `$env:COMPUTERNAME; echo SSH_OK` |
