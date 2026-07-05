---
description: Guide for adding a new test scenario to the CNV Scenarios suite
globs:
  - "**/run-workloads.sh"
  - "resource-limits/**"
  - "hot-plug/**"
  - "performance/**"
  - "scale-testing/**"
  - "database/**"
---

# Adding a New Test Scenario

## Checklist

1. **Create the scenario directory** under the appropriate category:
   - `resource-limits/` -- CPU, memory, disk constraints
   - `hot-plug/` -- dynamic attach/detach operations
   - `performance/` -- high-resource or stress scenarios
   - `scale-testing/` -- density and capacity scenarios
   - `database/` -- application-level workloads

2. **Create required files:**
   ```
   <category>/<scenario-name>/
   ├── <scenario>-test.yml          # kube-burner config (Go templates)
   ├── vars.yml                     # Full mode variables
   ├── vars-sanity.yml              # Sanity mode (minimal resources)
   ├── vm-<scenario>-template.yml   # Linux VM spec template
   ├── vm-<scenario>-windows-template.yml  # Windows VM template (if OS=both)
   └── templates/
       └── secret_ssh_public.yml    # SSH key secret
   ```

3. **Register the test in `run-workloads.sh`:**
   - Add to `TEST_REGISTRY`: `["my-test"]="category/my-test:my-test-test.yml:yml"`
   - Add to `TEST_ORDER` array (controls `--all` execution order)
   - Add to `TEST_OS_SUPPORT`: `["my-test"]="linux"` or `"both"` or `"windows"`

4. **Add validation function** to `config/scripts/check.sh` (or create a local `check.sh` for scale scenarios needing percentage-based sampling).

5. **Wire validation** via `beforeCleanup` in the kube-burner config:
   ```yaml
   beforeCleanup: "../../config/scripts/wrapper.sh check_my_scenario {{ $jobCounterLabelKey }} ..."
   ```

6. **If pre-execution setup is needed**, add a `setup_*` function and register it in `run_setup()`.

7. **If post-execution cleanup is needed**, add logic in `run_cleanup()`.

## File Extension Convention

Use `.yml` (not `.yaml`) for all vars and template files. The `TEST_REGISTRY` uses the extension field to locate files. A mismatch between the actual file extension and the registry entry causes silent test skipping.

## Vars File Requirements

Both `vars.yml` and `vars-sanity.yml` must contain:
- `testName` -- flows into ES index naming and Grafana queries
- `testNamespace` -- use `TIMESTAMP` placeholder for runtime uniqueness in sanity
- `esServer: ""` -- empty string for portability; set via environment at runtime
- All variables referenced by Go templates in the config and VM template files

Variable names are **camelCase and case-sensitive** (e.g., `cpuCores`, `vmsPerNamespace`).

## Pitfalls

### 1. `measurements` must be under `global`, not per-job

kube-burner rejects `measurements:` when placed under a job definition. Always place it in the `global:` section:

```yaml
global:
  measurements:
    - name: vmiLatencyMeasurement
      ...
```

### 2. `customStatusPaths` jq runs on the `.status` sub-object

kube-burner extracts the `.status` field first, then runs the jq query against it. A key of `.status.phase` becomes `.status.status.phase` internally (double-nested). Use status-relative paths:

```yaml
# CORRECT -- relative to .status
customStatusPaths:
  - key: '.phase'
    value: 'Succeeded'

# WRONG -- double-nested, will silently poll until timeout
customStatusPaths:
  - key: '.status.phase'
    value: 'Succeeded'
```

The symptom is kube-burner polling every second with no log output for the entire `maxWaitTimeout` duration (potentially hours). Use `--log-level=debug` to diagnose.

### 3. Go template engine has no Sprig functions

kube-burner uses standard Go `text/template`, not Helm/Sprig. Functions like `required`, `toYaml`, `include` are not available. Use plain `{{ .varName }}` with `| default "value"` for defaults.

### 4. `defaultMissingKeysWithZero: true` affects string variables

When enabled in the kube-burner config, any undefined variable renders as `0` (not empty string). Be careful with variables that must remain strings (URLs, names). Always define all variables in both vars files.

### 5. Dynamic resource counts use the `until` loop pattern

For N data disks, N NICs, or similar repeating resources, use Go `until` in three synchronized places:

```yaml
# 1. devices.disks
{{- range $i := until (.dataDisks | int) }}
- disk:
    bus: virtio
  name: datadisk-{{ add $i 1 }}
{{- end }}

# 2. spec.volumes
{{- range $i := until (.dataDisks | int) }}
- dataVolume:
    name: datadisk-{{ add $i 1 }}-{{$.Replica}}
  name: datadisk-{{ add $i 1 }}
{{- end }}

# 3. dataVolumeTemplates
{{- range $i := until (.dataDisks | int) }}
- metadata:
    name: datadisk-{{ add $i 1 }}-{{$.Replica}}
  spec:
    source:
      blank: {}
    storage:
      resources:
        requests:
          storage: {{$.diskSize}}
{{- end }}
```

Use `$.varName` (not `.varName`) inside `range` blocks to access outer template data.

### 6. Vars drive both VM construction AND validation

The same variable (e.g., `cpuCores: 8`) should drive the VM spec template AND the `beforeCleanup` validation call. This prevents drift between what is built and what is checked:

```yaml
beforeCleanup: "... cpuCores={{ .cpuCores | default 8 }} ..."
```

### 7. `beforeCleanup` parameters with spaces break shell parsing

kube-burner executes `beforeCleanup` via the shell. Multi-word values get word-split into separate tokens. Encode spaces as underscores in Go templates and decode in the validation function:

```yaml
# In kube-burner config:
expectedOS={{ .expectedOS | default "Windows" | replace " " "_" }}

# In check.sh:
expected_os="${cfg[expectedOS]:-Windows}"
expected_os="${expected_os//_/ }"
```

### 8. The `esServer` environment variable must be written into `temp_vars`

Setting `esServer` only in the shell environment is insufficient. The runner must `sed` it into the generated vars file because kube-burner and downstream scripts read from the file, not the environment. The runner already handles this for `PROM` and `PROM_TOKEN`.

### 9. Sanity vars should use minimal resources

Sanity mode exists for quick code-path validation, not stress testing. Keep VM counts at 1-2, memory low, disk sizes small. The goal is exercising all validation phases, not pushing cluster limits.
