---
description: Pitfalls and patterns for kube-burner configuration files
globs:
  - "**/*-test.yml"
  - "**/*-test.yaml"
---

# kube-burner Configuration Guide

## Go Template Engine

kube-burner uses Go `text/template` enhanced with the [Sprig](https://masterminds.github.io/sprig/) library. Key differences from Helm:

- **Sprig functions are available** -- `default`, `replace`, `upper`, `lower`, `trim`, `printf`, `add`, `sub`, `mul`, `div`, `until`, `int`, `hasPrefix`, `hasSuffix`, `eq`, `ne`, `lt`, `gt`, and the rest of the Sprig set
- **Helm-only functions are NOT available** -- `include`, `tpl`, `lookup`, and `required` are Helm features, not Sprig
- **Custom kube-burner helpers:** `Binomial`, `IndexToCombination`, `GetSubnet24`, `GetIPAddress`, `ReadFile`
- Variables come from the vars file passed via `--user-data`
- The `counter` variable is special: setting `counter=0` triggers a cleanup-only job

### Variable Access in Range Blocks

Inside `{{ range }}` blocks, the dot (`.`) context changes to the loop variable. Use `$` to access the outer template data:

```yaml
{{- range $i := until (.dataDisks | int) }}
  storage: {{$.diskSize}}      # $ reaches outer context
{{- end }}
```

### Conditional Blocks for OS Branching

```yaml
{{- if eq (.guestOS | default "linux") "windows" }}
  # Windows-specific template path
{{- else }}
  # Linux template path
{{- end }}
```

## `customStatusPaths` Configuration

### The `.status` Sub-Object Rule

kube-burner's `verifyCondition()` function extracts the `.status` sub-object from the resource, then runs the jq query against that extracted object. All jq keys must be **relative to `.status`**, not the full object.

```yaml
# CORRECT -- jq runs against the .status sub-object
waitFor:
  - kind: DataVolume
    customStatusPaths:
      - key: '.phase'
        value: 'Succeeded'

# WRONG -- looks for .status.status.phase (does not exist)
waitFor:
  - kind: DataVolume
    customStatusPaths:
      - key: '.status.phase'
        value: 'Succeeded'
```

**Symptom of incorrect path:** kube-burner silently polls every second for the full `maxWaitTimeout` (potentially hours) with no log output between "Waiting up to..." and eventual timeout errors. A 13-minute silent gap is normal -- it is the API watch connection timing out and reconnecting, not a hang.

**Debugging:** Run with `--log-level=debug` to see per-second polling output confirming the waiter is active but the condition never matches.

### Condition Paths for Common Resources

```yaml
# DataVolume phase
- key: '.phase'
  value: 'Succeeded'

# Pod condition
- key: '(.conditions[] | select(.type == "Ready")).status'
  value: "True"

# VMI condition
- key: '(.conditions[] | select(.type == "Ready")).status'
  value: "True"
```

## `measurements` Placement

Measurements must be in the `global:` section, NOT under individual jobs:

```yaml
# CORRECT
global:
  measurements:
    - name: vmiLatencyMeasurement
      metricName: vmiLatencyQuantilesMeasurement
      ...

# WRONG -- kube-burner rejects with "field measurements not found in type config.rawJob"
jobs:
  - name: create-vms
    measurements:  # ERROR
      - name: vmiLatencyMeasurement
```

## `beforeCleanup` Hook

### Command Construction

The `beforeCleanup` field is executed by the shell. Use Go template expansions to pass variables:

```yaml
beforeCleanup: "../../config/scripts/wrapper.sh check_my_scenario \
  {{ $jobCounterLabelKey }} {{ $jobCounterLabelValue }} \
  {{ .testNamespace }} {{ .privateKey }} {{ .vmUser }} \
  {{ .resultsPath }} cpuCores={{ .cpuCores | default 4 }}"
```

### Space-Encoding for Multi-Word Values

Shell word-splitting breaks multi-word parameter values. Encode spaces as underscores:

```yaml
expectedOS={{ .expectedOS | default "Windows" | replace " " "_" }}
```

The validation function decodes: `expected_os="${expected_os//_/ }"`

### Vars-as-Single-Source-of-Truth

The same variable should drive both VM construction and validation. This prevents drift:

```yaml
# VM template uses cpuCores for the VM spec
# beforeCleanup passes the same cpuCores to the checker
beforeCleanup: "... cpuCores={{ .cpuCores | default 4 }} \
  dataDisks={{ .dataDisks | default 1 }} \
  diskSize={{ .diskSize | default \"50Gi\" }}"
```

Separate "expected" keys (e.g., `expectedDiskUtilGB`) are only needed for values that are not VM spec properties.

## `defaultMissingKeysWithZero`

When `defaultMissingKeysWithZero: true` is set in the config:
- Any undefined variable renders as `0` (integer zero)
- This breaks string variables (URLs, names, paths) that are expected to be empty when unset
- Always define ALL variables in both `vars.yml` and `vars-sanity.yml`
- Be especially careful with conditional blocks: `{{ if .myVar }}` will be true for `0`

## Job Configuration Patterns

### Cleanup Job Pattern

Use `counter=0` to trigger cleanup-only behavior:

```yaml
jobs:
  - name: cleanup
    jobType: delete
    waitForDeletion: true
    objects:
      - kind: Namespace
        labelSelector:
          matchLabels:
            purpose: test
```

### Namespace Isolation

Use the `TIMESTAMP` placeholder in `testNamespace` for unique per-run namespaces:

```yaml
# In vars-sanity.yml
testNamespace: cnv-sanity-my-test-TIMESTAMP
```

The runner replaces `TIMESTAMP` with a unique suffix at runtime.

## Common Configuration Mistakes

1. **Forgetting `metricsEndpoints`** -- Without explicit metrics endpoints, kube-burner does not collect Prometheus metrics
2. **Wrong relative paths in `beforeCleanup`** -- Paths are relative to the kube-burner config file location, not the repo root
3. **Missing `inputVars` for templates** -- VM templates reference variables that must be explicitly passed through `inputVars` in the job config
4. **`maxWaitTimeout` too short for Windows** -- CDI import + Windows boot needs 30m minimum; Linux typically needs 10m
