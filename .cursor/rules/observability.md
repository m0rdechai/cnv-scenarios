---
description: Patterns for Elasticsearch indexing, metadata collection, and Grafana dashboards
globs:
  - "config/scripts/metadata-collector.sh"
  - "config/scripts/validation-indexer.sh"
  - "config/scripts/alert-collector.sh"
  - "config/scripts/log-indexer*"
  - "config/grafana/**"
  - "config/metrics-profiles/**"
---

# Observability Pipeline Guide

## Data Flow

```
kube-burner run
  ├── metrics-profile → ES (cnv-* indices, per-metric documents)
  ├── metadata-collector.sh → ES (cnv-metadata index)
  ├── validation-indexer.sh → ES (cnv-validation index)
  ├── alert-collector.sh → ES (cnv-alerts index)
  └── log-indexer → ES (cnv-logs index)
```

All indexing is conditional on `esServer` being non-empty in the generated vars file.

## ES Indexing Requirements

### `esServer` Must Be in the Generated Vars File

Setting `esServer` only in the shell environment is insufficient. The runner must write it into the `temp_vars` file (same pattern as `PROM`/`PROM_TOKEN`). Without it in the file:
- kube-burner's `{{- if .esServer }}` block never activates
- Post-run scripts that read `esServer` from vars skip ES operations

Committed vars files use `esServer: ""` for portability. The runner injects the value at runtime.

### Kubernetes Quantity Normalization

Kubernetes resource quantities (e.g., `100m` for CPU, `256Mi` for memory) break ES numeric field mappings. If an ES index already has a field mapped as `long` from a prior numeric document, a string like `"100m"` causes `mapper_parsing_exception`.

**Fix:** Normalize quantities to plain numbers before indexing:
- CPU: strip `m` suffix (milliCPU notation)
- Memory: convert to consistent units (MB or GB)
- Storage: convert to consistent units (GB)

### Sensitive Data Filtering

The metadata collector captures all vars from the YAML file. Filter sensitive fields before indexing:
- Field names containing: `token`, `password`, `secret`, `key` (case-insensitive)
- Specific fields: `PROM_TOKEN`, `privateKey`

Never index credentials, bearer tokens, or SSH key paths to Elasticsearch.

### `testName` Source of Truth

The `testName` in validation JSON reports may not match the canonical test name from `run-workloads.sh`. The `validation-indexer.sh` accepts a `--test-name` parameter to override the JSON value with the authoritative name from the runner.

## Metric Names

### `kubevirt-metrics.yaml` Is the Single Source of Truth

The `metricName` values in `config/metrics-profiles/kubevirt-metrics.yaml` are used verbatim by kube-burner when indexing to Elasticsearch. Dashboard queries must use these exact strings.

Common naming mismatches that cause "No data" in dashboards:

| Wrong (guessed/truncated) | Correct (from metrics profile) |
|---------------------------|-------------------------------|
| `99thEtcdDiskWalFsync` | `99thEtcdDiskWalFsyncDurationSeconds` |
| `99thEtcdDiskBackendCommit` | `99thEtcdDiskBackendCommitDurationSeconds` |
| `etcdDBSize` | `etcdDBPhysicalSizeBytes` or `etcdDBLogicalSizeBytes` |
| `nodeNetworkBytesReceived` | `rxNetworkBytes` |
| `nodeNetworkBytesTransmitted` | `txNetworkBytes` |
| `containerCPU` | `containerCPU-Prometheus` |

**Before adding any metric to a dashboard:** verify it exists by querying ES with a `metricName.keyword` terms aggregation filtered by a known UUID. Cross-reference against `kubevirt-metrics.yaml`.

### VMI Latency Data Structure

The `vmiLatencyQuantilesMeasurement` documents have an unintuitive structure:
- `quantileName` contains **lifecycle phase names** (e.g., `VMIRunning`, `VMIScheduled`)
- `P50`, `P95`, `P99`, `avg`, `min`, `max` are **numeric columns** (values in milliseconds)
- `jobName` distinguishes measurement source (`create-vms`, `startup-vms`, `shutdown-vms`)

**Common mistake:** Treating `quantileName` as a percentile selector (e.g., filtering `quantileName.keyword: P99`). This matches zero documents.

**Correct query pattern:**
```json
{
  "query": "uuid.keyword: $uuid AND metricName.keyword: vmiLatencyQuantilesMeasurement AND NOT jobName.keyword: shutdown-vms",
  "metrics": [{"field": "P99", "type": "avg"}],
  "bucketAggs": [{"field": "quantileName.keyword", "type": "terms"}]
}
```

Filter out `shutdown-vms` jobs -- they store int64 min sentinel values in latency fields for inapplicable phases, poisoning aggregations.

### VMI Latency Units Are Milliseconds

kube-burner stores VMI lifecycle latency values in milliseconds (e.g., `vmiCreatedLatency: 54207`). Dashboard panels must use `unit: "ms"` not `unit: "s"`.

## Grafana Dashboard Patterns

### Dashboard Import API (Not DB API)

The `/api/dashboards/db` endpoint does NOT process `__inputs` template variables. Dashboards with `${DS_CNV_ES}` references render broken. Use `/api/dashboards/import` with explicit inputs mapping:

```json
{
  "dashboard": { "...": "..." },
  "overwrite": true,
  "inputs": [
    {"name": "DS_CNV_ES", "type": "datasource", "pluginId": "elasticsearch", "value": "<uid>"}
  ]
}
```

### Separate Indices Need Separate Datasources

Each ES index (`cnv-metadata`, `cnv-alerts`, `cnv-*`) needs its own Grafana datasource. A datasource pinned to one index cannot query another.

### Collapsed Rows Must Nest Child Panels

For rows with `"collapsed": true`, child panels must be inside the row's `"panels"` array, not as sibling elements at the top level. Otherwise Grafana shows "(0 panels)".

### `filterFieldsByName` with Regex Silently Fails (Grafana 10.1)

Using `filterFieldsByName` with `include.pattern` (regex) combined with `renameByRegex` produces "No data" silently. The same filter works with explicit `include.names` arrays.

**Always use explicit field name lists** instead of regex patterns for field filtering in Grafana 10.1+.

### Transform Pipeline Ordering

For displaying many key-value pairs with readable names:

```
filterFieldsByName → organize(renameByName) → reduce(seriesToRows) → organize(headers)
                     ↑ renames columns         ↑ columns → rows       ↑ "Field" → "Property"
                     (affects cell values)      (names → Field col)    (affects headers only)
```

The rename must happen **before** the reduce transform for property names to appear correctly in the final table.

### ES `extended_stats` Returns Opaque Field

`{"type": "extended_stats"}` returns a single field named `"Extended Stats"` containing only the average. It does NOT break out `std_deviation`, `min`, `max`, `count`. Use separate `avg`, `min`, `max`, `count` metric types instead.

### Sparse Data and `date_histogram`

For time-series with sparse data (1 doc per test run):
- Use fixed interval (`1d` or `1w`) instead of `"auto"`
- Set `min_doc_count: "1"` to only return buckets with data
- `min_doc_count: "0"` with `interval: "auto"` on sparse data may return 0 frames

### Business Charts Plugin (volkovlabs-echarts-panel)

Required panel options for JavaScript execution:
- `"editorMode": "code"` -- without this, JS is not executed (blank panel, no error)
- Boilerplate objects: `baidu`, `gaode`, `google`, `map: "none"`, `themeEditor`, `visualEditor`

Data access in `getOption` function:
- `context.panel.data.series[N].fields[M].name` -- field name
- `context.panel.data.series[N].fields[M].values` -- value array
- `context.grafana.replaceVariables('$varName')` -- resolve variables
- `context.grafana.theme.isDark` -- dark mode detection

### Prometheus Token Refresh

Prometheus tokens expire during long sequential runs. Create a fresh token (`--duration=1h`) before each test, not once upfront. The runner handles this via `refresh_prometheus_token()`.

### Ceph Metrics Depend on ODF

The Ceph metrics (`cephOSD*`, `cephCluster*`) only produce data on clusters with OpenShift Data Foundation installed. Empty panels on non-ODF clusters are expected, not a bug.

## Debugging Queries

### Grafana DS Proxy API

Bypass the UI to see exact data frame schema:

```bash
curl -s -u "$GRAFANA_USER:$GRAFANA_PASSWORD" "http://<grafana>/api/ds/query" \
  -H "Content-Type: application/json" \
  -d '{"queries":[{"refId":"A","datasource":{"type":"elasticsearch","uid":"<uid>"},
       "query":"<lucene>","metrics":[{"id":"1","type":"raw_data","settings":{"size":"1"}}],
       "bucketAggs":[],"timeField":"timestamp"}],"from":"now-1y","to":"now"}'
```

This reveals exact field names and types, confirming whether the issue is data delivery or transform pipeline.
