#!/bin/bash
#
# metadata-collector.sh - Collect OCP cluster and environment metadata
#
# Gathers cluster version, node hardware, operator versions, storage classes,
# test configuration, validation results, and run summary. Outputs metadata.json
# and optionally indexes to Elasticsearch for correlation with kube-burner
# metrics via UUID.
#
# Usage:
#   metadata-collector.sh \
#     --uuid <kube-burner-uuid> \
#     --test-name <test-name> \
#     --mode <sanity|full> \
#     --run-timestamp <run-YYYYMMDD-HHMMSS> \
#     --vars-file <path-to-temp-vars> \
#     --results-dir <path-to-results> \
#     [--exit-code <0|1>] \
#     [--duration <seconds>] \
#     [--validation-dir <path>] \
#     [--es-server <url>] \
#     [--metadata-index <name>] \
#     [--test-index <name>]
#

set -eo pipefail

# =============================================================================
# ARGUMENT PARSING
# =============================================================================

UUID=""
TEST_NAME=""
MODE=""
RUN_TIMESTAMP=""
VARS_FILE=""
RESULTS_DIR=""
EXIT_CODE=""
DURATION=""
VALIDATION_DIR=""
ES_SERVER=""
METADATA_INDEX="cnv-metadata"
TEST_INDEX=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --uuid)           UUID="$2";           shift 2 ;;
        --test-name)      TEST_NAME="$2";      shift 2 ;;
        --mode)           MODE="$2";           shift 2 ;;
        --run-timestamp)  RUN_TIMESTAMP="$2";  shift 2 ;;
        --vars-file)      VARS_FILE="$2";      shift 2 ;;
        --results-dir)    RESULTS_DIR="$2";    shift 2 ;;
        --exit-code)      EXIT_CODE="$2";      shift 2 ;;
        --duration)       DURATION="$2";       shift 2 ;;
        --validation-dir) VALIDATION_DIR="$2"; shift 2 ;;
        --es-server)      ES_SERVER="$2";      shift 2 ;;
        --metadata-index) METADATA_INDEX="$2"; shift 2 ;;
        --test-index)     TEST_INDEX="$2";     shift 2 ;;
        *)
            echo "metadata-collector: Unknown argument: $1" >&2
            exit 1
            ;;
    esac
done

if [[ -z "$UUID" || -z "$TEST_NAME" || -z "$RESULTS_DIR" ]]; then
    echo "metadata-collector: --uuid, --test-name, and --results-dir are required" >&2
    exit 1
fi

mkdir -p "$RESULTS_DIR"

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

get_yaml_value() {
    local key="$1"
    local file="$2"
    local default="${3:-}"

    if [[ -f "$file" ]]; then
        local value
        value=$(grep "^${key}:" "$file" 2>/dev/null | head -1 | awk '{print $2}' | tr -d '"' | tr -d "'")
        if [[ -n "$value" ]]; then
            echo "$value"
            return
        fi
    fi
    echo "$default"
}

oc_safe() {
    oc "$@" 2>/dev/null || echo ""
}

# =============================================================================
# CLUSTER METADATA COLLECTION
# =============================================================================

echo "metadata-collector: Collecting cluster metadata for UUID=${UUID}..."

ocp_version=$(oc_safe get clusterversion version -o jsonpath='{.status.desired.version}')
cluster_id=$(oc_safe get clusterversion version -o jsonpath='{.spec.clusterID}')
platform=$(oc_safe get infrastructure cluster -o jsonpath='{.status.platform}')
api_url=$(oc_safe get infrastructure cluster -o jsonpath='{.status.apiServerURL}')
network_type=$(oc_safe get network.config cluster -o jsonpath='{.spec.networkType}')

# =============================================================================
# NODE INFORMATION
# =============================================================================

nodes_json=$(oc_safe get nodes -o json)

if [[ -n "$nodes_json" ]]; then
    node_total=$(echo "$nodes_json" | jq '.items | length')
    node_masters=$(echo "$nodes_json" | jq '[.items[] | select(.metadata.labels["node-role.kubernetes.io/master"] != null or .metadata.labels["node-role.kubernetes.io/control-plane"] != null)] | length')
    node_workers=$(echo "$nodes_json" | jq '[.items[] | select(.metadata.labels["node-role.kubernetes.io/worker"] != null)] | length')

    worker_details=$(echo "$nodes_json" | jq '[
        .items[]
        | select(.metadata.labels["node-role.kubernetes.io/worker"] != null)
        | {
            name: .metadata.name,
            cpuModel: (
                [.metadata.labels | to_entries[] | select(.key | startswith("host-model-cpu.node.kubevirt.io/")) | .key | ltrimstr("host-model-cpu.node.kubevirt.io/")] | first //
                .metadata.labels["feature.node.kubernetes.io/cpu-model.family"] //
                .metadata.labels["node.kubernetes.io/instance-type"] //
                "unknown"
            ),
            cpuCores: (.status.capacity.cpu // "0" | tonumber),
            memoryGiB: (((.status.capacity.memory // "0Ki" | gsub("Ki$"; "") | tonumber) / 1048576) | floor),
            architecture: (.status.nodeInfo.architecture // "unknown")
        }
    ]')
else
    node_total=0
    node_masters=0
    node_workers=0
    worker_details="[]"
fi

# =============================================================================
# OPERATOR VERSIONS
# =============================================================================

get_csv_version() {
    local namespace="$1"
    local pattern="$2"
    oc_safe get csv -n "$namespace" -o json | \
        jq -r --arg pat "$pattern" '
            [.items[] | select(.metadata.name | test($pat)) | .spec.version] | first // "N/A"
        '
}

cnv_version=$(get_csv_version "openshift-cnv" "kubevirt-hyperconverged-operator")
hco_version="$cnv_version"
odf_version=$(get_csv_version "openshift-storage" "odf-operator")
sriov_version=$(get_csv_version "openshift-sriov-network-operator" "sriov-network-operator")
nmstate_version=$(get_csv_version "openshift-nmstate" "kubernetes-nmstate-operator")

# =============================================================================
# STORAGE CLASSES
# =============================================================================

sc_json=$(oc_safe get sc -o json)

if [[ -n "$sc_json" ]]; then
    default_sc=$(echo "$sc_json" | jq -r '
        [.items[] | select(.metadata.annotations["storageclass.kubernetes.io/is-default-class"] == "true") | .metadata.name] | first // "none"
    ')
    storage_classes=$(echo "$sc_json" | jq '[
        .items[] | {
            name: .metadata.name,
            provisioner: .provisioner,
            reclaimPolicy: .reclaimPolicy
        }
    ]')
else
    default_sc="unknown"
    storage_classes="[]"
fi

# =============================================================================
# KUBE-BURNER VERSION
# =============================================================================

kb_version=""
kb_job_summary="${RESULTS_DIR}/iteration-1/jobSummary.json"
if [[ -f "$kb_job_summary" ]]; then
    kb_version=$(jq -r '.[0].version // ""' "$kb_job_summary" 2>/dev/null)
fi
if [[ -z "$kb_version" ]]; then
    kb_version=$(kube-burner version 2>/dev/null | head -1 || echo "unknown")
fi

# =============================================================================
# TEST CONFIGURATION (from vars file)
# =============================================================================

if [[ -n "$VARS_FILE" && -f "$VARS_FILE" ]]; then
    tc_vmCount=$(get_yaml_value "vmCount" "$VARS_FILE" "")
    if [[ -z "$tc_vmCount" || "$tc_vmCount" == "0" ]]; then
        tc_vmCount=$(get_yaml_value "vmsPerNamespace" "$VARS_FILE" "0")
    fi

    tc_cpuCores=$(get_yaml_value "cpuCores" "$VARS_FILE" "")
    if [[ -z "$tc_cpuCores" || "$tc_cpuCores" == "0" ]]; then
        tc_cpuCores=$(get_yaml_value "vmCpuRequest" "$VARS_FILE" "")
    fi
    if [[ -z "$tc_cpuCores" || "$tc_cpuCores" == "0" ]]; then
        tc_cpuCores=$(get_yaml_value "vmCpuCores" "$VARS_FILE" "")
    fi
    if [[ -z "$tc_cpuCores" || "$tc_cpuCores" == "0" ]]; then
        tc_cpuCores=$(get_yaml_value "minCpu" "$VARS_FILE" "0")
    fi
    # Normalize Kubernetes CPU quantities (e.g. "100m" → 100) to integer for ES long mapping
    tc_cpuCores="${tc_cpuCores%\"}"
    tc_cpuCores="${tc_cpuCores#\"}"
    if [[ "$tc_cpuCores" =~ ^[0-9]+m$ ]]; then
        tc_cpuCores="${tc_cpuCores%m}"
    fi

    tc_memory=$(get_yaml_value "memory" "$VARS_FILE" "")
    if [[ -z "$tc_memory" ]]; then
        tc_memory=$(get_yaml_value "memorySize" "$VARS_FILE" "")
    fi
    if [[ -z "$tc_memory" ]]; then
        tc_memory=$(get_yaml_value "highMemory" "$VARS_FILE" "")
    fi
    if [[ -z "$tc_memory" ]]; then
        tc_memory=$(get_yaml_value "minMemory" "$VARS_FILE" "")
    fi
    if [[ -z "$tc_memory" ]]; then
        tc_memory=$(get_yaml_value "vmMemory" "$VARS_FILE" "unknown")
    fi

    tc_storage=$(get_yaml_value "storage" "$VARS_FILE" "")
    if [[ -z "$tc_storage" ]]; then
        tc_storage=$(get_yaml_value "diskSize" "$VARS_FILE" "")
    fi
    if [[ -z "$tc_storage" ]]; then
        tc_storage=$(get_yaml_value "rootStorage" "$VARS_FILE" "")
    fi
    if [[ -z "$tc_storage" ]]; then
        tc_storage=$(get_yaml_value "largeDiskSize" "$VARS_FILE" "")
    fi
    if [[ -z "$tc_storage" ]]; then
        tc_storage=$(get_yaml_value "minStorage" "$VARS_FILE" "")
    fi
    if [[ -z "$tc_storage" ]]; then
        tc_storage=$(get_yaml_value "storageSize" "$VARS_FILE" "unknown")
    fi

    tc_storageClassName=$(get_yaml_value "storageClassName" "$VARS_FILE" "unknown")
else
    tc_vmCount="0"
    tc_cpuCores="0"
    tc_memory="unknown"
    tc_storage="unknown"
    tc_storageClassName="unknown"
fi

runtimeConfig="{}"
if [[ -n "$VARS_FILE" && -f "$VARS_FILE" ]]; then
    runtimeConfig=$(python3 -c "
import json, sys
try:
    import yaml
    with open(sys.argv[1]) as f:
        d = yaml.safe_load(f) or {}
except ImportError:
    import re
    d = {}
    with open(sys.argv[1]) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and ':' in line:
                k, _, v = line.partition(':')
                k = k.strip()
                v = v.strip().strip('\"').strip(\"'\")
                if v and k:
                    d[k] = v
sensitive = {'PROM_TOKEN', 'privateKey', 'password', 'token', 'secret'}
flat = {}
for k, v in d.items():
    if k in sensitive or 'token' in k.lower() or 'password' in k.lower():
        continue
    if isinstance(v, (list, dict)):
        flat[k] = json.dumps(v)
    else:
        flat[k] = str(v)
json.dump(flat, sys.stdout)
" "$VARS_FILE" 2>/dev/null) || runtimeConfig="{}"
fi

# =============================================================================
# TEST CATEGORY (derived from test name)
# =============================================================================

declare -A CATEGORY_MAP=(
    ["cpu-limits"]="Resource Limits"
    ["memory-limits"]="Resource Limits"
    ["disk-limits"]="Resource Limits"
    ["disk-hotplug"]="Hot-plug"
    ["nic-hotplug"]="Hot-plug"
    ["high-memory"]="Performance"
    ["large-disk"]="Performance"
    ["minimal-resources"]="Performance"
    ["per-host-density"]="Scale"
    ["virt-capacity-benchmark"]="Scale"
)

test_category="Unknown"
for key in "${!CATEGORY_MAP[@]}"; do
    if [[ "$TEST_NAME" == *"$key"* ]]; then
        test_category="${CATEGORY_MAP[$key]}"
        break
    fi
done

# =============================================================================
# VALIDATION SUMMARY (from validation JSON files)
# =============================================================================

val_total=0
val_passed=0
val_failed=0
val_skipped=0
val_overall="UNKNOWN"

search_dir="${VALIDATION_DIR:-${RESULTS_DIR}}"
val_files=()
while IFS= read -r -d '' f; do
    val_files+=("$f")
done < <(find "$search_dir" -name "validation-*.json" -type f -print0 2>/dev/null)

if [[ ${#val_files[@]} -gt 0 ]]; then
    for vf in "${val_files[@]}"; do
        file_status=$(jq -r '.overallStatus // .status // "UNKNOWN"' "$vf" 2>/dev/null)
        file_validations=$(jq -r '.validations // []' "$vf" 2>/dev/null)

        count=$(echo "$file_validations" | jq 'length' 2>/dev/null || echo 0)
        p=$(echo "$file_validations" | jq '[.[] | select(.status == "PASS")] | length' 2>/dev/null || echo 0)
        f_count=$(echo "$file_validations" | jq '[.[] | select(.status == "FAIL" or .status == "FAILED")] | length' 2>/dev/null || echo 0)
        s=$(echo "$file_validations" | jq '[.[] | select(.status == "SKIP")] | length' 2>/dev/null || echo 0)

        val_total=$((val_total + count))
        val_passed=$((val_passed + p))
        val_failed=$((val_failed + f_count))
        val_skipped=$((val_skipped + s))
    done

    if [[ $val_failed -gt 0 ]]; then
        val_overall="FAILURE"
    elif [[ $val_passed -gt 0 ]]; then
        val_overall="SUCCESS"
    fi
fi

# Determine test result from exit code + validation
if [[ -n "$EXIT_CODE" && "$EXIT_CODE" != "0" ]]; then
    test_result="FAILURE"
elif [[ "$val_overall" == "FAILURE" ]]; then
    test_result="FAILURE"
elif [[ "$val_overall" == "SUCCESS" ]]; then
    test_result="SUCCESS"
elif [[ -n "$EXIT_CODE" && "$EXIT_CODE" == "0" ]]; then
    test_result="SUCCESS"
else
    test_result="UNKNOWN"
fi

# =============================================================================
# BUILD METADATA JSON
# =============================================================================

metadata_file="${RESULTS_DIR}/metadata.json"

jq -n \
    --arg uuid "$UUID" \
    --arg timestamp "$(date -Iseconds)" \
    --arg metricName "metadata" \
    --arg testName "$TEST_NAME" \
    --arg testMode "${MODE:-unknown}" \
    --arg runTimestamp "${RUN_TIMESTAMP:-unknown}" \
    --arg kubeBurnerVersion "$kb_version" \
    --arg testResult "$test_result" \
    --argjson exitCode "${EXIT_CODE:-null}" \
    --argjson durationSeconds "${DURATION:-null}" \
    --arg testCategory "$test_category" \
    --arg ocpVersion "${ocp_version:-unknown}" \
    --arg clusterId "${cluster_id:-unknown}" \
    --arg platform "${platform:-unknown}" \
    --arg apiUrl "${api_url:-unknown}" \
    --arg networkType "${network_type:-unknown}" \
    --argjson nodeTotal "${node_total:-0}" \
    --argjson nodeMasters "${node_masters:-0}" \
    --argjson nodeWorkers "${node_workers:-0}" \
    --argjson workerDetails "${worker_details:-[]}" \
    --arg cnvVersion "${cnv_version:-N/A}" \
    --arg hcoVersion "${hco_version:-N/A}" \
    --arg odfVersion "${odf_version:-N/A}" \
    --arg sriovVersion "${sriov_version:-N/A}" \
    --arg nmstateVersion "${nmstate_version:-N/A}" \
    --arg defaultStorageClass "$default_sc" \
    --argjson storageClasses "$storage_classes" \
    --arg varsFile "${VARS_FILE:-}" \
    --arg vmCount "${tc_vmCount}" \
    --arg cpuCores "${tc_cpuCores}" \
    --arg memory "$tc_memory" \
    --arg storage "$tc_storage" \
    --arg storageClassName "$tc_storageClassName" \
    --argjson runtimeConfig "$runtimeConfig" \
    --argjson valTotal "$val_total" \
    --argjson valPassed "$val_passed" \
    --argjson valFailed "$val_failed" \
    --argjson valSkipped "$val_skipped" \
    --arg valOverall "$val_overall" \
    '{
        uuid: $uuid,
        timestamp: $timestamp,
        metricName: $metricName,
        testName: $testName,
        testMode: $testMode,
        runTimestamp: $runTimestamp,
        kubeBurnerVersion: $kubeBurnerVersion,
        testResult: $testResult,
        exitCode: $exitCode,
        durationSeconds: $durationSeconds,
        testCategory: $testCategory,
        cluster: {
            ocpVersion: $ocpVersion,
            clusterId: $clusterId,
            platform: $platform,
            apiUrl: $apiUrl,
            networkType: $networkType
        },
        nodes: {
            total: $nodeTotal,
            masters: $nodeMasters,
            workers: $nodeWorkers,
            workerDetails: $workerDetails
        },
        operators: {
            cnvVersion: $cnvVersion,
            hcoVersion: $hcoVersion,
            odfVersion: $odfVersion,
            sriovVersion: $sriovVersion,
            nmstateVersion: $nmstateVersion
        },
        storage: {
            defaultClass: $defaultStorageClass,
            classes: $storageClasses
        },
        testConfig: {
            varsFile: $varsFile,
            vmCount: ($vmCount | tonumber? // $vmCount),
            cpuCores: ($cpuCores | tonumber? // $cpuCores),
            memory: $memory,
            storage: $storage,
            storageClassName: $storageClassName
        },
        runtimeConfig: $runtimeConfig,
        validationSummary: {
            totalPhases: $valTotal,
            passed: $valPassed,
            failed: $valFailed,
            skipped: $valSkipped,
            overallStatus: $valOverall
        }
    }' > "$metadata_file"

echo "metadata-collector: Metadata saved to ${metadata_file}"

# =============================================================================
# ELASTICSEARCH INDEXING
# =============================================================================

if [[ -n "$ES_SERVER" ]]; then
    echo "metadata-collector: Indexing metadata to Elasticsearch..."

    # Index to dedicated metadata index
    response=$(curl -s -o /dev/null -w "%{http_code}" \
        -X POST "${ES_SERVER}/${METADATA_INDEX}/_doc" \
        -H 'Content-Type: application/json' \
        -k \
        -d @"$metadata_file" 2>/dev/null) || true

    if [[ "$response" == "201" || "$response" == "200" ]]; then
        echo "metadata-collector: Indexed to ${METADATA_INDEX} (HTTP ${response})"
    else
        echo "metadata-collector: WARNING: Failed to index to ${METADATA_INDEX} (HTTP ${response})" >&2
    fi

    # Index to per-test index if specified
    if [[ -n "$TEST_INDEX" ]]; then
        response=$(curl -s -o /dev/null -w "%{http_code}" \
            -X POST "${ES_SERVER}/${TEST_INDEX}/_doc" \
            -H 'Content-Type: application/json' \
            -k \
            -d @"$metadata_file" 2>/dev/null) || true

        if [[ "$response" == "201" || "$response" == "200" ]]; then
            echo "metadata-collector: Indexed to ${TEST_INDEX} (HTTP ${response})"
        else
            echo "metadata-collector: WARNING: Failed to index to ${TEST_INDEX} (HTTP ${response})" >&2
        fi
    fi
fi

echo "metadata-collector: Done."
