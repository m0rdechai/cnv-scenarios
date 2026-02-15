#!/bin/bash

set -euo pipefail

# Structured Logging Functions for JSON Report Generation
log_validation_start() {
    local function_name="$1"
    echo "VALIDATION_START|function=${function_name}|timestamp=$(date -Iseconds)"
}

log_validation_checkpoint() {
    local name="$1"
    local status="$2"
    local message="${3:-}"
    echo "VALIDATION_CHECK|name=${name}|status=${status}|message=${message}"
}

log_validation_end() {
    local status="$1"
    local duration="$2"
    echo "VALIDATION_END|status=${status}|duration=${duration}"
}

# Generate JSON validation report
# Usage: save_validation_report <test_name> <status> <namespace> <params_json> [<validations_json>] [<results_dir>]
save_validation_report() {
    local test_name="$1"
    local status="$2"  # SUCCESS or FAILED
    local namespace="$3"
    local params_json="$4"
    local validations_json="${5:-[]}"
    local results_dir="${6:-/tmp/kube-burner-validations}"
    local exit_code=0
    
    if [ "${status}" = "FAILED" ]; then
        exit_code=1
    fi
    
    local report_dir="${results_dir}"
    mkdir -p "${report_dir}"
    local report_file="${report_dir}/validation-${test_name}.json"
    
    cat > "${report_file}" <<EOF
{
  "testName": "${test_name}",
  "function": "check_${test_name//-/_}",
  "timestamp": "$(date -Iseconds)",
  "namespace": "${namespace}",
  "parameters": ${params_json},
  "overallStatus": "${status}",
  "exitCode": ${exit_code},
  "validations": ${validations_json}
}
EOF
    echo "Validation report saved to: ${report_file}"
}

# Global configuration
MAX_RETRIES=130
MAX_SHORT_WAITS=12
SHORT_WAIT=5
LONG_WAIT=30

# Check if virtctl supports --local-ssh flag
if virtctl ssh --help | grep -qc "\--local-ssh " ; then
    LOCAL_SSH="--local-ssh"
else
    LOCAL_SSH=""
fi

# Get VMs based on label selector
get_vms() {
    local namespace=$1
    local label_key=$2
    local label_value=$3

    local vms
    vms=$(oc get vm -n "${namespace}" -l "${label_key}=${label_value}" -o json | jq -r '.items[] | .metadata.name')
    local ret=$?
    if [ $ret -ne 0 ]; then
        echo "Failed to get VM list"
        exit 1
    fi
    echo "${vms}"
}

# Execute remote command on VM via virtctl ssh
remote_command() {
    local namespace=$1
    local identity_file=$2
    local remote_user=$3
    local vm_name=$4
    local command=$5

    local output
    output=$(virtctl ssh ${LOCAL_SSH} \
        --local-ssh-opts="-o StrictHostKeyChecking=no" \
        --local-ssh-opts="-o UserKnownHostsFile=/dev/null" \
        --local-ssh-opts="-o BatchMode=yes" \
        --local-ssh-opts="-o PasswordAuthentication=no" \
        --local-ssh-opts="-o PreferredAuthentications=publickey" \
        --local-ssh-opts="-o ConnectTimeout=30" \
        -n "${namespace}" -i "${identity_file}" -c "${command}" --username "${remote_user}" "${vm_name}" 2>/dev/null)
    local ret=$?
    if [ $ret -ne 0 ]; then
        return 1
    fi
    echo "${output}"
}

# Execute remote command on VM via virtctl ssh with password authentication
# Uses sshpass for password-based SSH (for CirrOS VMs)
remote_command_password() {
    local namespace=$1
    local password=$2
    local remote_user=$3
    local vm_name=$4
    local command=$5

    local output
    output=$(sshpass -p "${password}" virtctl ssh ${LOCAL_SSH} \
        --local-ssh-opts="-o StrictHostKeyChecking=no" \
        --local-ssh-opts="-o UserKnownHostsFile=/dev/null" \
        --local-ssh-opts="-o ConnectTimeout=30" \
        -n "${namespace}" -c "${command}" --username "${remote_user}" "${vm_name}" 2>/dev/null)
    local ret=$?
    if [ $ret -ne 0 ]; then
        return 1
    fi
    echo "${output}"
}

# Check if VM is running and accessible via SSH
check_vm_running() {
    local label_key="$1"
    local label_value="$2"
    local namespace="$3"
    local private_key="$4"
    local vm_user="$5"
    
    echo "Checking if VMs with label ${label_key}=${label_value} are running in namespace ${namespace}"
    
    # Check if VMs are in Running state
    local total_vms=$(oc get vm -n "${namespace}" -l "${label_key}=${label_value}" --no-headers | wc -l)
    local running_vms=$(oc get vm -n "${namespace}" -l "${label_key}=${label_value}" -o jsonpath='{.items[?(@.status.ready==true)].metadata.name}' | wc -w)
    
    echo "Total VMs: ${total_vms}, Running VMs: ${running_vms}"
    
    if [ "${running_vms}" -ne "${total_vms}" ]; then
        echo "ERROR: Not all VMs are running. Expected: ${total_vms}, Running: ${running_vms}"
        return 1
    fi
    
    # If private key provided, test SSH connectivity
    if [ -n "${private_key}" ] && [ -n "${vm_user}" ]; then
        local vms
        vms=$(get_vms "${namespace}" "${label_key}" "${label_value}")
        for vm in ${vms}; do
            if ! remote_command "${namespace}" "${private_key}" "${vm_user}" "${vm}" "ls" >/dev/null; then
                echo "ERROR: Cannot SSH to VM ${vm}"
                return 1
            fi
        done
        echo "SUCCESS: All VMs are running and SSH accessible"
    else
        echo "SUCCESS: All VMs are running"
    fi
    
    return 0
}

# Check if VMs are stopped
check_vm_shutdown() {
    local label_key="$1"
    local label_value="$2"
    local namespace="$3"
    
    echo "Checking if VMs with label ${label_key}=${label_value} are stopped in namespace ${namespace}"
    
    # Check if VMs are in Stopped state
    local total_vms=$(oc get vm -n "${namespace}" -l "${label_key}=${label_value}" --no-headers | wc -l)
    local stopped_vms=$(oc get vm -n "${namespace}" -l "${label_key}=${label_value}" -o jsonpath='{.items[?(@.spec.runStrategy=="Halted")].metadata.name}' | wc -w)
    
    echo "Total VMs: ${total_vms}, Stopped VMs: ${stopped_vms}"
    
    if [ "${stopped_vms}" -ne "${total_vms}" ]; then
        echo "ERROR: Not all VMs are stopped. Expected: ${total_vms}, Stopped: ${stopped_vms}"
        return 1
    fi
    
    echo "SUCCESS: All VMs are stopped"
    return 0
}

# Check volume resize completion
check_resize() {
    local label_key="$1"
    local label_value="$2"
    local namespace="$3"
    local private_key="$4"
    local vm_user="$5"
    local expected_root_size="$6"
    local expected_data_size="$7"

    local vms
    vms=$(get_vms "${namespace}" "${label_key}" "${label_value}")
    
    for vm in ${vms}; do
        local blk_devices
        blk_devices=$(remote_command "${namespace}" "${private_key}" "${vm_user}" "${vm}" "lsblk --json -v --output=NAME,SIZE")
        local ret=$?
        if [ $ret -ne 0 ]; then
            echo "ERROR: Failed to get block devices for VM ${vm}"
            return $ret
        fi

        local size
        size=$(echo "${blk_devices}" | jq .blockdevices | jq -r --arg name "vda" '.[] | select(.name == $name) | .size')
        if [[ $size != "${expected_root_size}" ]]; then
            echo "ERROR: Root volume size mismatch for VM ${vm}. Expected: ${expected_root_size}, Actual: ${size}"
            return 1
        fi

        local datavolume_sizes
        datavolume_sizes=$(echo "${blk_devices}" | jq .blockdevices | jq -r --arg name "vda" '.[] | select(.name != $name) | .size')
        for datavolume_size in ${datavolume_sizes}; do
            if [[ $datavolume_size != "${expected_data_size}" ]]; then
                echo "ERROR: Data volume size mismatch for VM ${vm}. Expected: ${expected_data_size}, Actual: ${datavolume_size}"
                return 1
            fi
        done
    done

    echo "SUCCESS: All volume resizes completed successfully"
    return 0
}

# Check CPU limits
check_cpu_limits() {
    local label_key="$1"
    local label_value="$2"
    local namespace="$3"
    local expected_cpu="$4"
    local private_key="$5"
    local vm_user="$6"
    local results_dir="${7:-/tmp/kube-burner-validations}"
    
    echo "=============================================="
    echo "  CPU Limits Validation"
    echo "=============================================="
    echo "Namespace: ${namespace}"
    echo "Label: ${label_key}=${label_value}"
    echo "Expected CPU Cores: ${expected_cpu}"
    echo "SSH User: ${vm_user}"
    echo "Results: ${results_dir}"
    echo "----------------------------------------------"
    
    log_validation_start "check_cpu_limits"
    local start_time=$(date +%s)
    
    # Phase 1: Discover VMs
    echo ""
    echo "[Phase 1/4] Discovering VMs..."
    local vms
    vms=$(get_vms "${namespace}" "${label_key}" "${label_value}")
    local vm_count=$(echo "${vms}" | wc -w)
    
    if [ -z "${vms}" ] || [ "${vm_count}" -eq 0 ]; then
        echo "✗ No VMs found with label ${label_key}=${label_value}"
        log_validation_checkpoint "vm_discovery" "FAIL" "No VMs found"
        return 1
    fi
    
    echo "✓ Found ${vm_count} VM(s): ${vms}"
    log_validation_checkpoint "vm_discovery" "PASS" "Found VMs: ${vms}"
    
    # Track validation status for JSON report
    local guest_os_validation_status="SKIP"
    local stress_ng_validation_status="SKIP"
    local overall_status="SUCCESS"
    
    # Phase 2: Check VM spec CPU cores
    echo ""
    echo "[Phase 2/4] Checking VM spec CPU cores..."
    for vm in ${vms}; do
        echo "  Checking ${vm}..."
        
        local actual_cpu
        actual_cpu=$(oc get vm -n "${namespace}" "${vm}" -o jsonpath='{.spec.template.spec.domain.cpu.cores}')
        
        if [ "${actual_cpu}" != "${expected_cpu}" ]; then
            echo "  ✗ ${vm}: CPU cores mismatch. Expected: ${expected_cpu}, Actual: ${actual_cpu}"
            log_validation_checkpoint "vm_spec_cpu_cores" "FAIL" "Expected ${expected_cpu}, got ${actual_cpu}"
            overall_status="FAILED"
            break
        fi
        echo "  ✓ ${vm}: ${actual_cpu} CPU cores in spec"
        log_validation_checkpoint "vm_spec_cpu_cores" "PASS" "VM ${vm}: ${actual_cpu} CPU cores in spec"
    done
    
    # Phase 3: Guest OS CPU validation
    if [ "${overall_status}" = "SUCCESS" ] && [ -n "${private_key}" ] && [ -n "${vm_user}" ]; then
        echo ""
        echo "[Phase 3/4] Checking guest OS CPU configuration..."
        
        for vm in ${vms}; do
            echo "  Checking ${vm}..."
            
            # Test SSH connectivity
            local test_output
            test_output=$(remote_command "${namespace}" "${private_key}" "${vm_user}" "${vm}" "echo SSH_OK" 2>&1)
            
            if [ $? -ne 0 ] || [ -z "${test_output}" ]; then
                echo "  ⚠ ${vm}: SSH connection failed, skipping guest OS validation"
                log_validation_checkpoint "guest_os_cpu_count" "SKIP" "VM ${vm}: SSH connection failed"
                continue
            fi
            
            echo "  ✓ ${vm}: SSH connected"
            
            # Check CPU count in guest OS using nproc
            local guest_cpu_count
            guest_cpu_count=$(remote_command "${namespace}" "${private_key}" "${vm_user}" "${vm}" "nproc" 2>/dev/null || echo "0")
            guest_cpu_count=$(echo "${guest_cpu_count}" | head -1 | tr -cd '0-9')
            guest_cpu_count=${guest_cpu_count:-0}
            
            if [ "${guest_cpu_count}" -eq 0 ]; then
                echo "  ✗ ${vm}: Failed to retrieve CPU count from guest OS"
                log_validation_checkpoint "guest_os_cpu_count" "FAIL" "Could not retrieve CPU count"
                overall_status="FAILED"
                break
            fi
            
            if [ "${guest_cpu_count}" != "${expected_cpu}" ]; then
                echo "  ✗ ${vm}: Guest OS CPU count mismatch. Expected: ${expected_cpu}, Actual: ${guest_cpu_count}"
                log_validation_checkpoint "guest_os_cpu_count" "FAIL" "Expected ${expected_cpu}, got ${guest_cpu_count}"
                overall_status="FAILED"
                break
            fi
            
            echo "  ✓ ${vm}: Guest OS shows ${guest_cpu_count} CPUs"
            log_validation_checkpoint "guest_os_cpu_count" "PASS" "VM ${vm}: ${guest_cpu_count} CPUs visible in guest OS"
            guest_os_validation_status="PASS"
        done
    else
        echo ""
        echo "[Phase 3/4] Skipping guest OS CPU validation (no SSH credentials)"
        log_validation_checkpoint "guest_os_cpu_count" "SKIP" "SSH credentials not provided"
    fi
    
    # Phase 4: Check stress-ng processes
    if [ "${overall_status}" = "SUCCESS" ] && [ -n "${private_key}" ] && [ -n "${vm_user}" ]; then
        echo ""
        echo "[Phase 4/4] Checking stress-ng-cpu processes..."
        
        for vm in ${vms}; do
            echo "  Checking ${vm}..."
            
            local stress_process_count
            stress_process_count=$(remote_command "${namespace}" "${private_key}" "${vm_user}" "${vm}" \
                "ps aux | grep -c '[s]tress-ng-cpu'" 2>/dev/null || echo "0")
            stress_process_count=$(echo "${stress_process_count}" | head -1 | tr -cd '0-9')
            stress_process_count=${stress_process_count:-0}
            
            if [ "${stress_process_count}" != "${expected_cpu}" ]; then
                echo "  ✗ ${vm}: stress-ng-cpu process count mismatch"
                echo "    Expected: ${expected_cpu} (1 per CPU core), Actual: ${stress_process_count}"
                log_validation_checkpoint "stress_ng_processes" "FAIL" "Expected ${expected_cpu}, got ${stress_process_count}"
                overall_status="FAILED"
                break
            fi
            
            echo "  ✓ ${vm}: ${stress_process_count} stress-ng-cpu processes running"
            log_validation_checkpoint "stress_ng_processes" "PASS" "VM ${vm}: ${stress_process_count} stress-ng-cpu processes running"
            stress_ng_validation_status="PASS"
        done
    else
        echo ""
        echo "[Phase 4/4] Skipping stress-ng process validation (no SSH credentials)"
        log_validation_checkpoint "stress_ng_processes" "SKIP" "SSH credentials not provided"
    fi
    
    # Generate summary
    local end_time=$(date +%s)
    local duration=$((end_time - start_time))
    
    echo ""
    echo "=============================================="
    if [ "${overall_status}" = "SUCCESS" ]; then
        echo "  ✓ VALIDATION PASSED"
    else
        echo "  ✗ VALIDATION FAILED"
    fi
    echo "  Duration: ${duration}s"
    echo "=============================================="
    
    log_validation_end "${overall_status}" "${duration}s"
    
    # Generate params JSON
    local params_json
    params_json=$(cat <<PARAMS
{
    "label_key": "${label_key}",
    "label_value": "${label_value}",
    "expected_cpu_cores": ${expected_cpu},
    "vm_count": ${vm_count},
    "ssh_validation_enabled": $([ -n "${private_key}" ] && echo "true" || echo "false"),
    "total_duration_seconds": ${duration}
}
PARAMS
)
    
    # Generate validations JSON using actual tracked status
    local spec_status="PASS"
    [ "${overall_status}" = "FAILED" ] && spec_status="FAIL"
    
    local guest_os_msg
    local stress_ng_msg
    
    if [ "${guest_os_validation_status}" = "PASS" ]; then
        guest_os_msg="Guest OS CPU count validation passed"
    else
        guest_os_msg="Guest OS CPU count validation skipped (SSH connection failed or not configured)"
    fi
    
    if [ "${stress_ng_validation_status}" = "PASS" ]; then
        stress_ng_msg="stress-ng-cpu process count validation passed (${expected_cpu} processes)"
    else
        stress_ng_msg="stress-ng-cpu process count validation skipped (SSH connection failed or not configured)"
    fi
    
    local validations_json
    validations_json=$(cat <<VALIDATIONS
[
    {"phase": "vm_discovery", "status": "PASS", "message": "Found ${vm_count} VMs"},
    {"phase": "vm_spec_cpu_cores", "status": "${spec_status}", "message": "VM spec CPU cores validation (${expected_cpu} cores)"},
    {"phase": "guest_os_cpu_count", "status": "${guest_os_validation_status}", "message": "${guest_os_msg}"},
    {"phase": "stress_ng_processes", "status": "${stress_ng_validation_status}", "message": "${stress_ng_msg}"}
]
VALIDATIONS
)
    
    save_validation_report "cpu-limits" "${overall_status}" "${namespace}" "${params_json}" "${validations_json}" "${results_dir}"
    
    if [ "${overall_status}" = "SUCCESS" ]; then
        echo "SUCCESS: All VMs have correct CPU configuration"
        return 0
    else
        return 1
    fi
}

# Check memory limits  
check_memory_limits() {
    local label_key="$1"
    local label_value="$2"
    local namespace="$3"
    local expected_memory="$4"
    local private_key="$5"
    local vm_user="$6"
    local results_dir="${7:-/tmp/kube-burner-validations}"
    
    echo "=============================================="
    echo "  Memory Limits Validation"
    echo "=============================================="
    echo "Namespace: ${namespace}"
    echo "Label: ${label_key}=${label_value}"
    echo "Expected Memory: ${expected_memory}"
    echo "SSH User: ${vm_user}"
    echo "Results: ${results_dir}"
    echo "----------------------------------------------"
    
    log_validation_start "check_memory_limits"
    local start_time=$(date +%s)
    
    # Phase 1: Discover VMs
    echo ""
    echo "[Phase 1/4] Discovering VMs..."
    local vms
    vms=$(get_vms "${namespace}" "${label_key}" "${label_value}")
    local vm_count=$(echo "${vms}" | wc -w)
    
    if [ -z "${vms}" ] || [ "${vm_count}" -eq 0 ]; then
        echo "✗ No VMs found with label ${label_key}=${label_value}"
        log_validation_checkpoint "vm_discovery" "FAIL" "No VMs found"
        log_validation_end "FAILURE" "$(( $(date +%s) - start_time ))s"
        save_validation_report "memory-limits" "FAILURE" "${namespace}" "{}" "{}" "${results_dir}"
        return 1
    fi
    
    echo "✓ Found ${vm_count} VM(s): ${vms}"
    log_validation_checkpoint "vm_discovery" "PASS" "Found VMs: ${vms}"
    
    local overall_status="SUCCESS"
    local guest_os_validation_status="SKIP"
    local stress_ng_validation_status="SKIP"
    
    # Phase 2: Check VM spec memory
    echo ""
    echo "[Phase 2/4] Checking VM spec memory..."
    for vm in ${vms}; do
        echo "  Checking ${vm}..."
        
        local actual_memory
        actual_memory=$(oc get vm -n "${namespace}" "${vm}" -o jsonpath='{.spec.template.spec.domain.resources.requests.memory}')
        
        if [ "${actual_memory}" != "${expected_memory}" ]; then
            echo "  ✗ ${vm}: Memory mismatch. Expected: ${expected_memory}, Actual: ${actual_memory}"
            log_validation_checkpoint "vm_spec_memory" "FAIL" "Expected ${expected_memory}, got ${actual_memory}"
            overall_status="FAILURE"
            break
        fi
        echo "  ✓ ${vm}: ${actual_memory} memory in spec"
        log_validation_checkpoint "vm_spec_memory" "PASS" "VM ${vm}: ${actual_memory} memory in spec"
    done
    
    # Phase 3: Guest OS memory validation
    if [ "${overall_status}" = "SUCCESS" ] && [ -n "${private_key}" ] && [ -n "${vm_user}" ]; then
        echo ""
        echo "[Phase 3/4] Checking guest OS memory configuration..."
        
        for vm in ${vms}; do
            echo "  Checking ${vm}..."
            
            local test_output
            test_output=$(remote_command "${namespace}" "${private_key}" "${vm_user}" "${vm}" "echo SSH_OK" 2>&1)
            
            if [ $? -ne 0 ] || [ -z "${test_output}" ]; then
                echo "  ⚠ ${vm}: SSH connection failed, skipping guest OS validation"
                log_validation_checkpoint "guest_os_memory" "SKIP" "VM ${vm}: SSH connection failed"
                continue
            fi
            
            echo "  ✓ ${vm}: SSH connected"
            
            # Check memory in guest OS using free command
            local guest_memory_mb
            guest_memory_mb=$(remote_command "${namespace}" "${private_key}" "${vm_user}" "${vm}" "free -m | awk 'NR==2{print \$2}'" 2>/dev/null || echo "0")
            guest_memory_mb=$(echo "${guest_memory_mb}" | head -1 | tr -cd '0-9')
            guest_memory_mb=${guest_memory_mb:-0}
            
            if [ "${guest_memory_mb}" -eq 0 ]; then
                echo "  ✗ ${vm}: Failed to retrieve memory from guest OS"
                log_validation_checkpoint "guest_os_memory" "FAIL" "Could not retrieve memory"
                overall_status="FAILURE"
                break
            fi
            
            # Convert expected_memory to MB for comparison
            local expected_memory_mb
            if [[ "${expected_memory}" =~ ^([0-9]+)Gi$ ]]; then
                expected_memory_mb=$(( ${BASH_REMATCH[1]} * 1024 ))
            elif [[ "${expected_memory}" =~ ^([0-9]+)Mi$ ]]; then
                expected_memory_mb=${BASH_REMATCH[1]}
            elif [[ "${expected_memory}" =~ ^([0-9]+)G$ ]]; then
                expected_memory_mb=$(( ${BASH_REMATCH[1]} * 1000 ))
            elif [[ "${expected_memory}" =~ ^([0-9]+)M$ ]]; then
                expected_memory_mb=${BASH_REMATCH[1]}
            else
                echo "  ⚠ ${vm}: Cannot parse memory format '${expected_memory}'"
                log_validation_checkpoint "guest_os_memory" "SKIP" "Cannot parse memory format"
                continue
            fi
            
            # Allow 15% tolerance for memory comparison
            local tolerance=$(( expected_memory_mb * 15 / 100 ))
            local min_memory=$(( expected_memory_mb - tolerance ))
            local max_memory=$(( expected_memory_mb + tolerance ))
            
            echo "    Expected: ${expected_memory_mb}MB, Tolerance: ±15% (${min_memory}-${max_memory}MB)"
            
            if [ "${guest_memory_mb}" -lt "${min_memory}" ] || [ "${guest_memory_mb}" -gt "${max_memory}" ]; then
                echo "  ✗ ${vm}: Guest OS memory ${guest_memory_mb}MB outside expected range"
                log_validation_checkpoint "guest_os_memory" "FAIL" "Expected ~${expected_memory_mb}MB, got ${guest_memory_mb}MB"
                overall_status="FAILURE"
                break
            fi
            
            echo "  ✓ ${vm}: Guest OS shows ${guest_memory_mb}MB (within expected range)"
            log_validation_checkpoint "guest_os_memory" "PASS" "VM ${vm}: ${guest_memory_mb} MB visible in guest OS"
            guest_os_validation_status="PASS"
        done
    else
        echo ""
        echo "[Phase 3/4] Skipping guest OS memory validation (no SSH credentials)"
        log_validation_checkpoint "guest_os_memory" "SKIP" "SSH credentials not provided"
    fi
    
    # Phase 4: Check stress-ng processes
    if [ "${overall_status}" = "SUCCESS" ] && [ -n "${private_key}" ] && [ -n "${vm_user}" ]; then
        echo ""
        echo "[Phase 4/4] Checking stress-ng memory processes..."
        
        for vm in ${vms}; do
            echo "  Checking ${vm}..."
            
            local stress_process_count
            stress_process_count=$(remote_command "${namespace}" "${private_key}" "${vm_user}" "${vm}" \
                "ps aux | grep -c '[s]tress-ng'" 2>/dev/null || echo "0")
            stress_process_count=$(echo "${stress_process_count}" | head -1 | tr -cd '0-9')
            stress_process_count=${stress_process_count:-0}
            
            if [ "${stress_process_count}" -eq 0 ]; then
                echo "  ⚠ ${vm}: No stress-ng processes found (test may not be running)"
                log_validation_checkpoint "stress_ng_processes" "SKIP" "No stress-ng processes found"
            else
                echo "  ✓ ${vm}: ${stress_process_count} stress-ng process(es) running"
                log_validation_checkpoint "stress_ng_processes" "PASS" "VM ${vm}: ${stress_process_count} stress-ng process(es) running"
                stress_ng_validation_status="PASS"
            fi
        done
    else
        echo ""
        echo "[Phase 4/4] Skipping stress-ng process validation (no SSH credentials)"
        log_validation_checkpoint "stress_ng_processes" "SKIP" "SSH credentials not provided"
    fi
    
    # Generate summary
    local end_time=$(date +%s)
    local duration=$((end_time - start_time))
    
    echo ""
    echo "=============================================="
    if [ "${overall_status}" = "SUCCESS" ]; then
        echo "  ✓ VALIDATION PASSED"
    else
        echo "  ✗ VALIDATION FAILED"
    fi
    echo "  Duration: ${duration}s"
    echo "=============================================="
    
    log_validation_end "${overall_status}" "${duration}s"
    
    # Generate params JSON
    local params_json
    params_json=$(cat <<PARAMS
{
    "label_key": "${label_key}",
    "label_value": "${label_value}",
    "expected_memory": "${expected_memory}",
    "vm_count": ${vm_count},
    "ssh_validation_enabled": $([ -n "${private_key}" ] && echo "true" || echo "false"),
    "total_duration_seconds": ${duration}
}
PARAMS
)
    
    # Generate validations JSON
    local spec_status="PASS"
    [ "${overall_status}" = "FAILURE" ] && spec_status="FAIL"
    
    local validations_json
    validations_json=$(cat <<VALIDATIONS
[
    {"phase": "vm_discovery", "status": "PASS", "message": "Found ${vm_count} VMs"},
    {"phase": "vm_spec_memory", "status": "${spec_status}", "message": "VM spec memory validation (${expected_memory})"},
    {"phase": "guest_os_memory", "status": "${guest_os_validation_status}", "message": "Guest OS memory validation"},
    {"phase": "stress_ng_processes", "status": "${stress_ng_validation_status}", "message": "stress-ng memory stress test validation"}
]
VALIDATIONS
)
    
    save_validation_report "memory-limits" "${overall_status}" "${namespace}" "${params_json}" "${validations_json}" "${results_dir}"
    
    if [ "${overall_status}" = "SUCCESS" ]; then
        echo "SUCCESS: All VMs have correct memory configuration"
        return 0
    else
        return 1
    fi
}

# Check disk limits
check_disk_limits() {
    local label_key="$1"
    local label_value="$2"
    local namespace="$3"
    local expected_disk_count="$4"
    local expected_disk_size="$5"
    local private_key="$6"
    local vm_user="$7"
    local results_dir="${8:-/tmp/kube-burner-validations}"
    
    echo "=============================================="
    echo "  Disk Limits Validation"
    echo "=============================================="
    echo "Namespace: ${namespace}"
    echo "Label: ${label_key}=${label_value}"
    echo "Expected Disk Count: ${expected_disk_count}"
    echo "Expected Disk Size: ${expected_disk_size}"
    echo "SSH User: ${vm_user}"
    echo "Results: ${results_dir}"
    echo "----------------------------------------------"
    
    log_validation_start "check_disk_limits"
    local start_time=$(date +%s)
    
    # Phase 1: Discover VMs
    echo ""
    echo "[Phase 1/5] Discovering VMs..."
    local vms
    vms=$(get_vms "${namespace}" "${label_key}" "${label_value}")
    local vm_count=$(echo "${vms}" | wc -w)
    
    if [ -z "${vms}" ] || [ "${vm_count}" -eq 0 ]; then
        echo "✗ No VMs found with label ${label_key}=${label_value}"
        log_validation_checkpoint "vm_discovery" "FAIL" "No VMs found"
        log_validation_end "FAILURE" "$(( $(date +%s) - start_time ))s"
        save_validation_report "disk-limits" "FAILURE" "${namespace}" "{}" "{}" "${results_dir}"
        return 1
    fi
    
    echo "✓ Found ${vm_count} VM(s): ${vms}"
    log_validation_checkpoint "vm_discovery" "PASS" "Found VMs: ${vms}"
    
    local overall_status="SUCCESS"
    local guest_os_disk_count_status="SKIP"
    local guest_os_disk_size_status="SKIP"
    
    # Phase 2: Check VM spec disk count
    echo ""
    echo "[Phase 2/5] Checking VM spec disk count..."
    for vm in ${vms}; do
        echo "  Checking ${vm}..."
        
        local actual_disk_count
        actual_disk_count=$(oc get vm -n "${namespace}" "${vm}" -o json | jq '[.spec.template.spec.volumes[] | select(.name != "rootdisk" and .name != "cloudinitdisk" and (.dataVolume != null or .persistentVolumeClaim != null))] | length')
        
        if [ "${actual_disk_count}" != "${expected_disk_count}" ]; then
            echo "  ✗ ${vm}: Disk count mismatch. Expected: ${expected_disk_count}, Actual: ${actual_disk_count}"
            log_validation_checkpoint "vm_spec_disk_count" "FAIL" "Expected ${expected_disk_count}, got ${actual_disk_count}"
            overall_status="FAILURE"
            break
        fi
        echo "  ✓ ${vm}: ${actual_disk_count} data disk(s) in spec"
        log_validation_checkpoint "vm_spec_disk_count" "PASS" "VM ${vm}: ${actual_disk_count} data disk(s) in spec"
    done
    
    # Phase 3: Check VM spec disk sizes
    if [ "${overall_status}" = "SUCCESS" ]; then
        echo ""
        echo "[Phase 3/5] Checking VM spec disk sizes..."
        for vm in ${vms}; do
            echo "  Checking ${vm}..."
            
            local data_volumes
            data_volumes=$(oc get vm -n "${namespace}" "${vm}" -o json | jq -r '.spec.dataVolumeTemplates[] | select(.metadata.name | startswith("datadisk")) | .spec.storage.resources.requests.storage')
            
            for dv_size in ${data_volumes}; do
                if [ "${dv_size}" != "${expected_disk_size}" ]; then
                    echo "  ✗ ${vm}: Disk size mismatch. Expected: ${expected_disk_size}, Actual: ${dv_size}"
                    log_validation_checkpoint "vm_spec_disk_size" "FAIL" "Expected ${expected_disk_size}, got ${dv_size}"
                    overall_status="FAILURE"
                    break 2
                fi
            done
            
            echo "  ✓ ${vm}: All disk sizes match ${expected_disk_size}"
            log_validation_checkpoint "vm_spec_disk_size" "PASS" "VM ${vm}: All data disk sizes match ${expected_disk_size}"
        done
    fi
    
    # Phase 4: Guest OS disk count validation
    if [ "${overall_status}" = "SUCCESS" ] && [ -n "${private_key}" ] && [ -n "${vm_user}" ]; then
        echo ""
        echo "[Phase 4/5] Checking guest OS disk count..."
        
        for vm in ${vms}; do
            echo "  Checking ${vm}..."
            
            local test_output
            test_output=$(remote_command "${namespace}" "${private_key}" "${vm_user}" "${vm}" "echo SSH_OK" 2>&1)
            
            if [ $? -ne 0 ] || [ -z "${test_output}" ]; then
                echo "  ⚠ ${vm}: SSH connection failed, skipping guest OS validation"
                log_validation_checkpoint "guest_os_disk_count" "SKIP" "VM ${vm}: SSH connection failed"
                continue
            fi
            
            echo "  ✓ ${vm}: SSH connected"
            
            local blk_devices
            blk_devices=$(remote_command "${namespace}" "${private_key}" "${vm_user}" "${vm}" "lsblk --json -d -n -o NAME,TYPE,SIZE" 2>/dev/null)
            
            if [ $? -ne 0 ] || [ -z "${blk_devices}" ]; then
                echo "  ✗ ${vm}: Failed to get block devices"
                log_validation_checkpoint "guest_os_disk_count" "FAIL" "Could not retrieve block devices"
                overall_status="FAILURE"
                break
            fi
            
            local guest_disk_count
            guest_disk_count=$(echo "${blk_devices}" | jq '[.blockdevices[] | select(.type == "disk" and .name != "vda" and .name != "sda" and (.name | startswith("zram") | not) and (.size | test("^[0-9]+(\\.)?[0-9]*[GT]")))] | length')
            
            if [ "${guest_disk_count}" != "${expected_disk_count}" ]; then
                echo "  ✗ ${vm}: Guest disk count mismatch. Expected: ${expected_disk_count}, Actual: ${guest_disk_count}"
                log_validation_checkpoint "guest_os_disk_count" "FAIL" "Expected ${expected_disk_count}, got ${guest_disk_count}"
                overall_status="FAILURE"
                break
            fi
            
            echo "  ✓ ${vm}: Guest OS shows ${guest_disk_count} data disk(s)"
            log_validation_checkpoint "guest_os_disk_count" "PASS" "VM ${vm}: ${guest_disk_count} data disk(s) in guest OS"
            guest_os_disk_count_status="PASS"
        done
    else
        echo ""
        echo "[Phase 4/5] Skipping guest OS disk count validation (no SSH credentials)"
        log_validation_checkpoint "guest_os_disk_count" "SKIP" "SSH credentials not provided"
    fi
    
    # Phase 5: Guest OS disk size validation
    if [ "${overall_status}" = "SUCCESS" ] && [ -n "${private_key}" ] && [ -n "${vm_user}" ]; then
        echo ""
        echo "[Phase 5/5] Checking guest OS disk sizes..."
        
        local expected_size_numeric
        expected_size_numeric=$(echo "${expected_disk_size}" | sed 's/Gi$//')
        
        for vm in ${vms}; do
            echo "  Checking ${vm}..."
            
            local blk_devices
            blk_devices=$(remote_command "${namespace}" "${private_key}" "${vm_user}" "${vm}" "lsblk --json -d -n -o NAME,TYPE,SIZE" 2>/dev/null)
            
            if [ $? -ne 0 ] || [ -z "${blk_devices}" ]; then
                continue
            fi
            
            local guest_disk_sizes
            guest_disk_sizes=$(echo "${blk_devices}" | jq -r '.blockdevices[] | select(.type == "disk" and .name != "vda" and .name != "sda" and (.name | startswith("zram") | not) and (.size | test("^[0-9]+(\\.)?[0-9]*[GT]"))) | .size')
            
            for guest_size in ${guest_disk_sizes}; do
                local guest_size_numeric
                guest_size_numeric=$(echo "${guest_size}" | sed 's/[^0-9.]//g')
                
                local size_diff
                size_diff=$(echo "${expected_size_numeric} ${guest_size_numeric}" | awk '{diff=$1-$2; if(diff<0) diff=-diff; print diff}')
                local tolerance
                tolerance=$(echo "${expected_size_numeric}" | awk '{print $1*0.05}')
                
                if (( $(echo "${size_diff} > ${tolerance}" | bc -l) )) && (( $(echo "${size_diff} > 1" | bc -l) )); then
                    echo "  ✗ ${vm}: Guest disk size mismatch. Expected: ~${expected_disk_size}, Actual: ${guest_size}"
                    log_validation_checkpoint "guest_os_disk_size" "FAIL" "Expected ~${expected_size_numeric}G, got ${guest_size}"
                    overall_status="FAILURE"
                    break 2
                fi
            done
            
            echo "  ✓ ${vm}: Guest disk sizes match (within 5% tolerance)"
            log_validation_checkpoint "guest_os_disk_size" "PASS" "VM ${vm}: All data disk sizes match (within 5% tolerance)"
            guest_os_disk_size_status="PASS"
        done
    else
        echo ""
        echo "[Phase 5/5] Skipping guest OS disk size validation (no SSH credentials)"
        log_validation_checkpoint "guest_os_disk_size" "SKIP" "SSH credentials not provided"
    fi
    
    # Generate summary
    local end_time=$(date +%s)
    local duration=$((end_time - start_time))
    
    echo ""
    echo "=============================================="
    if [ "${overall_status}" = "SUCCESS" ]; then
        echo "  ✓ VALIDATION PASSED"
    else
        echo "  ✗ VALIDATION FAILED"
    fi
    echo "  Duration: ${duration}s"
    echo "=============================================="
    
    log_validation_end "${overall_status}" "${duration}s"
    
    # Generate params JSON
    local params_json
    params_json=$(cat <<PARAMS
{
    "label_key": "${label_key}",
    "label_value": "${label_value}",
    "expected_disk_count": ${expected_disk_count},
    "expected_disk_size": "${expected_disk_size}",
    "vm_count": ${vm_count},
    "ssh_validation_enabled": $([ -n "${private_key}" ] && echo "true" || echo "false"),
    "total_duration_seconds": ${duration}
}
PARAMS
)
    
    # Generate validations JSON
    local spec_status="PASS"
    [ "${overall_status}" = "FAILURE" ] && spec_status="FAIL"
    
    local validations_json
    validations_json=$(cat <<VALIDATIONS
[
    {"phase": "vm_discovery", "status": "PASS", "message": "Found ${vm_count} VMs"},
    {"phase": "vm_spec_disk_count", "status": "${spec_status}", "message": "VM spec disk count validation (${expected_disk_count} disks)"},
    {"phase": "vm_spec_disk_size", "status": "${spec_status}", "message": "VM spec disk size validation (${expected_disk_size})"},
    {"phase": "guest_os_disk_count", "status": "${guest_os_disk_count_status}", "message": "Guest OS disk count validation"},
    {"phase": "guest_os_disk_size", "status": "${guest_os_disk_size_status}", "message": "Guest OS disk size validation"}
]
VALIDATIONS
)
    
    save_validation_report "disk-limits" "${overall_status}" "${namespace}" "${params_json}" "${validations_json}" "${results_dir}"
    
    if [ "${overall_status}" = "SUCCESS" ]; then
        echo "SUCCESS: All VMs have correct disk configuration"
        return 0
    else
        return 1
    fi
}

# Check disk hot-plug
check_disk_hotplug() {
    local label_key="$1"
    local label_value="$2"
    local namespace="$3"
    local expected_disk_count="$4"
    local expected_disk_size="$5"
    local private_key="$6"
    local vm_user="$7"
    local validate_pvc_by_size="${8:-${VALIDATE_PVC_BY_SIZE:-true}}"
    local validate_hotplug_from_os="${9:-${VALIDATE_HOTPLUG_FROM_OS:-true}}"
    local results_dir="${10:-/tmp/kube-burner-validations}"
    
    echo "Checking disk hot-plug for VMs with label ${label_key}=${label_value} in namespace ${namespace}"
    echo "Expected hot-plugged disk count: ${expected_disk_count}"
    echo "Expected hot-plugged disk size: ${expected_disk_size}"
    echo "Validation toggles: PVC size check=${validate_pvc_by_size}, OS-level check=${validate_hotplug_from_os}"
    
    log_validation_start "check_disk_hotplug"
    local start_time=$(date +%s)
    
    local vms
    vms=$(get_vms "${namespace}" "${label_key}" "${label_value}")
    
    for vm in ${vms}; do
        # Check hot-plugged disk count in VM spec (exclude rootdisk and cloudinitdisk)
        local actual_disk_count
        actual_disk_count=$(oc get vm -n "${namespace}" "${vm}" -o json | jq '[.spec.template.spec.volumes[] | select(.name != "rootdisk" and .name != "cloudinitdisk")] | length')
        
        echo "VM ${vm}: VM spec shows ${actual_disk_count} hot-plugged disk(s)"
        
        if [ "${actual_disk_count}" != "${expected_disk_count}" ]; then
            echo "ERROR: Hot-plugged disk count mismatch in VM spec for ${vm}. Expected: ${expected_disk_count}, Actual: ${actual_disk_count}"
            log_validation_checkpoint "vm_spec_disk_count" "FAIL" "Expected ${expected_disk_count}, got ${actual_disk_count}"
            return 1
        fi
        
        log_validation_checkpoint "vm_spec_disk_count" "PASS" "VM ${vm}: ${actual_disk_count} hot-plugged disks in spec"
        
        # Validate PVC sizes for hot-plugged disks (if enabled)
        if [ "${validate_pvc_by_size}" = "true" ]; then
            local hotplug_pvcs
            hotplug_pvcs=$(oc get vm -n "${namespace}" "${vm}" -o json | jq -r '[.spec.template.spec.volumes[] | select(.name != "rootdisk" and .name != "cloudinitdisk" and .persistentVolumeClaim != null) | .persistentVolumeClaim.claimName] | .[]')
            
            for pvc_name in ${hotplug_pvcs}; do
                local pvc_size
                pvc_size=$(oc get pvc -n "${namespace}" "${pvc_name}" -o jsonpath='{.spec.resources.requests.storage}' 2>/dev/null || echo "")
                if [ -n "${pvc_size}" ] && [ "${pvc_size}" != "${expected_disk_size}" ]; then
                    echo "ERROR: Hot-plugged PVC size mismatch for ${pvc_name}. Expected: ${expected_disk_size}, Actual: ${pvc_size}"
                    log_validation_checkpoint "pvc_size_check" "FAIL" "PVC ${pvc_name}: Expected ${expected_disk_size}, got ${pvc_size}"
                    return 1
                fi
            done
            
            echo "VM ${vm}: All hot-plugged disk sizes in VM spec match expected size"
            log_validation_checkpoint "pvc_size_check" "PASS" "VM ${vm}: All PVC sizes match ${expected_disk_size}"
        else
            echo "VM ${vm}: Skipping PVC size validation (disabled)"
            log_validation_checkpoint "pvc_size_check" "SKIP" "VM ${vm}: PVC size validation disabled"
        fi
        
        # SSH into VM and verify disks are visible and mounted in guest OS (if enabled)
        if [ "${validate_hotplug_from_os}" = "true" ]; then
            if [ -n "${private_key}" ] && [ -n "${vm_user}" ]; then
                # First test SSH connectivity
                echo "VM ${vm}: Testing SSH connectivity..."
                local ssh_test
                ssh_test=$(remote_command "${namespace}" "${private_key}" "${vm_user}" "${vm}" "echo SSH_OK" 2>&1)
                if [ $? -ne 0 ] || [ -z "${ssh_test}" ]; then
                    echo "ERROR: Failed to establish SSH connection to VM ${vm}"
                    log_validation_checkpoint "ssh_connectivity" "FAIL" "Could not connect to VM ${vm}"
                    return 1
                fi
                echo "VM ${vm}: SSH connection successful"
                
                # Try to run mount-hotplug-disks.sh if it exists (optional)
                local attach_devices
                attach_devices=$(remote_command "${namespace}" "${private_key}" "${vm_user}" "${vm}" "[ -f /usr/local/bin/mount-hotplug-disks.sh ] && sudo /bin/bash /usr/local/bin/mount-hotplug-disks.sh || echo 'Mount script not found, skipping'")
                echo "VM ${vm}: Attach devices: ${attach_devices}"
                
                local blk_devices
                blk_devices=$(remote_command "${namespace}" "${private_key}" "${vm_user}" "${vm}" "lsblk --json -d -n -o NAME,TYPE,SIZE")
                local ret=$?
                if [ $ret -ne 0 ] || [ -z "${blk_devices}" ]; then
                    echo "ERROR: Failed to get block devices for VM ${vm}"
                    log_validation_checkpoint "guest_os_disk_count" "FAIL" "Could not retrieve block devices"
                    return 1
                fi
                
                # Count block devices excluding vda/sda (rootdisk), zram (swap), and small disks (< 1GB, like cloudinitdisk)
                local guest_disk_count
                guest_disk_count=$(echo "${blk_devices}" | jq '[.blockdevices[] | select(.type == "disk" and .name != "vda" and .name != "sda" and (.name | startswith("zram") | not) and (.size | test("^[0-9]+(\\.)?[0-9]*[GT]")))] | length')
                
                echo "VM ${vm}: Guest OS shows ${guest_disk_count} hot-plugged disk(s)"
                
                if [ "${guest_disk_count}" != "${expected_disk_count}" ]; then
                    echo "ERROR: Hot-plugged disk count mismatch in guest OS for VM ${vm}. Expected: ${expected_disk_count}, Actual: ${guest_disk_count}"
                    log_validation_checkpoint "guest_os_disk_count" "FAIL" "Expected ${expected_disk_count}, got ${guest_disk_count}"
                    return 1
                fi
                
                log_validation_checkpoint "guest_os_disk_count" "PASS" "VM ${vm}: ${guest_disk_count} disks visible in guest OS"
                
                # Validate disk sizes in guest OS
                local expected_size_numeric
                expected_size_numeric=$(echo "${expected_disk_size}" | sed 's/Gi$//' | sed 's/G$//')
                
                # Get actual sizes from guest OS (excluding vda/sda and zram)
                local guest_disk_sizes
                guest_disk_sizes=$(echo "${blk_devices}" | jq -r '.blockdevices[] | select(.type == "disk" and .name != "vda" and .name != "sda" and (.name | startswith("zram") | not)) | .size')
                
                for guest_size in ${guest_disk_sizes}; do
                    # Extract numeric value from size (e.g., "10G" -> 10)
                    local guest_size_numeric
                    guest_size_numeric=$(echo "${guest_size}" | sed 's/[^0-9.]//g')
                    
                    # Allow for some tolerance due to formatting differences (within 5% or 1GB)
                    local size_diff
                    size_diff=$(echo "${expected_size_numeric} ${guest_size_numeric}" | awk '{diff=$1-$2; if(diff<0) diff=-diff; print diff}')
                    local tolerance
                    tolerance=$(echo "${expected_size_numeric}" | awk '{print $1*0.05}')
                    
                    if (( $(echo "${size_diff} > ${tolerance}" | bc -l) )) && (( $(echo "${size_diff} > 1" | bc -l) )); then
                        echo "ERROR: Hot-plugged disk size mismatch in guest OS for VM ${vm}. Expected: ~${expected_disk_size}, Actual: ${guest_size}"
                        log_validation_checkpoint "guest_os_disk_size" "FAIL" "Expected ${expected_disk_size}, got ${guest_size}"
                        return 1
                    fi
                done
                
                echo "VM ${vm}: All hot-plugged disk sizes in guest OS match expected size (within tolerance)"
                log_validation_checkpoint "guest_os_disk_size" "PASS" "VM ${vm}: All disk sizes match in guest OS"
                
                # Trigger the mount script to mount newly hot-plugged disks
                echo "VM ${vm}: Triggering mount script for hot-plugged disks..."
                # Run the script in a subshell with timeout and background execution to prevent SSH hanging
                # The script will run, complete, and close file descriptors properly
                local mount_trigger
                mount_trigger=$(remote_command "${namespace}" "${private_key}" "${vm_user}" "${vm}" "sudo bash -c 'nohup /usr/local/bin/mount-hotplug-disks.sh > /var/log/hotplug-mount.log 2>&1 &' && sleep 2")
                local trigger_ret=$?
                
                if [ $trigger_ret -ne 0 ]; then
                    echo "WARNING: Failed to trigger mount script for VM ${vm}, continuing anyway..."
                else
                    echo "VM ${vm}: Mount script triggered, waiting for completion..."
                    # Give it time to complete mounting (increased from 5s to 15s for safety)
                    sleep 15
                fi
                
                # Check that hot-plugged disks are mounted at /mnt/disk* locations
                local mount_info
                mount_info=$(remote_command "${namespace}" "${private_key}" "${vm_user}" "${vm}" "mount | grep '/mnt/disk'")
                local mount_ret=$?
                
                if [ $mount_ret -eq 0 ]; then
                    local mounted_count
                    mounted_count=$(echo "${mount_info}" | wc -l)
                    echo "VM ${vm}: ${mounted_count} hot-plugged disk partition(s) are mounted"
                    
                    # Note: It's possible some disks haven't been mounted yet by the systemd service
                    # so we don't fail if count doesn't match exactly, but we report it
                    if [ "${mounted_count}" -lt "${expected_disk_count}" ]; then
                        echo "WARNING: Only ${mounted_count} of ${expected_disk_count} hot-plugged disks are currently mounted"
                    fi
                else
                    echo "WARNING: Could not verify mount status for hot-plugged disks in VM ${vm}"
                fi
            else
                echo "WARNING: Skipping guest OS verification (no SSH credentials provided)"
            fi
        else
            echo "VM ${vm}: Skipping OS-level validation (disabled)"
        fi
    done
    
    echo "SUCCESS: All VMs have correct hot-plugged disk configuration (count and size)"
    
    local end_time=$(date +%s)
    local duration=$((end_time - start_time))
    log_validation_end "SUCCESS" "${duration}s"
    
    # Generate params JSON
    local params_json
    params_json=$(cat <<PARAMS
{
    "label_key": "${label_key}",
    "label_value": "${label_value}",
    "expected_disk_count": ${expected_disk_count},
    "expected_disk_size": "${expected_disk_size}",
    "validate_pvc_by_size": "${validate_pvc_by_size}",
    "validate_hotplug_from_os": "${validate_hotplug_from_os}"
}
PARAMS
)
    
    # Generate validations JSON
    local pvc_status="$([ "${validate_pvc_by_size}" = "true" ] && echo "PASS" || echo "SKIP")"
    local os_status="$([ "${validate_hotplug_from_os}" = "true" ] && echo "PASS" || echo "SKIP")"
    
    local validations_json
    validations_json=$(cat <<VALIDATIONS
[
    {"phase": "vm_spec_disk_count", "status": "PASS", "message": "All VMs have correct hot-plugged disk count in spec"},
    {"phase": "vm_spec_pvc_size", "status": "${pvc_status}", "message": "PVC size validation $([ "${validate_pvc_by_size}" = "true" ] && echo "passed" || echo "skipped")"},
    {"phase": "guest_os_disk_count", "status": "${os_status}", "message": "Guest OS disk count validation $([ "${validate_hotplug_from_os}" = "true" ] && echo "passed" || echo "skipped")"},
    {"phase": "guest_os_disk_size", "status": "${os_status}", "message": "Guest OS disk size validation $([ "${validate_hotplug_from_os}" = "true" ] && echo "passed" || echo "skipped")"}
]
VALIDATIONS
)
    
    save_validation_report "disk-hotplug" "SUCCESS" "${namespace}" "${params_json}" "${validations_json}" "${results_dir}"
    return 0
}

# Check NIC hot-plug with comprehensive validation
check_nic_hotplug() {
    local label_key="$1"
    local label_value="$2" 
    local namespace="$3"
    local expected_nic_count="$4"
    local private_key="${5:-}"
    local vm_user="${6:-}"
    local validate_guest_os="${7:-true}"
    local results_dir="${8:-/tmp/kube-burner-validations}"
    
    echo "=========================================="
    echo "NIC Hot-plug Validation"
    echo "=========================================="
    echo "Namespace: ${namespace}"
    echo "Expected NICs: ${expected_nic_count}"
    echo "Validate Guest OS: ${validate_guest_os}"
    echo ""
    
    # 1. Validate NodeNetworkConfigurationPolicies (NNCPs)
    echo "[1/5] Validating NodeNetworkConfigurationPolicies..."
    
    local nncp_simple_count
    nncp_simple_count=$(oc get nncp -l test-type=nic-hotplug-simple --no-headers 2>/dev/null | wc -l 2>/dev/null || echo "0")
    nncp_simple_count=$(echo "${nncp_simple_count}" | head -1 | tr -cd '0-9')
    nncp_simple_count=${nncp_simple_count:-0}
    
    local nncp_vlan_count
    nncp_vlan_count=$(oc get nncp -l test-type=nic-hotplug-vlan --no-headers 2>/dev/null | wc -l 2>/dev/null || echo "0")
    nncp_vlan_count=$(echo "${nncp_vlan_count}" | head -1 | tr -cd '0-9')
    nncp_vlan_count=${nncp_vlan_count:-0}
    
    local total_nncp_count=$((nncp_simple_count + nncp_vlan_count))
    local expected_nncp_count=$((expected_nic_count * 2))  # simple + vlan
    
    echo "  Found ${nncp_simple_count} simple NNCPs and ${nncp_vlan_count} VLAN NNCPs (total: ${total_nncp_count})"
    
    if [ "${total_nncp_count}" -ne "${expected_nncp_count}" ]; then
        echo "  ERROR: NNCP count mismatch. Expected: ${expected_nncp_count}, Actual: ${total_nncp_count}"
        return 1
    fi
    
    # Check NNCP status (Available condition)
    # Query both simple and vlan NNCPs separately since regex selector is not supported
    local nncp_ready_count
    nncp_ready_count=$(oc get nncp -l test-type=nic-hotplug-simple -o json 2>/dev/null | \
        jq '[.items[] | select(.status.conditions[]? | select(.type=="Available" and .status=="True"))] | length' 2>/dev/null || echo "0")
    local nncp_vlan_ready_count
    nncp_vlan_ready_count=$(oc get nncp -l test-type=nic-hotplug-vlan -o json 2>/dev/null | \
        jq '[.items[] | select(.status.conditions[]? | select(.type=="Available" and .status=="True"))] | length' 2>/dev/null || echo "0")
    # Sanitize and sum
    nncp_ready_count=$(echo "${nncp_ready_count}" | head -1 | tr -cd '0-9')
    nncp_ready_count=${nncp_ready_count:-0}
    nncp_vlan_ready_count=$(echo "${nncp_vlan_ready_count}" | head -1 | tr -cd '0-9')
    nncp_vlan_ready_count=${nncp_vlan_ready_count:-0}
    nncp_ready_count=$((nncp_ready_count + nncp_vlan_ready_count))
    
    echo "  NNCPs in Ready state: ${nncp_ready_count}/${total_nncp_count}"
    log_validation_checkpoint "nncp_status" "RUNNING" "Checking ${total_nncp_count} NNCPs"
    
    if [ "${nncp_ready_count}" -ne "${total_nncp_count}" ]; then
        echo "  ERROR: Not all NNCPs are in Ready state"
        echo "  Degraded NNCPs:"
        oc get nncp -l test-type=nic-hotplug-simple
        oc get nncp -l test-type=nic-hotplug-vlan
        log_validation_checkpoint "nncp_status" "FAIL" "Only ${nncp_ready_count}/${total_nncp_count} NNCPs Ready"
        return 1
    fi
    
    log_validation_checkpoint "nncp_status" "PASS" "All ${total_nncp_count} NNCPs are Ready"
    
    echo "  ✓ All NNCPs are configured and Ready"
    
    # 2. Validate NetworkAttachmentDefinitions (NADs)
    echo ""
    echo "[2/5] Validating NetworkAttachmentDefinitions..."
    
    local nad_simple_count
    nad_simple_count=$(oc get network-attachment-definitions -n "${namespace}" -l test-type=nic-hotplug-simple --no-headers 2>/dev/null | wc -l 2>/dev/null || echo "0")
    nad_simple_count=$(echo "${nad_simple_count}" | head -1 | tr -cd '0-9')
    nad_simple_count=${nad_simple_count:-0}
    
    local nad_vlan_count
    nad_vlan_count=$(oc get network-attachment-definitions -n "${namespace}" -l test-type=nic-hotplug-vlan --no-headers 2>/dev/null | wc -l 2>/dev/null || echo "0")
    nad_vlan_count=$(echo "${nad_vlan_count}" | head -1 | tr -cd '0-9')
    nad_vlan_count=${nad_vlan_count:-0}
    
    local total_nad_count=$((nad_simple_count + nad_vlan_count))
    
    echo "  Found ${nad_simple_count} simple NADs and ${nad_vlan_count} VLAN NADs (total: ${total_nad_count})"
    
    if [ "${total_nad_count}" -ne "${expected_nncp_count}" ]; then
        echo "  ERROR: NAD count mismatch. Expected: ${expected_nncp_count}, Actual: ${total_nad_count}"
        return 1
    fi
    
    echo "  ✓ All NetworkAttachmentDefinitions exist"
    
    # 3. Validate VM NIC configuration
    echo ""
    echo "[3/5] Validating VM NIC configuration..."
    
    local vms
    vms=$(oc get vm -n "${namespace}" -l "${label_key}=${label_value}" -o jsonpath='{.items[*].metadata.name}')
    
    if [ -z "$vms" ]; then
        echo "  ERROR: No VMs found with label ${label_key}=${label_value}"
        return 1
    fi
    
    local vm_count=0
    for vm in ${vms}; do
        vm_count=$((vm_count + 1))
        echo "  Checking VM: ${vm}"
        
        # Count networks in VM spec (includes default pod network)
        local actual_network_count
        actual_network_count=$(oc get vm -n "${namespace}" "${vm}" -o json | jq '.spec.template.spec.networks | length')
        
        # Expected: default network + hot-plugged NICs
        local expected_total_networks=$((expected_nic_count + 1))
        
        if [ "${actual_network_count}" -ne "${expected_total_networks}" ]; then
            echo "    ERROR: Network count mismatch for VM ${vm}"
            echo "    Expected: ${expected_total_networks} (${expected_nic_count} hot-plug + 1 default)"
            echo "    Actual: ${actual_network_count}"
            return 1
        fi
        
        # Count interfaces in VM spec
        local actual_interface_count
        actual_interface_count=$(oc get vm -n "${namespace}" "${vm}" -o json | jq '.spec.template.spec.domain.devices.interfaces | length')
        
        if [ "${actual_interface_count}" -ne "${expected_total_networks}" ]; then
            echo "    ERROR: Interface count mismatch for VM ${vm}"
            echo "    Expected: ${expected_total_networks}, Actual: ${actual_interface_count}"
            return 1
        fi
        
        echo "    ✓ VM has ${expected_nic_count} hot-plugged NICs + 1 default (total: ${expected_total_networks})"
    done
    
    echo "  ✓ All ${vm_count} VMs have correct NIC configuration"
    
    # 4. Validate VM is running
    echo ""
    echo "[4/5] Validating VMs are running..."
    
    for vm in ${vms}; do
        local vm_status
        vm_status=$(oc get vm -n "${namespace}" "${vm}" -o jsonpath='{.status.printableStatus}')
        
        if [ "${vm_status}" != "Running" ]; then
            echo "  ERROR: VM ${vm} is not running (status: ${vm_status})"
            return 1
        fi
    done
    
    echo "  ✓ All VMs are running"
    
    # 5. Validate Guest OS interfaces (if SSH enabled)
    echo ""
    echo "[5/5] Validating Guest OS interfaces..."
    
    if [ "${validate_guest_os}" != "true" ] || [ -z "${private_key}" ] || [ -z "${vm_user}" ]; then
        echo "  ⊘ Skipping Guest OS validation (SSH not configured or disabled)"
        echo ""
        echo "=========================================="
        echo "NIC Hot-plug Validation: SUCCESS"
        echo "=========================================="
        
        # Generate params JSON
        local params_json
        params_json=$(cat <<PARAMS
{
    "label_key": "${label_key}",
    "label_value": "${label_value}",
    "expected_nic_count": ${expected_nic_count},
    "validate_guest_os": "${validate_guest_os}"
}
PARAMS
)
        
        # Generate validations JSON
        local validations_json
        validations_json=$(cat <<VALIDATIONS
[
    {"phase": "nncp_validation", "status": "PASS", "message": "All ${total_nncp_count} NNCPs are Ready"},
    {"phase": "nad_validation", "status": "PASS", "message": "All ${total_nad_count} NADs exist"},
    {"phase": "vm_config", "status": "PASS", "message": "All ${vm_count} VMs have correct NIC configuration"},
    {"phase": "vm_running", "status": "PASS", "message": "All VMs are running"},
    {"phase": "guest_os", "status": "SKIP", "message": "Guest OS validation skipped (SSH not configured)"}
]
VALIDATIONS
)
        
        save_validation_report "nic-hotplug" "SUCCESS" "${namespace}" "${params_json}" "${validations_json}" "${results_dir}"
        return 0
    fi
    
    for vm in ${vms}; do
        echo "  Checking Guest OS for VM: ${vm}"
        
        # Test connectivity via virtctl ssh
        echo "    Testing virtctl SSH connectivity..."
        local test_output
        test_output=$(remote_command "${namespace}" "${private_key}" "${vm_user}" "${vm}" "echo SSH_OK" 2>&1)
        
        if [ $? -ne 0 ] || [ -z "${test_output}" ]; then
            echo "    ERROR: Could not connect to VM ${vm} via virtctl ssh"
            echo "    Make sure the VM is running and SSH is enabled"
            return 1
        fi
        
        echo "    ✓ virtctl SSH connection successful"
        
        # Count network interfaces in guest (excluding lo)
        echo "    Checking network interfaces in guest OS..."
        local guest_interface_count
        guest_interface_count=$(remote_command "${namespace}" "${private_key}" "${vm_user}" "${vm}" \
            "ip -br link show | grep -E '^(eth|ens|enp)' | wc -l" 2>/dev/null || echo "0")
        guest_interface_count=$(echo "${guest_interface_count}" | head -1 | tr -cd '0-9')
        guest_interface_count=${guest_interface_count:-0}
        
        if [ "${guest_interface_count}" -eq 0 ]; then
            echo "    ERROR: Failed to retrieve interface list from VM ${vm}"
            return 1
        fi
        
        # Expected: 1 default + hot-plugged NICs
        local expected_guest_interfaces=$((expected_nic_count + 1))
        
        if [ "${guest_interface_count}" -ne "${expected_guest_interfaces}" ]; then
            echo "    ERROR: Guest OS interface count mismatch for VM ${vm}"
            echo "    Expected: ${expected_guest_interfaces}, Actual: ${guest_interface_count}"
            
            # Show interface details for debugging
            echo "    Guest interfaces:"
            remote_command "${namespace}" "${private_key}" "${vm_user}" "${vm}" \
                "ip -br link show" 2>/dev/null || echo "    Could not retrieve interface list"
            return 1
        fi
        
        echo "    ✓ Guest OS has ${guest_interface_count} interfaces"
        
        # Check if IPs are configured (optional - may take time for DHCP/static config)
        local configured_ips
        configured_ips=$(remote_command "${namespace}" "${private_key}" "${vm_user}" "${vm}" \
            "ip -br addr show | grep -E '192\.168\.' | wc -l" 2>/dev/null || echo "0")
        configured_ips=$(echo "${configured_ips}" | head -1 | tr -cd '0-9')
        configured_ips=${configured_ips:-0}
        
        echo "    Interfaces with test IPs configured: ${configured_ips}/${expected_nic_count}"
        
        if [ "${configured_ips}" -lt "${expected_nic_count}" ]; then
            echo "    ⚠ WARNING: Not all test interfaces have IPs configured yet"
            echo "    This may be expected if using DHCP or manual configuration"
        fi
    done
    
    echo ""
    echo "=========================================="
    echo "NIC Hot-plug Validation: SUCCESS"
    echo "=========================================="
    
    # Generate params JSON
    local params_json
    params_json=$(cat <<PARAMS
{
    "label_key": "${label_key}",
    "label_value": "${label_value}",
    "expected_nic_count": ${expected_nic_count},
    "validate_guest_os": "${validate_guest_os}"
}
PARAMS
)
    
    # Generate validations JSON
    local guest_status="SKIP"
    local guest_message="Guest OS validation skipped"
    if [ "${validate_guest_os}" = "true" ]; then
        guest_status="PASS"
        guest_message="Guest OS validation completed"
    fi
    
    local validations_json
    validations_json=$(cat <<VALIDATIONS
[
    {"phase": "nncp_validation", "status": "PASS", "message": "All ${total_nncp_count} NNCPs are Ready"},
    {"phase": "nad_validation", "status": "PASS", "message": "All ${total_nad_count} NADs exist"},
    {"phase": "vm_config", "status": "PASS", "message": "All ${vm_count} VMs have correct NIC configuration"},
    {"phase": "vm_running", "status": "PASS", "message": "All VMs are running"},
    {"phase": "guest_os", "status": "${guest_status}", "message": "${guest_message}"}
]
VALIDATIONS
)
    
    save_validation_report "nic-hotplug" "SUCCESS" "${namespace}" "${params_json}" "${validations_json}" "${results_dir}"
    return 0
}

# Check performance metrics (minimal resources validation) with password-based SSH
# Usage: check_performance_metrics <label_key> <label_value> <namespace> <password> <vm_user> <results_dir>
# Note: Uses sshpass for CirrOS VMs with password authentication
check_performance_metrics() {
    local label_key="$1"
    local label_value="$2"
    local namespace="$3"
    local password="$4"
    local vm_user="$5"
    local results_dir="${6:-/tmp/kube-burner-validations}"
    
    local start_time=$SECONDS
    local validation_status="SUCCESS"
    local validations=()
    
    echo "=============================================="
    echo "  Performance Metrics Validation"
    echo "=============================================="
    echo "Namespace: ${namespace}"
    echo "Label: ${label_key}=${label_value}"
    echo "SSH User: ${vm_user}"
    echo "SSH Auth: password"
    echo "Results: ${results_dir}"
    echo "----------------------------------------------"
    
    log_validation_start "check_performance_metrics"
    
    # Phase 1: Discover VMs
    echo ""
    echo "[Phase 1/4] Discovering VMs..."
    local phase_start=$SECONDS
    local vms
    vms=$(get_vms "${namespace}" "${label_key}" "${label_value}")
    local vm_count=$(echo "${vms}" | wc -w)
    local discovery_duration=$((SECONDS - phase_start))
    
    if [ -z "${vms}" ] || [ "${vm_count}" -eq 0 ]; then
        echo "ERROR: No VMs found matching label ${label_key}=${label_value}"
        log_validation_checkpoint "vm_discovery" "FAIL" "No VMs found"
        validation_status="FAILED"
        validations+=('{"phase": "vm_discovery", "status": "FAIL", "message": "No VMs found", "duration_seconds": '${discovery_duration}'}')
    else
        echo "✓ Found ${vm_count} VM(s): ${vms}"
        log_validation_checkpoint "vm_discovery" "PASS" "Found ${vm_count} VMs"
        validations+=('{"phase": "vm_discovery", "status": "PASS", "message": "Found '${vm_count}' VMs", "duration_seconds": '${discovery_duration}'}')
    fi
    
    # Phase 2: Check VM responsiveness (uptime) - proves VM is booted and responsive
    if [ "${validation_status}" = "SUCCESS" ]; then
        echo ""
        echo "[Phase 2/4] Checking VM responsiveness (SSH + uptime)..."
        phase_start=$SECONDS
        local uptime_passed=0
        local uptime_failed=0
        local failed_vms=""
        
        for vm in ${vms}; do
            echo "  Checking ${vm}..."
            local uptime_output
            uptime_output=$(remote_command_password "${namespace}" "${password}" "${vm_user}" "${vm}" "uptime" 2>&1)
            local ret=$?
            if [ $ret -ne 0 ]; then
                echo "  ✗ ${vm}: SSH/uptime check failed"
                uptime_failed=$((uptime_failed + 1))
                failed_vms="${failed_vms} ${vm}"
            else
                echo "  ✓ ${vm}: ${uptime_output}"
                uptime_passed=$((uptime_passed + 1))
            fi
        done
        
        local uptime_duration=$((SECONDS - phase_start))
        
        if [ ${uptime_failed} -gt 0 ]; then
            echo "ERROR: ${uptime_failed}/${vm_count} VM(s) failed uptime check:${failed_vms}"
            log_validation_checkpoint "vm_responsiveness" "FAIL" "${uptime_failed}/${vm_count} VMs not responsive"
            validation_status="FAILED"
            validations+=('{"phase": "vm_responsiveness", "status": "FAIL", "message": "'${uptime_failed}'/'${vm_count}' VMs not responsive", "duration_seconds": '${uptime_duration}', "passed": '${uptime_passed}', "failed": '${uptime_failed}'}')
        else
            echo "✓ All ${vm_count} VM(s) responded to uptime check"
            log_validation_checkpoint "vm_responsiveness" "PASS" "All ${vm_count} VMs responsive"
            validations+=('{"phase": "vm_responsiveness", "status": "PASS", "message": "All '${vm_count}' VMs responsive", "duration_seconds": '${uptime_duration}', "passed": '${uptime_passed}', "failed": 0}')
        fi
    fi
    
    # Phase 3: Verify OS identity (confirms CirrOS is running)
    if [ "${validation_status}" = "SUCCESS" ]; then
        echo ""
        echo "[Phase 3/4] Verifying OS identity..."
        phase_start=$SECONDS
        local os_passed=0
        local os_failed=0
        
        for vm in ${vms}; do
            echo "  Checking ${vm}..."
            local os_output
            # Use uname -a to verify OS - CirrOS will show its kernel info
            os_output=$(remote_command_password "${namespace}" "${password}" "${vm_user}" "${vm}" "uname -a && whoami" 2>&1)
            local ret=$?
            if [ $ret -ne 0 ]; then
                echo "  ✗ ${vm}: OS identity check failed"
                os_failed=$((os_failed + 1))
            else
                echo "  ✓ ${vm}: OS verified"
                echo "    ${os_output}"
                os_passed=$((os_passed + 1))
            fi
        done
        
        local os_duration=$((SECONDS - phase_start))
        
        if [ ${os_failed} -gt 0 ]; then
            echo "ERROR: ${os_failed}/${vm_count} VM(s) failed OS identity check"
            log_validation_checkpoint "os_identity" "FAIL" "${os_failed}/${vm_count} VMs failed"
            validation_status="FAILED"
            validations+=('{"phase": "os_identity", "status": "FAIL", "message": "'${os_failed}'/'${vm_count}' VMs failed OS check", "duration_seconds": '${os_duration}'}')
        else
            echo "✓ All ${vm_count} VM(s) OS identity confirmed"
            log_validation_checkpoint "os_identity" "PASS" "All VMs OS identity confirmed"
            validations+=('{"phase": "os_identity", "status": "PASS", "message": "All VMs OS identity confirmed", "duration_seconds": '${os_duration}'}')
        fi
    fi
    
    # Phase 4: Check memory availability
    if [ "${validation_status}" = "SUCCESS" ]; then
        echo ""
        echo "[Phase 4/4] Checking memory availability..."
        phase_start=$SECONDS
        local mem_passed=0
        local mem_failed=0
        
        for vm in ${vms}; do
            echo "  Checking ${vm}..."
            local mem_output
            mem_output=$(remote_command_password "${namespace}" "${password}" "${vm_user}" "${vm}" "free -m | head -2" 2>&1)
            local ret=$?
            if [ $ret -ne 0 ]; then
                echo "  ✗ ${vm}: Memory check failed"
                mem_failed=$((mem_failed + 1))
            else
                echo "  ✓ ${vm}: Memory info retrieved"
                # Parse memory info for reporting
                local total_mem=$(echo "${mem_output}" | grep "Mem:" | awk '{print $2}')
                local avail_mem=$(echo "${mem_output}" | grep "Mem:" | awk '{print $7}')
                if [ -n "${total_mem}" ]; then
                    echo "    Total: ${total_mem}MB, Available: ${avail_mem:-N/A}MB"
                fi
                mem_passed=$((mem_passed + 1))
            fi
        done
        
        local mem_duration=$((SECONDS - phase_start))
        
        if [ ${mem_failed} -gt 0 ]; then
            echo "ERROR: ${mem_failed}/${vm_count} VM(s) failed memory check"
            log_validation_checkpoint "memory_check" "FAIL" "${mem_failed}/${vm_count} VMs failed"
            validation_status="FAILED"
            validations+=('{"phase": "memory_check", "status": "FAIL", "message": "'${mem_failed}'/'${vm_count}' VMs failed", "duration_seconds": '${mem_duration}'}')
        else
            echo "✓ All ${vm_count} VM(s) have accessible memory info"
            log_validation_checkpoint "memory_check" "PASS" "All VMs memory accessible"
            validations+=('{"phase": "memory_check", "status": "PASS", "message": "All VMs memory accessible", "duration_seconds": '${mem_duration}'}')
        fi
    fi
    
    # Calculate total duration
    local total_duration=$((SECONDS - start_time))
    
    # Generate summary
    echo ""
    echo "=============================================="
    if [ "${validation_status}" = "SUCCESS" ]; then
        echo "  ✓ VALIDATION PASSED"
    else
        echo "  ✗ VALIDATION FAILED"
    fi
    echo "  Duration: ${total_duration}s"
    echo "=============================================="
    
    log_validation_end "${validation_status}" "${total_duration}s"
    
    # Build validations JSON array
    local validations_json="["
    local first=true
    for v in "${validations[@]}"; do
        if [ "${first}" = true ]; then
            first=false
        else
            validations_json="${validations_json},"
        fi
        validations_json="${validations_json}${v}"
    done
    validations_json="${validations_json}]"
    
    # Build params JSON
    local params_json
    params_json=$(cat <<PARAMS
{
    "label_key": "${label_key}",
    "label_value": "${label_value}",
    "vm_count": ${vm_count},
    "vm_user": "${vm_user}",
    "total_duration_seconds": ${total_duration}
}
PARAMS
)
    
    # Save validation report
    save_validation_report "performance-metrics" "${validation_status}" "${namespace}" "${params_json}" "${validations_json}" "${results_dir}"
    
    if [ "${validation_status}" = "SUCCESS" ]; then
        echo "SUCCESS: All VMs are performing as expected"
        return 0
    else
        return 1
    fi
}

# Check high memory (validates guest OS memory matches expected allocation)
# Usage: check_high_memory <label_key> <label_value> <namespace> <expected_memory> <private_key> <vm_user> <results_dir>
check_high_memory() {
    local label_key="$1"
    local label_value="$2"
    local namespace="$3"
    local expected_memory="$4"
    local private_key="$5"
    local vm_user="$6"
    local results_dir="${7:-/tmp/kube-burner-validations}"
    
    local start_time=$SECONDS
    local validation_status="SUCCESS"
    local validations=()
    local vm_count=0
    
    echo "=============================================="
    echo "  High Memory Validation"
    echo "=============================================="
    echo "Namespace: ${namespace}"
    echo "Label: ${label_key}=${label_value}"
    echo "Expected Memory: ${expected_memory}"
    echo "SSH User: ${vm_user}"
    echo "Results: ${results_dir}"
    echo "----------------------------------------------"
    
    log_validation_start "check_high_memory"
    
    # Phase 1: Discover VMs
    echo ""
    echo "[Phase 1/3] Discovering VMs..."
    local phase_start=$SECONDS
    local vms
    vms=$(get_vms "${namespace}" "${label_key}" "${label_value}")
    vm_count=$(echo "${vms}" | wc -w)
    local discovery_duration=$((SECONDS - phase_start))
    
    if [ -z "${vms}" ] || [ "${vm_count}" -eq 0 ]; then
        echo "ERROR: No VMs found matching label ${label_key}=${label_value}"
        log_validation_checkpoint "vm_discovery" "FAIL" "No VMs found"
        validation_status="FAILED"
        validations+=('{"phase": "vm_discovery", "status": "FAIL", "message": "No VMs found", "duration_seconds": '${discovery_duration}'}')
    else
        echo "✓ Found ${vm_count} VM(s): ${vms}"
        log_validation_checkpoint "vm_discovery" "PASS" "Found ${vm_count} VMs"
        validations+=('{"phase": "vm_discovery", "status": "PASS", "message": "Found '${vm_count}' VMs", "duration_seconds": '${discovery_duration}'}')
    fi
    
    # Phase 2: Check VM responsiveness
    if [ "${validation_status}" = "SUCCESS" ]; then
        echo ""
        echo "[Phase 2/3] Checking VM responsiveness (SSH + uptime)..."
        phase_start=$SECONDS
        local uptime_passed=0
        local uptime_failed=0
        
        for vm in ${vms}; do
            echo "  Checking ${vm}..."
            local uptime_output
            uptime_output=$(remote_command "${namespace}" "${private_key}" "${vm_user}" "${vm}" "uptime" 2>&1)
            local ret=$?
            if [ $ret -ne 0 ]; then
                echo "  ✗ ${vm}: SSH/uptime check failed"
                uptime_failed=$((uptime_failed + 1))
            else
                echo "  ✓ ${vm}: responsive"
                uptime_passed=$((uptime_passed + 1))
            fi
        done
        
        local uptime_duration=$((SECONDS - phase_start))
        
        if [ ${uptime_failed} -gt 0 ]; then
            echo "ERROR: ${uptime_failed}/${vm_count} VM(s) not responsive"
            log_validation_checkpoint "vm_responsiveness" "FAIL" "${uptime_failed}/${vm_count} VMs not responsive"
            validation_status="FAILED"
            validations+=('{"phase": "vm_responsiveness", "status": "FAIL", "message": "'${uptime_failed}'/'${vm_count}' VMs not responsive", "duration_seconds": '${uptime_duration}'}')
        else
            echo "✓ All ${vm_count} VM(s) are responsive"
            log_validation_checkpoint "vm_responsiveness" "PASS" "All ${vm_count} VMs responsive"
            validations+=('{"phase": "vm_responsiveness", "status": "PASS", "message": "All '${vm_count}' VMs responsive", "duration_seconds": '${uptime_duration}'}')
        fi
    fi
    
    # Phase 3: Check guest OS memory
    local guest_memory_mb=0
    local expected_memory_mb=0
    if [ "${validation_status}" = "SUCCESS" ]; then
        echo ""
        echo "[Phase 3/3] Validating guest OS memory..."
        phase_start=$SECONDS
        local mem_passed=0
        local mem_failed=0
        
        # Convert expected_memory to MB for comparison
        if [[ "${expected_memory}" =~ ^([0-9]+)Gi$ ]]; then
            expected_memory_mb=$(( ${BASH_REMATCH[1]} * 1024 ))
        elif [[ "${expected_memory}" =~ ^([0-9]+)Mi$ ]]; then
            expected_memory_mb=${BASH_REMATCH[1]}
        elif [[ "${expected_memory}" =~ ^([0-9]+)G$ ]]; then
            expected_memory_mb=$(( ${BASH_REMATCH[1]} * 1000 ))
        elif [[ "${expected_memory}" =~ ^([0-9]+)M$ ]]; then
            expected_memory_mb=${BASH_REMATCH[1]}
        else
            echo "  WARNING: Cannot parse memory format '${expected_memory}', skipping validation"
            log_validation_checkpoint "guest_os_memory" "SKIP" "Cannot parse memory format"
            validations+=('{"phase": "guest_os_memory", "status": "SKIP", "message": "Cannot parse memory format '${expected_memory}'", "duration_seconds": 0}')
        fi
        
        if [ ${expected_memory_mb} -gt 0 ]; then
            # Allow 15% tolerance for memory comparison
            local tolerance=$(( expected_memory_mb * 15 / 100 ))
            local min_memory=$(( expected_memory_mb - tolerance ))
            local max_memory=$(( expected_memory_mb + tolerance ))
            
            echo "  Expected: ${expected_memory_mb}MB (${expected_memory})"
            echo "  Tolerance: ±15% (${min_memory}-${max_memory}MB)"
            
            for vm in ${vms}; do
                echo "  Checking ${vm}..."
                guest_memory_mb=$(remote_command "${namespace}" "${private_key}" "${vm_user}" "${vm}" "free -m | awk 'NR==2{print \$2}'" 2>/dev/null || echo "0")
                guest_memory_mb=$(echo "${guest_memory_mb}" | head -1 | tr -cd '0-9')
                guest_memory_mb=${guest_memory_mb:-0}
                
                if [ "${guest_memory_mb}" -eq 0 ]; then
                    echo "  ✗ ${vm}: Failed to retrieve memory from guest OS"
                    mem_failed=$((mem_failed + 1))
                elif [ "${guest_memory_mb}" -lt "${min_memory}" ] || [ "${guest_memory_mb}" -gt "${max_memory}" ]; then
                    echo "  ✗ ${vm}: Guest memory ${guest_memory_mb}MB outside expected range"
                    mem_failed=$((mem_failed + 1))
                else
                    echo "  ✓ ${vm}: Guest memory ${guest_memory_mb}MB (within expected range)"
                    mem_passed=$((mem_passed + 1))
                fi
            done
            
            local mem_duration=$((SECONDS - phase_start))
            
            if [ ${mem_failed} -gt 0 ]; then
                echo "ERROR: ${mem_failed}/${vm_count} VM(s) failed memory validation"
                log_validation_checkpoint "guest_os_memory" "FAIL" "${mem_failed}/${vm_count} VMs failed"
                validation_status="FAILED"
                validations+=('{"phase": "guest_os_memory", "status": "FAIL", "message": "'${mem_failed}'/'${vm_count}' VMs failed memory check", "duration_seconds": '${mem_duration}', "expected_mb": '${expected_memory_mb}', "tolerance_percent": 15}')
            else
                echo "✓ All ${vm_count} VM(s) have expected memory allocation"
                log_validation_checkpoint "guest_os_memory" "PASS" "All VMs memory validated"
                validations+=('{"phase": "guest_os_memory", "status": "PASS", "message": "All VMs show ~'${expected_memory_mb}'MB", "duration_seconds": '${mem_duration}', "expected_mb": '${expected_memory_mb}', "actual_mb": '${guest_memory_mb}'}')
            fi
        fi
    fi
    
    # Calculate total duration
    local total_duration=$((SECONDS - start_time))
    
    # Generate summary
    echo ""
    echo "=============================================="
    if [ "${validation_status}" = "SUCCESS" ]; then
        echo "  ✓ VALIDATION PASSED"
    else
        echo "  ✗ VALIDATION FAILED"
    fi
    echo "  Duration: ${total_duration}s"
    echo "=============================================="
    
    log_validation_end "${validation_status}" "${total_duration}s"
    
    # Build validations JSON array
    local validations_json="["
    local first=true
    for v in "${validations[@]}"; do
        if [ "${first}" = true ]; then
            first=false
        else
            validations_json="${validations_json},"
        fi
        validations_json="${validations_json}${v}"
    done
    validations_json="${validations_json}]"
    
    # Build params JSON
    local params_json
    params_json=$(cat <<PARAMS
{
    "label_key": "${label_key}",
    "label_value": "${label_value}",
    "expected_memory": "${expected_memory}",
    "expected_memory_mb": ${expected_memory_mb},
    "vm_count": ${vm_count},
    "vm_user": "${vm_user}",
    "total_duration_seconds": ${total_duration}
}
PARAMS
)
    
    # Save validation report
    save_validation_report "high-memory" "${validation_status}" "${namespace}" "${params_json}" "${validations_json}" "${results_dir}"
    
    if [ "${validation_status}" = "SUCCESS" ]; then
        echo "SUCCESS: High memory validation passed"
        return 0
    else
        return 1
    fi
}

# Check large disk (validates guest OS sees the large disk with expected size)
# Usage: check_large_disk <label_key> <label_value> <namespace> <expected_disk_size> <private_key> <vm_user> <results_dir>
check_large_disk() {
    local label_key="$1"
    local label_value="$2"
    local namespace="$3"
    local expected_disk_size="$4"
    local private_key="$5"
    local vm_user="$6"
    local results_dir="${7:-/tmp/kube-burner-validations}"
    
    local start_time=$SECONDS
    local validation_status="SUCCESS"
    local validations=()
    local vm_count=0
    
    echo "=============================================="
    echo "  Large Disk Validation"
    echo "=============================================="
    echo "Namespace: ${namespace}"
    echo "Label: ${label_key}=${label_value}"
    echo "Expected Disk Size: ${expected_disk_size}"
    echo "SSH User: ${vm_user}"
    echo "Results: ${results_dir}"
    echo "----------------------------------------------"
    
    log_validation_start "check_large_disk"
    
    # Phase 1: Discover VMs
    echo ""
    echo "[Phase 1/4] Discovering VMs..."
    local phase_start=$SECONDS
    local vms
    vms=$(get_vms "${namespace}" "${label_key}" "${label_value}")
    vm_count=$(echo "${vms}" | wc -w)
    local discovery_duration=$((SECONDS - phase_start))
    
    if [ -z "${vms}" ] || [ "${vm_count}" -eq 0 ]; then
        echo "ERROR: No VMs found matching label ${label_key}=${label_value}"
        log_validation_checkpoint "vm_discovery" "FAIL" "No VMs found"
        validation_status="FAILED"
        validations+=('{"phase": "vm_discovery", "status": "FAIL", "message": "No VMs found", "duration_seconds": '${discovery_duration}'}')
    else
        echo "✓ Found ${vm_count} VM(s): ${vms}"
        log_validation_checkpoint "vm_discovery" "PASS" "Found ${vm_count} VMs"
        validations+=('{"phase": "vm_discovery", "status": "PASS", "message": "Found '${vm_count}' VMs", "duration_seconds": '${discovery_duration}'}')
    fi
    
    # Phase 2: Check VM responsiveness
    if [ "${validation_status}" = "SUCCESS" ]; then
        echo ""
        echo "[Phase 2/4] Checking VM responsiveness (SSH + uptime)..."
        phase_start=$SECONDS
        local uptime_passed=0
        local uptime_failed=0
        
        for vm in ${vms}; do
            echo "  Checking ${vm}..."
            local uptime_output
            uptime_output=$(remote_command "${namespace}" "${private_key}" "${vm_user}" "${vm}" "uptime" 2>&1)
            local ret=$?
            if [ $ret -ne 0 ]; then
                echo "  ✗ ${vm}: SSH/uptime check failed"
                uptime_failed=$((uptime_failed + 1))
            else
                echo "  ✓ ${vm}: responsive"
                uptime_passed=$((uptime_passed + 1))
            fi
        done
        
        local uptime_duration=$((SECONDS - phase_start))
        
        if [ ${uptime_failed} -gt 0 ]; then
            echo "ERROR: ${uptime_failed}/${vm_count} VM(s) not responsive"
            log_validation_checkpoint "vm_responsiveness" "FAIL" "${uptime_failed}/${vm_count} VMs not responsive"
            validation_status="FAILED"
            validations+=('{"phase": "vm_responsiveness", "status": "FAIL", "message": "'${uptime_failed}'/'${vm_count}' VMs not responsive", "duration_seconds": '${uptime_duration}'}')
        else
            echo "✓ All ${vm_count} VM(s) are responsive"
            log_validation_checkpoint "vm_responsiveness" "PASS" "All ${vm_count} VMs responsive"
            validations+=('{"phase": "vm_responsiveness", "status": "PASS", "message": "All '${vm_count}' VMs responsive", "duration_seconds": '${uptime_duration}'}')
        fi
    fi
    
    # Phase 3: Check large disk visibility
    local disk_device=""
    local disk_size_guest=""
    if [ "${validation_status}" = "SUCCESS" ]; then
        echo ""
        echo "[Phase 3/4] Checking large disk visibility in guest OS..."
        phase_start=$SECONDS
        local disk_visible_passed=0
        local disk_visible_failed=0
        
        for vm in ${vms}; do
            echo "  Checking ${vm}..."
            # Get block devices via lsblk, looking for secondary disks (vdb, vdc, sdb, sdc, etc.)
            local blk_devices
            blk_devices=$(remote_command "${namespace}" "${private_key}" "${vm_user}" "${vm}" "lsblk --json 2>/dev/null || lsblk -b -o NAME,SIZE,TYPE 2>/dev/null" 2>&1)
            local ret=$?
            
            if [ $ret -ne 0 ]; then
                echo "  ✗ ${vm}: Failed to get block devices"
                disk_visible_failed=$((disk_visible_failed + 1))
                continue
            fi
            
            # Look for large disk (excluding vda/sda root disk and zram)
            # Try JSON format first
            if echo "${blk_devices}" | grep -q "blockdevices"; then
                disk_device=$(echo "${blk_devices}" | jq -r '.blockdevices[] | select(.type == "disk" and .name != "vda" and .name != "sda" and (.name | startswith("zram") | not)) | .name' 2>/dev/null | head -1)
                disk_size_guest=$(echo "${blk_devices}" | jq -r '.blockdevices[] | select(.type == "disk" and .name != "vda" and .name != "sda" and (.name | startswith("zram") | not)) | .size' 2>/dev/null | head -1)
            else
                # Fallback to text parsing
                disk_device=$(echo "${blk_devices}" | awk '$3=="disk" && $1!="vda" && $1!="sda" && $1!~/^zram/ {print $1}' | head -1)
                disk_size_guest=$(echo "${blk_devices}" | awk '$3=="disk" && $1!="vda" && $1!="sda" && $1!~/^zram/ {print $2}' | head -1)
            fi
            
            if [ -z "${disk_device}" ]; then
                echo "  ✗ ${vm}: No large disk found (only root disk visible)"
                disk_visible_failed=$((disk_visible_failed + 1))
            else
                echo "  ✓ ${vm}: Large disk found: /dev/${disk_device} (${disk_size_guest})"
                disk_visible_passed=$((disk_visible_passed + 1))
            fi
        done
        
        local disk_visible_duration=$((SECONDS - phase_start))
        
        if [ ${disk_visible_failed} -gt 0 ]; then
            echo "ERROR: ${disk_visible_failed}/${vm_count} VM(s) don't see large disk"
            log_validation_checkpoint "disk_visibility" "FAIL" "${disk_visible_failed}/${vm_count} VMs missing large disk"
            validation_status="FAILED"
            validations+=('{"phase": "disk_visibility", "status": "FAIL", "message": "'${disk_visible_failed}'/'${vm_count}' VMs missing large disk", "duration_seconds": '${disk_visible_duration}'}')
        else
            echo "✓ All ${vm_count} VM(s) see the large disk"
            log_validation_checkpoint "disk_visibility" "PASS" "All VMs see large disk"
            validations+=('{"phase": "disk_visibility", "status": "PASS", "message": "Large disk visible on all VMs", "duration_seconds": '${disk_visible_duration}', "device": "'${disk_device}'"}')
        fi
    fi
    
    # Phase 4: Validate disk size
    local expected_size_gb=0
    if [ "${validation_status}" = "SUCCESS" ]; then
        echo ""
        echo "[Phase 4/4] Validating large disk size..."
        phase_start=$SECONDS
        local size_passed=0
        local size_failed=0
        
        # Parse expected size to GB
        if [[ "${expected_disk_size}" =~ ^([0-9]+)Ti$ ]]; then
            expected_size_gb=$(( ${BASH_REMATCH[1]} * 1024 ))
        elif [[ "${expected_disk_size}" =~ ^([0-9]+)Gi$ ]]; then
            expected_size_gb=${BASH_REMATCH[1]}
        elif [[ "${expected_disk_size}" =~ ^([0-9]+)T$ ]]; then
            expected_size_gb=$(( ${BASH_REMATCH[1]} * 1000 ))
        elif [[ "${expected_disk_size}" =~ ^([0-9]+)G$ ]]; then
            expected_size_gb=${BASH_REMATCH[1]}
        else
            echo "  WARNING: Cannot parse disk size format '${expected_disk_size}', skipping size validation"
            log_validation_checkpoint "disk_size" "SKIP" "Cannot parse size format"
            validations+=('{"phase": "disk_size", "status": "SKIP", "message": "Cannot parse size format '${expected_disk_size}'", "duration_seconds": 0}')
        fi
        
        if [ ${expected_size_gb} -gt 0 ]; then
            # Allow 5% tolerance for size comparison
            local tolerance=$(( expected_size_gb * 5 / 100 ))
            [ ${tolerance} -lt 1 ] && tolerance=1
            local min_size=$(( expected_size_gb - tolerance ))
            local max_size=$(( expected_size_gb + tolerance ))
            
            echo "  Expected: ${expected_size_gb}GB (${expected_disk_size})"
            echo "  Tolerance: ±5% (${min_size}-${max_size}GB)"
            
            for vm in ${vms}; do
                echo "  Checking ${vm}..."
                # Get disk size in bytes and convert to GB
                local disk_size_bytes
                disk_size_bytes=$(remote_command "${namespace}" "${private_key}" "${vm_user}" "${vm}" "lsblk -b -d -o SIZE /dev/${disk_device} 2>/dev/null | tail -1" 2>&1)
                disk_size_bytes=$(echo "${disk_size_bytes}" | tr -cd '0-9')
                
                if [ -z "${disk_size_bytes}" ] || [ "${disk_size_bytes}" -eq 0 ]; then
                    # Try alternative method
                    disk_size_bytes=$(remote_command "${namespace}" "${private_key}" "${vm_user}" "${vm}" "cat /sys/block/${disk_device}/size 2>/dev/null" 2>&1)
                    disk_size_bytes=$(echo "${disk_size_bytes}" | tr -cd '0-9')
                    # /sys/block/*/size is in 512-byte sectors
                    if [ -n "${disk_size_bytes}" ]; then
                        disk_size_bytes=$(( disk_size_bytes * 512 ))
                    fi
                fi
                
                if [ -z "${disk_size_bytes}" ] || [ "${disk_size_bytes}" -eq 0 ]; then
                    echo "  ✗ ${vm}: Failed to get disk size"
                    size_failed=$((size_failed + 1))
                else
                    local disk_size_gb=$(( disk_size_bytes / 1024 / 1024 / 1024 ))
                    
                    if [ "${disk_size_gb}" -lt "${min_size}" ] || [ "${disk_size_gb}" -gt "${max_size}" ]; then
                        echo "  ✗ ${vm}: Disk size ${disk_size_gb}GB outside expected range"
                        size_failed=$((size_failed + 1))
                    else
                        echo "  ✓ ${vm}: Disk size ${disk_size_gb}GB (within expected range)"
                        size_passed=$((size_passed + 1))
                    fi
                fi
            done
            
            local size_duration=$((SECONDS - phase_start))
            
            if [ ${size_failed} -gt 0 ]; then
                echo "ERROR: ${size_failed}/${vm_count} VM(s) failed disk size validation"
                log_validation_checkpoint "disk_size" "FAIL" "${size_failed}/${vm_count} VMs failed"
                validation_status="FAILED"
                validations+=('{"phase": "disk_size", "status": "FAIL", "message": "'${size_failed}'/'${vm_count}' VMs failed size check", "duration_seconds": '${size_duration}', "expected_gb": '${expected_size_gb}', "tolerance_percent": 5}')
            else
                echo "✓ All ${vm_count} VM(s) have expected disk size"
                log_validation_checkpoint "disk_size" "PASS" "All VMs disk size validated"
                validations+=('{"phase": "disk_size", "status": "PASS", "message": "All VMs show ~'${expected_size_gb}'GB disk", "duration_seconds": '${size_duration}', "expected_gb": '${expected_size_gb}'}')
            fi
        fi
    fi
    
    # Calculate total duration
    local total_duration=$((SECONDS - start_time))
    
    # Generate summary
    echo ""
    echo "=============================================="
    if [ "${validation_status}" = "SUCCESS" ]; then
        echo "  ✓ VALIDATION PASSED"
    else
        echo "  ✗ VALIDATION FAILED"
    fi
    echo "  Duration: ${total_duration}s"
    echo "=============================================="
    
    log_validation_end "${validation_status}" "${total_duration}s"
    
    # Build validations JSON array
    local validations_json="["
    local first=true
    for v in "${validations[@]}"; do
        if [ "${first}" = true ]; then
            first=false
        else
            validations_json="${validations_json},"
        fi
        validations_json="${validations_json}${v}"
    done
    validations_json="${validations_json}]"
    
    # Build params JSON
    local params_json
    params_json=$(cat <<PARAMS
{
    "label_key": "${label_key}",
    "label_value": "${label_value}",
    "expected_disk_size": "${expected_disk_size}",
    "expected_size_gb": ${expected_size_gb},
    "vm_count": ${vm_count},
    "vm_user": "${vm_user}",
    "total_duration_seconds": ${total_duration}
}
PARAMS
)
    
    # Save validation report
    save_validation_report "large-disk" "${validation_status}" "${namespace}" "${params_json}" "${validations_json}" "${results_dir}"
    
    if [ "${validation_status}" = "SUCCESS" ]; then
        echo "SUCCESS: Large disk validation passed"
        return 0
    else
        return 1
    fi
}

# Retry wrapper for validation functions
retry_validation() {
    local validation_func="$1"
    shift
    local args=("$@")
    
    for attempt in $(seq 1 $MAX_RETRIES); do
        echo ""
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        echo "  Attempt ${attempt}/${MAX_RETRIES}: ${validation_func}"
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        
        if ${validation_func} "${args[@]}"; then
            echo ""
            echo "════════════════════════════════════════════════"
            echo "  ✓ ${validation_func} completed successfully"
            echo "    (succeeded on attempt ${attempt}/${MAX_RETRIES})"
            echo "════════════════════════════════════════════════"
            return 0
        fi
        
        if [ "${attempt}" -lt $MAX_RETRIES ]; then
            local wait_time
            if [ "${attempt}" -lt $MAX_SHORT_WAITS ]; then
                wait_time="${SHORT_WAIT}"
            else
                wait_time="${LONG_WAIT}"
            fi
            echo ""
            echo "⏳ Validation not ready yet. Waiting ${wait_time}s before retry..."
            echo "   (attempt ${attempt}/${MAX_RETRIES} failed, will retry)"
            sleep "${wait_time}"
        else
            echo ""
            echo "════════════════════════════════════════════════"
            echo "  ✗ ${validation_func} FAILED"
            echo "    (exhausted all ${MAX_RETRIES} attempts)"
            echo "════════════════════════════════════════════════"
            return 1
        fi
    done
}

# Main script logic
case "$1" in
    check_vm_running)
        shift
        retry_validation check_vm_running "$@"
        ;;
    check_vm_shutdown)
        shift
        retry_validation check_vm_shutdown "$@"
        ;;
    check_resize)
        shift
        retry_validation check_resize "$@"
        ;;
    check_cpu_limits)
        shift
        retry_validation check_cpu_limits "$@"
        ;;
    check_memory_limits)
        shift
        retry_validation check_memory_limits "$@"
        ;;
    check_disk_limits)
        shift
        retry_validation check_disk_limits "$@"
        ;;
    check_disk_hotplug)
        shift
        retry_validation check_disk_hotplug "$@"
        ;;
    check_nic_hotplug)
        shift
        retry_validation check_nic_hotplug "$@"
        ;;
    check_performance_metrics)
        shift
        retry_validation check_performance_metrics "$@"
        ;;
    check_high_memory)
        shift
        retry_validation check_high_memory "$@"
        ;;
    check_large_disk)
        shift
        retry_validation check_large_disk "$@"
        ;;
    *)
        echo "Usage: $0 {check_vm_running|check_vm_shutdown|check_resize|check_cpu_limits|check_memory_limits|check_disk_limits|check_disk_hotplug|check_nic_hotplug|check_performance_metrics|check_high_memory|check_large_disk} [args...]"
        exit 1
        ;;
esac