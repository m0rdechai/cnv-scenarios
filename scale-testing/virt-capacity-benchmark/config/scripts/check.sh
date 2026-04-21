#!/bin/bash
#
# Virt-Capacity-Benchmark Validation Script
# Provides structured logging and JSON reports for VM validation phases
#

set -eo pipefail

# Global variables for logging
VALIDATION_LOG=""

#############################################
# LOGGING HELPER FUNCTIONS
#############################################

function log_validation_start() {
    local function_name="$1"
    local timestamp=$(date -Iseconds)
    echo "[${timestamp}] ======== VALIDATION START: ${function_name} ========" | tee -a "${VALIDATION_LOG:-/dev/null}"
}

function log_validation_checkpoint() {
    local phase="$1"
    local status="$2"
    local message="$3"
    local timestamp=$(date -Iseconds)
    echo "[${timestamp}] [${phase}] ${status}: ${message}" | tee -a "${VALIDATION_LOG:-/dev/null}"
}

function log_validation_end() {
    local status="$1"
    local duration="$2"
    local timestamp=$(date -Iseconds)
    echo "[${timestamp}] ======== VALIDATION END: ${status} (Duration: ${duration}) ========" | tee -a "${VALIDATION_LOG:-/dev/null}"
}

function save_validation_report() {
    local test_name="$1"
    local status="$2"
    local namespace="$3"
    local params_json="$4"
    local validations_json="$5"
    local results_dir="$6"
    
    local report_file="${results_dir}/validation-${test_name}.json"
    local timestamp=$(date -Iseconds)
    
    cat > "${report_file}" <<EOF
{
    "test_name": "${test_name}",
    "overallStatus": "${status}",
    "status": "${status}",
    "timestamp": "${timestamp}",
    "namespace": "${namespace}",
    "params": ${params_json},
    "validations": ${validations_json}
}
EOF
    
    echo "Validation report saved to: ${report_file}"
}

#############################################
# CHECK_VM_RUNNING FUNCTION
# With configurable percentage-based SSH validation
#############################################

function check_vm_running() {
    local label_key="$1"
    local label_value="$2"
    local namespace="$3"
    local private_key="${4:-}"
    local vm_user="${5:-}"
    local percentage_to_validate="${6:-25}"
    local max_ssh_retries="${7:-8}"
    local results_dir="${8:-/tmp/kube-burner-validations}"
    
    # Set up logging
    mkdir -p "${results_dir}"
    VALIDATION_LOG="${results_dir}/validation.log"
    
    # Determine namespace flag for oc commands
    local ns_flag="-n ${namespace}"
    if [ "${namespace}" = "all" ]; then
        ns_flag="-A"
    fi
    
    echo "=============================================="
    echo "  check_vm_running Validation"
    echo "=============================================="
    echo "Namespace:         ${namespace}"
    echo "Label:             ${label_key}=${label_value}"
    echo "SSH User:          ${vm_user:-not provided}"
    echo "SSH Validation:    ${percentage_to_validate}%"
    echo "SSH Max Retries:   ${max_ssh_retries} (15s interval)"
    echo "Results Dir:       ${results_dir}"
    echo ""
    
    log_validation_start "check_vm_running"
    local phase_start_time=$(date +%s)
    
    # VM Discovery
    local total_vms
    total_vms=$(oc get vm ${ns_flag} -l "${label_key}=${label_value}" --no-headers 2>/dev/null | wc -l)
    
    if [ "${total_vms}" -eq 0 ]; then
        echo "ERROR: No VMs found with label ${label_key}=${label_value}"
        log_validation_checkpoint "vm_discovery" "FAIL" "No VMs found"
        local phase_duration=$(( $(date +%s) - phase_start_time ))
        log_validation_end "FAILURE" "${phase_duration}s"
        
        local params_json="{\"phase_duration_seconds\": ${phase_duration}, \"total_vms\": 0}"
        local validations_json='[{"phase": "vm_discovery", "status": "FAIL", "message": "No VMs found", "duration_seconds": 0}]'
        save_validation_report "vm-running" "FAILURE" "${namespace}" "${params_json}" "${validations_json}" "${results_dir}"
        return 1
    fi
    
    log_validation_checkpoint "vm_discovery" "PASS" "Found ${total_vms} VMs"
    echo "Total VMs found: ${total_vms}"
    
    # Running State Check
    local running_check_start=$(date +%s)
    local running_vms
    running_vms=$(oc get vm ${ns_flag} -l "${label_key}=${label_value}" -o jsonpath='{.items[?(@.status.ready==true)].metadata.name}' | wc -w)
    local running_check_duration=$(( $(date +%s) - running_check_start ))
    
    echo "Running VMs: ${running_vms}/${total_vms}"
    echo "Running state check took: ${running_check_duration}s"
    
    # Node Distribution Report (for multi-node visibility)
    echo ""
    echo "VM Distribution by Node:"
    local node_distribution
    node_distribution=$(oc get vmi ${ns_flag} -l "${label_key}=${label_value}" \
        -o jsonpath='{range .items[*]}{.status.nodeName}{"\n"}{end}' 2>/dev/null | \
        sort | uniq -c | sort -rn || echo "  Unable to get node distribution")
    if [ -n "${node_distribution}" ]; then
        echo "${node_distribution}" | while read count node; do
            echo "  ${node}: ${count} VMs"
        done
    fi
    echo ""
    
    local overall_status="SUCCESS"
    local ssh_validation_status="SKIP"
    local ssh_vms_validated=0
    local ssh_vms_passed=0
    local ssh_vms_failed=0
    local ssh_validation_duration=0
    
    if [ "${running_vms}" -ne "${total_vms}" ]; then
        echo "ERROR: Not all VMs are running. Expected: ${total_vms}, Running: ${running_vms}"
        log_validation_checkpoint "vm_running_state" "FAIL" "Expected ${total_vms} running, got ${running_vms}"
        overall_status="FAILURE"
    else
        log_validation_checkpoint "vm_running_state" "PASS" "All ${total_vms} VMs are running"
        echo "SUCCESS: All VMs are running"
        
        # Random SSH connectivity validation (if credentials provided and percentage > 0)
        if [ -n "${private_key}" ] && [ -n "${vm_user}" ] && [ "${running_vms}" -gt 0 ] && [ "${percentage_to_validate}" -gt 0 ]; then
            local ssh_validation_start=$(date +%s)
            
            # Calculate number of VMs to validate
            local vms_to_validate=$(( total_vms * percentage_to_validate / 100 ))
            # Ensure at least 1 VM if percentage > 0 and VMs exist
            [ "${vms_to_validate}" -lt 1 ] && vms_to_validate=1
            
            echo ""
            echo "=============================================="
            echo "  SSH Validation Phase"
            echo "=============================================="
            echo "Total VMs:        ${total_vms}"
            echo "Validation %:     ${percentage_to_validate}%"
            echo "VMs to validate:  ${vms_to_validate}"
            echo ""
            
            # Get all VM names with their namespaces (format: namespace/vmname)
            local all_vms
            if [ "${namespace}" = "all" ]; then
                all_vms=$(oc get vm ${ns_flag} -l "${label_key}=${label_value}" -o jsonpath='{range .items[*]}{.metadata.namespace}/{.metadata.name}{" "}{end}')
            else
                all_vms=$(oc get vm ${ns_flag} -l "${label_key}=${label_value}" -o jsonpath='{range .items[*]}{.metadata.namespace}/{.metadata.name}{" "}{end}')
            fi
            
            # Shuffle VM list and select required number
            local selected_vms
            selected_vms=$(echo "${all_vms}" | tr ' ' '\n' | shuf | head -n "${vms_to_validate}")
            
            echo "Randomly selected VMs for SSH validation:"
            echo "${selected_vms}" | head -5
            [ "${vms_to_validate}" -gt 5 ] && echo "... and $((vms_to_validate - 5)) more"
            echo ""
            
            # Validate each selected VM with retry mechanism
            local vm_num=0
            for vm_entry in ${selected_vms}; do
                vm_num=$((vm_num + 1))
                # Extract namespace and VM name from format "namespace/vmname"
                local vm_ns=$(echo "${vm_entry}" | cut -d'/' -f1)
                local vm=$(echo "${vm_entry}" | cut -d'/' -f2)
                echo -n "[${vm_num}/${vms_to_validate}] Testing ${vm} (ns: ${vm_ns})... "
                
                local ssh_success=false
                local retry_count=0
                local last_error=""
                
                while [ ${retry_count} -lt ${max_ssh_retries} ] && [ "${ssh_success}" = "false" ]; do
                local ssh_test
                ssh_test=$(virtctl ssh \
                    --local-ssh-opts="-o StrictHostKeyChecking=no" \
                    --local-ssh-opts="-o UserKnownHostsFile=/dev/null" \
                    --local-ssh-opts="-o ConnectTimeout=15" \
                    --local-ssh-opts="-o BatchMode=yes" \
                    --local-ssh-opts="-o PasswordAuthentication=no" \
                    --local-ssh-opts="-o PreferredAuthentications=publickey" \
                    -n "${vm_ns}" -i "${private_key}" \
                    --command "hostname && echo SSH_OK" \
                    "${vm_user}@vmi/${vm}" 2>&1 || echo "SSH_FAILED")
                    
                    if echo "${ssh_test}" | grep -q "SSH_OK"; then
                        ssh_success=true
                    else
                        last_error=$(echo "${ssh_test}" | head -1)
                        retry_count=$((retry_count + 1))
                        if [ ${retry_count} -lt ${max_ssh_retries} ]; then
                            echo -n "retry ${retry_count}/${max_ssh_retries}... "
                            sleep 15
                        fi
                    fi
                done
                
                ssh_vms_validated=$((ssh_vms_validated + 1))
                
                if [ "${ssh_success}" = "true" ]; then
                    if [ ${retry_count} -gt 0 ]; then
                        echo "✓ PASS (after ${retry_count} retries)"
                    else
                        echo "✓ PASS"
                    fi
                    ssh_vms_passed=$((ssh_vms_passed + 1))
                else
                    echo "✗ FAIL (after ${max_ssh_retries} retries)"
                    ssh_vms_failed=$((ssh_vms_failed + 1))
                    echo "    Last error: ${last_error}"
                fi
            done
            
            ssh_validation_duration=$(( $(date +%s) - ssh_validation_start ))
            
            echo ""
            echo "=============================================="
            echo "  SSH Validation Summary"
            echo "=============================================="
            echo "VMs Validated: ${ssh_vms_validated}"
            echo "Passed:        ${ssh_vms_passed}"
            echo "Failed:        ${ssh_vms_failed}"
            echo "Duration:      ${ssh_validation_duration}s"
            echo "=============================================="
            
            # Determine SSH validation status
            if [ "${ssh_vms_failed}" -eq 0 ]; then
                ssh_validation_status="PASS"
                log_validation_checkpoint "ssh_validation" "PASS" "${ssh_vms_passed}/${ssh_vms_validated} VMs SSH accessible"
            else
                ssh_validation_status="PARTIAL"
                log_validation_checkpoint "ssh_validation" "PARTIAL" "${ssh_vms_passed}/${ssh_vms_validated} VMs SSH accessible, ${ssh_vms_failed} failed"
                # Note: We don't fail overall_status for partial SSH - it's informational
            fi
        else
            if [ -z "${private_key}" ] || [ -z "${vm_user}" ]; then
                log_validation_checkpoint "ssh_validation" "SKIP" "SSH credentials not provided"
                echo "SSH validation: SKIPPED (credentials not provided)"
            elif [ "${percentage_to_validate}" -eq 0 ]; then
                log_validation_checkpoint "ssh_validation" "SKIP" "SSH validation disabled (percentage=0)"
                echo "SSH validation: SKIPPED (disabled)"
            fi
        fi
    fi
    
    local phase_end_time=$(date +%s)
    local phase_duration=$((phase_end_time - phase_start_time))
    log_validation_end "${overall_status}" "${phase_duration}s"
    
    # Get node distribution for JSON report
    local node_count
    node_count=$(oc get vmi ${ns_flag} -l "${label_key}=${label_value}" \
        -o jsonpath='{range .items[*]}{.status.nodeName}{"\n"}{end}' 2>/dev/null | \
        sort -u | wc -l || echo "0")
    
    # Generate JSON report with detailed metrics
    local params_json
    params_json=$(cat <<PARAMS
{
    "label_key": "${label_key}",
    "label_value": "${label_value}",
    "total_vms": ${total_vms},
    "running_vms": ${running_vms},
    "nodes_used": ${node_count},
    "phase_duration_seconds": ${phase_duration},
    "running_check_duration_seconds": ${running_check_duration},
    "ssh_validation": {
        "enabled": $([ -n "${private_key}" ] && echo "true" || echo "false"),
        "percentage_configured": ${percentage_to_validate},
        "max_retries_configured": ${max_ssh_retries},
        "retry_interval_seconds": 15,
        "vms_validated": ${ssh_vms_validated},
        "vms_passed": ${ssh_vms_passed},
        "vms_failed": ${ssh_vms_failed},
        "duration_seconds": ${ssh_validation_duration}
    }
}
PARAMS
)
    
    local validations_json
    validations_json=$(cat <<VALIDATIONS
[
    {"phase": "vm_discovery", "status": "PASS", "message": "Found ${total_vms} VMs", "duration_seconds": 0},
    {"phase": "vm_running_state", "status": "$([ "${overall_status}" = "SUCCESS" ] && echo "PASS" || echo "FAIL")", "message": "${running_vms}/${total_vms} VMs running", "duration_seconds": ${running_check_duration}},
    {"phase": "ssh_validation", "status": "${ssh_validation_status}", "message": "${ssh_vms_passed}/${ssh_vms_validated} VMs SSH accessible", "duration_seconds": ${ssh_validation_duration}}
]
VALIDATIONS
)
    
    save_validation_report "vm-running" "${overall_status}" "${namespace}" "${params_json}" "${validations_json}" "${results_dir}"
    
    if [ "${overall_status}" = "FAILURE" ]; then
        return 1
    fi
    return 0
}

#############################################
# CHECK_RESIZE FUNCTION
# With JSON reporting and duration tracking
#############################################

function check_resize() {
    local label_key="$1"
    local label_value="$2"
    local namespace="$3"
    local private_key="$4"
    local vm_user="$5"
    local expected_root_size="$6"
    local expected_data_size="$7"
    local results_dir="${8:-/tmp/kube-burner-validations}"

    # Set up logging
    mkdir -p "${results_dir}"
    VALIDATION_LOG="${results_dir}/validation.log"

    echo "=============================================="
    echo "  check_resize Validation"
    echo "=============================================="
    echo "Namespace:           ${namespace}"
    echo "Label:               ${label_key}=${label_value}"
    echo "Expected Root Size:  ${expected_root_size}"
    echo "Expected Data Size:  ${expected_data_size}"
    echo "SSH User:            ${vm_user}"
    echo "Results Dir:         ${results_dir}"
    echo ""

    log_validation_start "check_resize"
    local phase_start_time=$(date +%s)

    local overall_status="SUCCESS"
    local vms_checked=0
    local vms_passed=0
    local vms_failed=0

    # Get VMs
    local vms
    vms=$(oc get vm -n "${namespace}" -l "${label_key}=${label_value}" -o jsonpath='{.items[*].metadata.name}')

    if [ -z "${vms}" ]; then
        echo "ERROR: No VMs found with label ${label_key}=${label_value}"
        log_validation_checkpoint "vm_discovery" "FAIL" "No VMs found"
        local phase_duration=$(( $(date +%s) - phase_start_time ))
        log_validation_end "FAILURE" "${phase_duration}s"

        local params_json="{\"phase_duration_seconds\": ${phase_duration}, \"total_vms\": 0}"
        local validations_json='[{"phase": "vm_discovery", "status": "FAIL", "message": "No VMs found", "duration_seconds": 0}]'
        save_validation_report "resize" "FAILURE" "${namespace}" "${params_json}" "${validations_json}" "${results_dir}"
        return 1
    fi

    local total_vms=$(echo "${vms}" | wc -w)
    echo "Total VMs found: ${total_vms}"
    log_validation_checkpoint "vm_discovery" "PASS" "Found ${total_vms} VMs"

    for vm in ${vms}; do
        vms_checked=$((vms_checked + 1))
        echo ""
        echo "[${vms_checked}/${total_vms}] Checking ${vm}..."

        # Get block devices via SSH
        local blk_devices
        blk_devices=$(virtctl ssh \
            --local-ssh-opts="-o StrictHostKeyChecking=no" \
            --local-ssh-opts="-o UserKnownHostsFile=/dev/null" \
            --local-ssh-opts="-o ConnectTimeout=30" \
            --local-ssh-opts="-o BatchMode=yes" \
            -n "${namespace}" -i "${private_key}" \
            --command "lsblk --json -v --output=NAME,SIZE" \
            "${vm_user}@vmi/${vm}" 2>/dev/null || echo "SSH_FAILED")

        if echo "${blk_devices}" | grep -q "SSH_FAILED"; then
            echo "  ✗ Failed to SSH to VM ${vm}"
            log_validation_checkpoint "resize_check" "FAIL" "SSH failed for VM ${vm}"
            vms_failed=$((vms_failed + 1))
            overall_status="FAILURE"
            continue
        fi

        # Check root volume size (vda)
        local root_size
        root_size=$(echo "${blk_devices}" | jq -r '.blockdevices[] | select(.name == "vda") | .size' 2>/dev/null)

        if [[ "${root_size}" != "${expected_root_size}" ]]; then
            echo "  ✗ Root volume size mismatch. Expected: ${expected_root_size}, Actual: ${root_size}"
            log_validation_checkpoint "resize_check" "FAIL" "Root size mismatch for VM ${vm}: expected ${expected_root_size}, got ${root_size}"
            vms_failed=$((vms_failed + 1))
            overall_status="FAILURE"
            continue
        fi

        echo "  ✓ Root volume: ${root_size}"

        # Check data volume sizes (non-vda volumes)
        local data_sizes
        data_sizes=$(echo "${blk_devices}" | jq -r '.blockdevices[] | select(.name != "vda") | .size' 2>/dev/null)

        local data_check_passed=true
        for data_size in ${data_sizes}; do
            if [[ "${data_size}" != "${expected_data_size}" ]]; then
                echo "  ✗ Data volume size mismatch. Expected: ${expected_data_size}, Actual: ${data_size}"
                log_validation_checkpoint "resize_check" "FAIL" "Data size mismatch for VM ${vm}: expected ${expected_data_size}, got ${data_size}"
                data_check_passed=false
                overall_status="FAILURE"
                break
            fi
            echo "  ✓ Data volume: ${data_size}"
        done

        if [ "${data_check_passed}" = "true" ]; then
            vms_passed=$((vms_passed + 1))
            log_validation_checkpoint "resize_check" "PASS" "VM ${vm}: all volumes correctly resized"
        else
            vms_failed=$((vms_failed + 1))
        fi
    done

    local phase_end_time=$(date +%s)
    local phase_duration=$((phase_end_time - phase_start_time))
    log_validation_end "${overall_status}" "${phase_duration}s"

    echo ""
    echo "=============================================="
    echo "  Resize Validation Summary"
    echo "=============================================="
    echo "VMs Checked:  ${vms_checked}"
    echo "Passed:       ${vms_passed}"
    echo "Failed:       ${vms_failed}"
    echo "Duration:     ${phase_duration}s"
    echo "Status:       ${overall_status}"
    echo "=============================================="

    # Generate JSON report
    local params_json
    params_json=$(cat <<PARAMS
{
    "label_key": "${label_key}",
    "label_value": "${label_value}",
    "total_vms": ${total_vms},
    "vms_checked": ${vms_checked},
    "vms_passed": ${vms_passed},
    "vms_failed": ${vms_failed},
    "expected_root_size": "${expected_root_size}",
    "expected_data_size": "${expected_data_size}",
    "phase_duration_seconds": ${phase_duration}
}
PARAMS
)

    local validations_json
    validations_json=$(cat <<VALIDATIONS
[
    {"phase": "vm_discovery", "status": "PASS", "message": "Found ${total_vms} VMs", "duration_seconds": 0},
    {"phase": "resize_check", "status": "$([ "${overall_status}" = "SUCCESS" ] && echo "PASS" || echo "FAIL")", "message": "${vms_passed}/${vms_checked} VMs resized correctly", "duration_seconds": ${phase_duration}}
]
VALIDATIONS
)

    save_validation_report "resize" "${overall_status}" "${namespace}" "${params_json}" "${validations_json}" "${results_dir}"

    if [ "${overall_status}" = "FAILURE" ]; then
        return 1
    fi
    return 0
}

#############################################
# MAIN SCRIPT LOGIC
#############################################

case "$1" in
    check_vm_running)
        shift
        check_vm_running "$@"
        ;;
    check_resize)
        shift
        check_resize "$@"
        ;;
    *)
        echo "Usage: $0 {check_vm_running|check_resize} [args...]"
        echo ""
        echo "check_vm_running <label_key> <label_value> <namespace> [private_key] [vm_user] [percentage_to_validate] [max_ssh_retries] [results_dir]"
        echo "check_resize <label_key> <label_value> <namespace> <private_key> <vm_user> <expected_root_size> <expected_data_size> [results_dir]"
        exit 1
        ;;
esac


