#!/bin/bash
#
# run-workloads.sh - Unified VME Test Runner
#
# A single script to run all VME (Virtual Machine Extension) scenarios
# for OpenShift Virtualization testing with kube-burner.
#
# Usage:
#   # Single test (replaces ./run-test.sh)
#   ./run-workloads.sh cpu-limits
#   ./run-workloads.sh cpu-limits --mode sanity
#   cpuCores=8 ./run-workloads.sh cpu-limits --log-level=debug
#
#   # Multiple tests (replaces run-workloads.sh)
#   ./run-workloads.sh --all --mode sanity
#   ./run-workloads.sh --all --mode full --parallel
#   ./run-workloads.sh cpu-limits memory-limits disk-limits --sequential
#
#   # List available tests
#   ./run-workloads.sh --list
#
# Environment Variables:
#   All test-specific variables can be passed as env vars (case-sensitive!)
#   Example: cpuCores=8 memorySize=16Gi ./run-workloads.sh cpu-limits memory-limits
#

set -eo pipefail

# =============================================================================
# CONFIGURATION
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULTS_BASE="/tmp/kube-burner-results"

# Test Registry: test_name -> "relative_dir:config_file:vars_extension"
declare -A TEST_REGISTRY=(
    ["cpu-limits"]="resource-limits/cpu-limits:cpu-limits-test.yml:yml"
    ["memory-limits"]="resource-limits/memory-limits:memory-limits-test.yml:yml"
    ["disk-limits"]="resource-limits/disk-limits:disk-limits-test.yml:yml"
    ["disk-hotplug"]="hot-plug/disk-hotplug:disk-hotplug-test.yml:yaml"
    ["nic-hotplug"]="hot-plug/nic-hotplug:nic-hotplug-test.yml:yml"
    ["minimal-resources"]="performance/minimal-resources:minimal-resources-test.yml:yml"
    ["large-disk"]="performance/large-disk:large-disk-performance.yml:yml"
    ["high-memory"]="performance/high-memory:high-memory-performance.yml:yml"
    ["per-host-density"]="scale-testing/per-host-density:per-host-density.yml:yml"
    ["virt-capacity-benchmark"]="scale-testing/virt-capacity-benchmark:virt-capacity-benchmark.yml:yml"
)

# Ordered list for --all execution
TEST_ORDER=(
    "cpu-limits"
    "memory-limits"
    "disk-limits"
    "disk-hotplug"
    "nic-hotplug"
    "minimal-resources"
    "large-disk"
    "high-memory"
    "per-host-density"
    "virt-capacity-benchmark"
)

# Default settings
MODE="full"                    # sanity or full
EXECUTION="sequential"         # sequential or parallel
KUBE_BURNER_ARGS=()           # Additional args to pass to kube-burner

# Main log file (initialized in main)
MAIN_LOG=""
MAIN_TIMESTAMP=""

# Results tracking for summary
declare -A TEST_RESULTS       # test -> exit_code
declare -A TEST_DURATIONS     # test -> duration_seconds
declare -A TEST_PATHS         # test -> results_path
declare -A TEST_VALIDATIONS   # test -> validation_status
declare -A TEST_VAL_FILES     # test -> validation_file_path

# =============================================================================
# LOGGING FUNCTIONS
# =============================================================================

# Log to main log file AND stdout with timestamp
logmain() {
    local level="${1:-INFO}"
    shift
    local timestamp=$(date "+%Y-%m-%d %H:%M:%S")
    local message="[$timestamp] $level  $*"
    echo "$message"
    if [[ -n "$MAIN_LOG" ]]; then
        echo "$message" >> "$MAIN_LOG"
    fi
}

# Log error to main log file AND stderr
logerr() {
    local timestamp=$(date "+%Y-%m-%d %H:%M:%S")
    local message="[$timestamp] ERROR $*"
    echo "$message" >&2
    if [[ -n "$MAIN_LOG" ]]; then
        echo "$message" >> "$MAIN_LOG"
    fi
}

# Log to stdout only (for test-specific output during execution)
log() {
    echo "$@"
}

# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

# Get vars file path based on mode and test extension
get_vars_file() {
    local test_dir="$1"
    local ext="$2"
    
    if [[ "$MODE" == "sanity" ]]; then
        echo "${test_dir}/vars-sanity.${ext}"
    else
        echo "${test_dir}/vars.${ext}"
    fi
}

# Read a value from a YAML file
# Usage: get_yaml_value "key" "vars_file" "default_value"
get_yaml_value() {
    local key="$1"
    local file="$2"
    local default="$3"
    
    if [[ -f "$file" ]]; then
        local value=$(grep "^${key}:" "$file" 2>/dev/null | head -1 | awk '{print $2}' | tr -d '"' | tr -d "'")
        if [[ -n "$value" ]]; then
            echo "$value"
            return
        fi
    fi
    echo "$default"
}

# Parse test registry entry
parse_registry() {
    local entry="$1"
    local field="$2"
    
    case "$field" in
        dir)    echo "$entry" | cut -d: -f1 ;;
        config) echo "$entry" | cut -d: -f2 ;;
        ext)    echo "$entry" | cut -d: -f3 ;;
    esac
}

# Format duration in human-readable format
format_duration() {
    local seconds=$1
    if (( seconds >= 3600 )); then
        printf "%dh %dm %ds" $((seconds/3600)) $((seconds%3600/60)) $((seconds%60))
    elif (( seconds >= 60 )); then
        printf "%dm %ds" $((seconds/60)) $((seconds%60))
    else
        printf "%ds" $seconds
    fi
}

# Get validation status and file paths from results directory
get_validation_info() {
    local results_dir="$1"
    local test_name="$2"
    
    local status="N/A"
    local first_file=""
    
    # Find validation JSON files
    local validation_files=$(find "${results_dir}" -name "validation-*.json" -type f 2>/dev/null | head -5)
    
    if [[ -n "$validation_files" ]]; then
        local all_success=true
        
        while IFS= read -r file; do
            if [[ -f "$file" ]]; then
                # Store first file for reference
                [[ -z "$first_file" ]] && first_file="$file"
                # Check both .overallStatus (our format) and .status (fallback)
                local file_status=$(jq -r '.overallStatus // .status // "UNKNOWN"' "$file" 2>/dev/null || echo "UNKNOWN")
                if [[ "$file_status" != "SUCCESS" && "$file_status" != "PASS" ]]; then
                    all_success=false
                    status="$file_status"
                    break
                fi
            fi
        done <<< "$validation_files"
        
        if [[ "$all_success" == true ]]; then
            status="SUCCESS"
        fi
    fi
    
    # Return status and first validation file (pipe-separated, no newlines in file path)
    echo "${status}|${first_file}"
}

# =============================================================================
# TEST-SPECIFIC SETUP FUNCTIONS
# =============================================================================

# Setup for per-host-density test
# Args: $1 = vars file path
setup_per_host_density() {
    local vars_file="$1"
    
    # Read values: CLI env var takes precedence, then vars file, then default
    local scale_mode="${scaleMode:-$(get_yaml_value "scaleMode" "$vars_file" "single-node")}"
    local vms_per_ns="${vmsPerNamespace:-$(get_yaml_value "vmsPerNamespace" "$vars_file" "10")}"
    local ns_count="${namespaceCount:-$(get_yaml_value "namespaceCount" "$vars_file" "1")}"
    local pct_validate="${percentage_of_vms_to_validate:-$(get_yaml_value "percentage_of_vms_to_validate" "$vars_file" "25")}"
    local ssh_retries="${max_ssh_retries:-$(get_yaml_value "max_ssh_retries" "$vars_file" "8")}"
    
    # Auto-select first worker if single-node mode with no targetNode
    if [[ "$scale_mode" != "multi-node" ]] && [[ -z "$targetNode" ]]; then
        local first_worker=$(kubectl get nodes -l node-role.kubernetes.io/worker= \
            --no-headers -o custom-columns=NAME:.metadata.name 2>/dev/null | head -1)
        if [[ -n "$first_worker" ]]; then
            export targetNode="$first_worker"
            logmain INFO "[per-host-density] Auto-selected targetNode: $targetNode"
        fi
    fi
    
    # Display scale configuration
    local total_vms=$((vms_per_ns * ns_count))
    log ""
    log "Scale Configuration:"
    log "  scaleMode=${scale_mode}"
    log "  namespaceCount=${ns_count}"
    log "  vmsPerNamespace=${vms_per_ns}"
    log "  totalVMs=${total_vms}"
    if [[ "$scale_mode" == "multi-node" ]]; then
        local worker_count=$(kubectl get nodes -l node-role.kubernetes.io/worker= --no-headers 2>/dev/null | wc -l || echo "unknown")
        log "  workerNodes=${worker_count}"
    else
        log "  targetNode=${targetNode:-not set}"
    fi
    log ""
    log "Validation Configuration:"
    log "  percentage_of_vms_to_validate=${pct_validate}%"
    log "  max_ssh_retries=${ssh_retries}"
    log ""
    
    return 0
}

# Setup for nic-hotplug test
setup_nic_hotplug() {
    if [[ -z "$baseInterface" ]]; then
        local detect_script="${SCRIPT_DIR}/config/scripts/detect-available-interface.sh"
        if [[ -x "$detect_script" ]]; then
            logmain INFO "[nic-hotplug] Auto-detecting baseInterface..."
            local detected=$("$detect_script" 2>/dev/null || true)
            if [[ -n "$detected" ]]; then
                export baseInterface="$detected"
                logmain INFO "[nic-hotplug] Auto-detected baseInterface: $baseInterface"
            else
                logerr "[nic-hotplug] Failed to auto-detect baseInterface"
                logerr "[nic-hotplug] Please set baseInterface manually: baseInterface=ens2f0 ./run-workloads.sh nic-hotplug"
                return 1
            fi
        else
            logerr "[nic-hotplug] detect-available-interface.sh not found or not executable"
            logerr "[nic-hotplug] Please set baseInterface manually: baseInterface=ens2f0 ./run-workloads.sh nic-hotplug"
            return 1
        fi
    fi
    
    # Determine nicCount: CLI env var > vars file > default
    local effective_nic_count="${nicCount:-}"
    if [[ -z "$effective_nic_count" ]]; then
        # Read from vars file based on MODE
        local vars_file="${SCRIPT_DIR}/hot-plug/nic-hotplug/vars.yml"
        if [[ "$MODE" == "sanity" ]]; then
            vars_file="${SCRIPT_DIR}/hot-plug/nic-hotplug/vars-sanity.yml"
        fi
        if [[ -f "$vars_file" ]]; then
            effective_nic_count=$(grep "^nicCount:" "$vars_file" 2>/dev/null | awk '{print $2}')
        fi
        # Fallback to default if still empty
        effective_nic_count="${effective_nic_count:-25}"
    fi
    
    log ""
    log "NIC Configuration:"
    log "  baseInterface=${baseInterface}"
    log "  nicCount=${effective_nic_count}"
    log ""
    
    return 0
}

# Dispatcher for test-specific setup
# Args: $1 = test_name, $2 = vars_file
run_setup() {
    local test_name="$1"
    local vars_file="$2"
    
    case "$test_name" in
        per-host-density)
            setup_per_host_density "$vars_file"
            ;;
        nic-hotplug)
            setup_nic_hotplug
            ;;
        *)
            # No special setup needed
            return 0
            ;;
    esac
}

# =============================================================================
# TEST-SPECIFIC CLEANUP FUNCTIONS
# =============================================================================

# Cleanup for per-host-density test
# Args: $1 = vars file path
cleanup_per_host_density() {
    local vars_file="$1"
    
    # Check if cleanup is enabled (CLI env var takes precedence, then vars file)
    local do_cleanup="${cleanup:-$(get_yaml_value "cleanup" "$vars_file" "true")}"
    
    if [[ "$do_cleanup" == "true" ]]; then
        logmain INFO "[per-host-density] Cleanup enabled - deleting test namespaces..."
        
        # Delete namespaces with the test label
        local deleted_count=$(kubectl delete ns -l kube-burner.io/test-name=per-host-density --wait=false 2>/dev/null | wc -l || echo "0")
        
        if [[ "$deleted_count" -gt 0 ]]; then
            logmain INFO "[per-host-density] Initiated deletion of namespaces (running in background)"
        else
            logmain INFO "[per-host-density] No test namespaces found to delete"
        fi
    else
        logmain INFO "[per-host-density] Cleanup disabled - namespaces preserved"
    fi
    
    return 0
}

# Dispatcher for test-specific cleanup
# Args: $1 = test_name, $2 = vars_file
run_cleanup() {
    local test_name="$1"
    local vars_file="$2"
    
    case "$test_name" in
        per-host-density)
            cleanup_per_host_density "$vars_file"
            ;;
        *)
            # No special cleanup needed
            return 0
            ;;
    esac
}

# =============================================================================
# CORE EXECUTION
# =============================================================================

# Run a single test
run_single_test() {
    local test_name="$1"
    shift
    local extra_args=("$@")
    
    # Validate test exists
    if [[ -z "${TEST_REGISTRY[$test_name]}" ]]; then
        logerr "Unknown test: $test_name"
        logerr "Use --list to see available tests"
        return 1
    fi
    
    # Parse registry entry
    local entry="${TEST_REGISTRY[$test_name]}"
    local rel_dir=$(parse_registry "$entry" "dir")
    local config=$(parse_registry "$entry" "config")
    local ext=$(parse_registry "$entry" "ext")
    
    local test_dir="${SCRIPT_DIR}/${rel_dir}"
    local config_file="${test_dir}/${config}"
    local vars_file=$(get_vars_file "$test_dir" "$ext")
    
    # Validate files exist
    if [[ ! -f "$config_file" ]]; then
        logerr "Config file not found: $config_file"
        return 1
    fi
    
    if [[ ! -f "$vars_file" ]]; then
        logerr "Vars file not found: $vars_file"
        logerr "Test '$test_name' may not support '$MODE' mode"
        return 1
    fi
    
    # Generate timestamp for this test run
    local run_timestamp="run-$(date +%Y%m%d-%H%M%S)"
    export runTimestamp="$run_timestamp"
    
    # Create results directory
    local results_path="${RESULTS_BASE}/${test_name}/${run_timestamp}"
    mkdir -p "$results_path"
    
    # Create temporary vars file with TIMESTAMP replaced and correct paths
    local unique_suffix="$(date +%Y%m%d-%H%M%S)-$(cat /dev/urandom | tr -dc 'a-z0-9' | fold -w 4 | head -n 1)"
    local temp_vars="${results_path}/vars-${test_name}-${MODE}.${ext}"
    sed -e "s/TIMESTAMP/${unique_suffix}/g" \
        -e "s|^resultsPath:.*|resultsPath: \"${RESULTS_BASE}/${test_name}\"|" \
        -e "s|^runTimestamp:.*|runTimestamp: \"${run_timestamp}\"|" \
        "$vars_file" > "$temp_vars"
    
    logmain INFO "[$test_name] Starting test"
    logmain INFO "[$test_name] Mode: $MODE"
    logmain INFO "[$test_name] Config: $config_file"
    logmain INFO "[$test_name] Vars: $temp_vars"
    logmain INFO "[$test_name] Results: $results_path"
    
    local start_time=$(date +%s)
    
    # Print test header
    log ""
    log "=============================================="
    log "  ${test_name} Test"
    log "=============================================="
    log "Timestamp: ${run_timestamp}"
    log "Mode: ${MODE}"
    log "Results: ${results_path}/"
    
    # Run test-specific setup (pass temp_vars for reading config values)
    if ! run_setup "$test_name" "$temp_vars"; then
        logerr "[$test_name] Setup failed"
        TEST_RESULTS[$test_name]=1
        TEST_DURATIONS[$test_name]=0
        TEST_PATHS[$test_name]="$results_path"
        TEST_VALIDATIONS[$test_name]="SETUP_FAILED"
        TEST_VAL_FILES[$test_name]=""
        return 1
    fi
    
    log ""
    log "Starting kube-burner..."
    log ""
    
    # Run kube-burner with temp vars file
    local exit_code=0
    (
        cd "$test_dir"
        kube-burner init \
            --config="$config" \
            --user-data="$temp_vars" \
            "${extra_args[@]}" "${KUBE_BURNER_ARGS[@]}" 2>&1 | tee "${results_path}/kube-burner.log"
        exit ${PIPESTATUS[0]}
    ) || exit_code=$?
    
    local end_time=$(date +%s)
    local duration=$((end_time - start_time))
    
    # Get validation info
    local val_info=$(get_validation_info "$results_path" "$test_name")
    local val_status=$(echo "$val_info" | cut -d'|' -f1)
    local val_file=$(echo "$val_info" | cut -d'|' -f2)
    
    # Store results
    TEST_RESULTS[$test_name]=$exit_code
    TEST_DURATIONS[$test_name]=$duration
    TEST_PATHS[$test_name]="$results_path"
    TEST_VALIDATIONS[$test_name]="$val_status"
    TEST_VAL_FILES[$test_name]="$val_file"
    
    # Build validation_files array properly using jq
    local val_files_json=$(find "$results_path" -name "validation-*.json" -type f 2>/dev/null | \
                          jq -R -s 'split("\n") | map(select(length > 0))')
    [[ -z "$val_files_json" || "$val_files_json" == "[]" ]] && val_files_json="[]"
    
    # Write summary.json using jq for proper JSON formatting
    local summary_json="${results_path}/summary.json"
    jq -n \
      --arg test "$test_name" \
      --arg mode "$MODE" \
      --argjson exit_code "$exit_code" \
      --arg results_path "$results_path" \
      --arg kube_burner_log "${results_path}/kube-burner.log" \
      --arg val_status "$val_status" \
      --argjson val_files "$val_files_json" \
      --argjson duration "$duration" \
      --arg timestamp "$(date -Iseconds)" \
      '{
        test: $test,
        mode: $mode,
        exit_code: $exit_code,
        results_path: $results_path,
        kube_burner_log: $kube_burner_log,
        validation_status: $val_status,
        validation_files: $val_files,
        duration_seconds: $duration,
        timestamp: $timestamp
      }' > "$summary_json"
    
    # Run test-specific cleanup (e.g., delete namespaces if cleanup=true)
    run_cleanup "$test_name" "$temp_vars"
    
    # Print test footer
    log ""
    log "=============================================="
    if [[ $exit_code -eq 0 ]]; then
        log "✓ Test Complete - SUCCESS"
        logmain INFO "[$test_name] Completed: exit_code=0, duration=$(format_duration $duration)"
    else
        log "✗ Test Complete - FAILED (exit code: $exit_code)"
        logmain INFO "[$test_name] Completed: exit_code=$exit_code, duration=$(format_duration $duration)"
    fi
    log "=============================================="
    log ""
    log "Results location:"
    log "  ${results_path}/"
    log ""
    log "View kube-burner log:"
    log "  cat ${results_path}/kube-burner.log"
    log ""
    log "View test results:"
    log "  ls -lh ${results_path}/iteration-*/"
    log ""
    if [[ -n "$val_files" ]]; then
        log "View validation reports:"
        echo "$val_files" | while IFS= read -r f; do
            [[ -n "$f" ]] && log "  cat $f"
        done
    fi
    log ""
    
    return $exit_code
}

# =============================================================================
# MULTI-TEST ORCHESTRATION
# =============================================================================

# Run tests sequentially
run_tests_sequential() {
    local tests=("$@")
    local failed=0
    
    for test_name in "${tests[@]}"; do
        logmain INFO "[$test_name] Queued for sequential execution"
    done
    
    for test_name in "${tests[@]}"; do
        run_single_test "$test_name" || ((failed++)) || true
    done
    
    return $failed
}

# Run tests in parallel
run_tests_parallel() {
    local tests=("$@")
    local pids=()
    local pid_to_test=()
    local parent_pid=$$
    
    # Start all tests in background
    for test_name in "${tests[@]}"; do
        logmain INFO "[$test_name] Starting in background"
        
        # Create a subshell for each test
        (
            # Redirect output to test-specific log
            local test_log="${RESULTS_BASE}/${test_name}-parallel-${parent_pid}.log"
            run_single_test "$test_name" > "$test_log" 2>&1
            exit $?
        ) &
        
        local pid=$!
        pids+=($pid)
        pid_to_test[$pid]="$test_name"
        
        logmain INFO "[$test_name] Started with PID $pid"
    done
    
    # Wait for all tests to complete
    local failed=0
    for pid in "${pids[@]}"; do
        local test_name="${pid_to_test[$pid]}"
        logmain INFO "[$test_name] Waiting for PID $pid..."
        
        if wait $pid; then
            logmain INFO "[$test_name] PID $pid completed successfully"
        else
            local exit_code=$?
            logmain INFO "[$test_name] PID $pid failed with exit code $exit_code"
            ((failed++)) || true
        fi
        
        # Show the test output
        local test_log="${RESULTS_BASE}/${test_name}-parallel-${parent_pid}.log"
        if [[ -f "$test_log" ]]; then
            cat "$test_log"
            rm -f "$test_log"
        fi
    done
    
    # After all parallel tests complete, read results from summary.json files
    # This is needed because associative arrays set in subshells don't propagate back
    for test_name in "${tests[@]}"; do
        # Find the most recent run directory for this test (more robust than find | xargs)
        local latest_run=$(ls -td "${RESULTS_BASE}/${test_name}"/run-* 2>/dev/null | head -1)
        local summary_file="${latest_run}/summary.json"
        
        if [[ -f "$summary_file" ]]; then
            # Parse the summary.json and populate arrays
            local exit_code=$(jq -r '.exit_code // 999' "$summary_file" 2>/dev/null)
            local duration=$(jq -r '.duration_seconds // 0' "$summary_file" 2>/dev/null)
            local results_path=$(jq -r '.results_path // "N/A"' "$summary_file" 2>/dev/null)
            local val_status=$(jq -r '.validation_status // "N/A"' "$summary_file" 2>/dev/null)
            # Get first validation file from array
            local val_file=$(jq -r '.validation_files[0] // ""' "$summary_file" 2>/dev/null)
            
            TEST_RESULTS[$test_name]=$exit_code
            TEST_DURATIONS[$test_name]=$duration
            TEST_PATHS[$test_name]="$results_path"
            TEST_VALIDATIONS[$test_name]="$val_status"
            TEST_VAL_FILES[$test_name]="$val_file"
            
            logmain INFO "[$test_name] Loaded results: exit=$exit_code, duration=${duration}s, validation=$val_status"
        else
            logmain INFO "[$test_name] No summary.json found at ${summary_file:-unknown}, test may have failed early"
            TEST_RESULTS[$test_name]=999
            TEST_DURATIONS[$test_name]=0
            TEST_PATHS[$test_name]="N/A"
            TEST_VALIDATIONS[$test_name]="NO_SUMMARY"
            TEST_VAL_FILES[$test_name]=""
        fi
    done
    
    return $failed
}

# =============================================================================
# SUMMARY DISPLAY
# =============================================================================

print_summary_table() {
    local tests=("$@")
    
    echo ""
    echo "================================================================================"
    echo "                           VME Test Suite Summary"
    echo "================================================================================"
    echo "MODE: $MODE | EXECUTION: $EXECUTION | TESTS: ${#tests[@]}"
    echo "MAIN LOG: $MAIN_LOG"
    echo ""
    
    printf "%-24s %-10s %-12s %-12s\n" "TEST" "STATUS" "VALIDATION" "DURATION"
    echo "--------------------------------------------------------------------------------"
    
    local passed=0
    local failed=0
    
    for test_name in "${tests[@]}"; do
        local exit_code=${TEST_RESULTS[$test_name]:-999}
        local duration=${TEST_DURATIONS[$test_name]:-0}
        local results_path=${TEST_PATHS[$test_name]:-"N/A"}
        local val_status=${TEST_VALIDATIONS[$test_name]:-"N/A"}
        local val_files=${TEST_VAL_FILES[$test_name]:-""}
        
        local status="UNKNOWN"
        if [[ $exit_code -eq 0 ]]; then
            status="PASS"
            ((passed++)) || true
        elif [[ $exit_code -eq 999 ]]; then
            status="SKIPPED"
        else
            status="FAIL"
            ((failed++)) || true
        fi
        
        local duration_str=$(format_duration $duration)
        
        printf "%-24s %-10s %-12s %-12s\n" "$test_name" "$status" "$val_status" "$duration_str"
        
        # Print results path
        if [[ "$results_path" != "N/A" ]]; then
            echo "  Results: ${results_path}/"
        fi
        
        # Print validation file or log file for failed tests
        if [[ -n "$val_files" && -f "$val_files" ]]; then
            echo "  Validation: $val_files"
        elif [[ "$status" == "FAIL" && -f "${results_path}/kube-burner.log" ]]; then
            echo "  Log: ${results_path}/kube-burner.log"
        fi
        
        echo ""
    done
    
    echo "================================================================================"
    echo "PASSED: $passed | FAILED: $failed | TOTAL: ${#tests[@]}"
    echo "================================================================================"
    echo ""
    echo "Main log file:"
    echo "  $MAIN_LOG"
    echo ""
}

# =============================================================================
# HELP AND LIST
# =============================================================================

show_help() {
    cat << EOF
run-workloads.sh - Unified VME Test Runner

USAGE:
    ./run-workloads.sh [OPTIONS] [TEST_NAMES...]

EXAMPLES:
    # Single test
    ./run-workloads.sh cpu-limits
    ./run-workloads.sh cpu-limits --mode sanity
    cpuCores=8 ./run-workloads.sh cpu-limits

    # Multiple tests
    ./run-workloads.sh --all --mode sanity
    ./run-workloads.sh --all --mode full --parallel
    ./run-workloads.sh cpu-limits memory-limits --sequential

    # List tests
    ./run-workloads.sh --list

OPTIONS:
    --mode <sanity|full>    Select vars file (default: full)
                            sanity: uses vars-sanity.yml
                            full: uses vars.yml
    
    --parallel              Run tests in parallel
    --sequential            Run tests sequentially (default)
    
    --all                   Run all available tests
    
    --list                  List available tests and exit
    
    --help, -h              Show this help message

KUBE-BURNER OPTIONS:
    All other options are passed directly to kube-burner:
    --log-level=debug       Set kube-burner log level
    --timeout=1h            Set timeout

ENVIRONMENT VARIABLES:
    Test-specific variables can be passed as environment variables.
    Variable names are CASE-SENSITIVE!
    
    Examples:
        cpuCores=8 ./run-workloads.sh cpu-limits
        vmsPerNamespace=100 targetNode=worker001 ./run-workloads.sh per-host-density
        baseInterface=ens2f0 nicCount=20 ./run-workloads.sh nic-hotplug

AVAILABLE TESTS:
EOF
    for test_name in "${TEST_ORDER[@]}"; do
        local entry="${TEST_REGISTRY[$test_name]}"
        local rel_dir=$(parse_registry "$entry" "dir")
        printf "    %-24s %s\n" "$test_name" "$rel_dir"
    done
}

list_tests() {
    echo "Available VME Tests:"
    echo ""
    printf "%-24s %-40s %-8s\n" "TEST NAME" "DIRECTORY" "VARS EXT"
    echo "--------------------------------------------------------------------------------"
    for test_name in "${TEST_ORDER[@]}"; do
        local entry="${TEST_REGISTRY[$test_name]}"
        local rel_dir=$(parse_registry "$entry" "dir")
        local ext=$(parse_registry "$entry" "ext")
        printf "%-24s %-40s .%-7s\n" "$test_name" "$rel_dir" "$ext"
    done
    echo ""
    echo "Run a test:"
    echo "  ./run-workloads.sh <test-name>"
    echo "  ./run-workloads.sh <test-name> --mode sanity"
    echo ""
}

# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

main() {
    local tests_to_run=()
    local show_list=false
    local show_help_flag=false
    
    # Parse arguments
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --mode)
                MODE="$2"
                if [[ "$MODE" != "sanity" && "$MODE" != "full" ]]; then
                    logerr "Invalid mode: $MODE (must be 'sanity' or 'full')"
                    exit 1
                fi
                shift 2
                ;;
            --mode=*)
                MODE="${1#*=}"
                if [[ "$MODE" != "sanity" && "$MODE" != "full" ]]; then
                    logerr "Invalid mode: $MODE (must be 'sanity' or 'full')"
                    exit 1
                fi
                shift
                ;;
            --parallel)
                EXECUTION="parallel"
                shift
                ;;
            --sequential)
                EXECUTION="sequential"
                shift
                ;;
            --all)
                tests_to_run=("${TEST_ORDER[@]}")
                shift
                ;;
            --list)
                show_list=true
                shift
                ;;
            --help|-h)
                show_help_flag=true
                shift
                ;;
            --*)
                # Pass unknown flags to kube-burner
                KUBE_BURNER_ARGS+=("$1")
                shift
                ;;
            *)
                # Assume it's a test name
                if [[ -n "${TEST_REGISTRY[$1]}" ]]; then
                    tests_to_run+=("$1")
                else
                    logerr "Unknown test or option: $1"
                    logerr "Use --list to see available tests or --help for usage"
                    exit 1
                fi
                shift
                ;;
        esac
    done
    
    # Handle --help
    if [[ "$show_help_flag" == true ]]; then
        show_help
        exit 0
    fi
    
    # Handle --list
    if [[ "$show_list" == true ]]; then
        list_tests
        exit 0
    fi
    
    # Validate we have tests to run
    if [[ ${#tests_to_run[@]} -eq 0 ]]; then
        logerr "No tests specified"
        logerr "Use --all to run all tests, or specify test names"
        logerr "Use --list to see available tests"
        exit 1
    fi
    
    # Initialize main log
    MAIN_TIMESTAMP=$(date +%Y%m%d-%H%M%S)
    mkdir -p "$RESULTS_BASE"
    MAIN_LOG="${RESULTS_BASE}/vme-test-${MAIN_TIMESTAMP}.log"
    touch "$MAIN_LOG"
    
    logmain INFO "Starting VME Test Suite"
    logmain INFO "Mode: $MODE | Execution: $EXECUTION | Tests: ${tests_to_run[*]}"
    logmain INFO "Main log: $MAIN_LOG"
    
    # Run tests
    local exit_code=0
    if [[ ${#tests_to_run[@]} -eq 1 ]]; then
        # Single test - run directly
        run_single_test "${tests_to_run[0]}" || exit_code=$?
    else
        # Multiple tests
        if [[ "$EXECUTION" == "parallel" ]]; then
            run_tests_parallel "${tests_to_run[@]}" || exit_code=$?
        else
            run_tests_sequential "${tests_to_run[@]}" || exit_code=$?
        fi
        
        # Print summary for multi-test runs
        print_summary_table "${tests_to_run[@]}"
    fi
    
    logmain INFO "All tests completed"
    
    exit $exit_code
}

# Run main
main "$@"

