#!/bin/bash
set -eo pipefail

# Debugging: Print how script was invoked and where it thinks it is
echo "Wrapper: \$0 is '$0'"
echo "Wrapper: PWD is '$(pwd)'"

# Try to resolve directory using BASH_SOURCE, handling unbound variable case
# We use ${BASH_SOURCE[0]:-} to avoid 'unbound variable' error if set -u is on (though we turned it off above)
SCRIPT_DIR=""
if [ -n "${BASH_SOURCE[0]:-}" ]; then
    SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
fi

echo "Wrapper: Resolved SCRIPT_DIR to '${SCRIPT_DIR:-}'"

# Fallback logic if automatic detection failed or check.sh isn't there
CHECK_SCRIPT="${SCRIPT_DIR}/check.sh"

if [ -z "${SCRIPT_DIR}" ] || [ ! -f "${CHECK_SCRIPT}" ]; then
    echo "Wrapper: check.sh not found at resolved path. Trying fallback relative path..."
    # Fallback for standard project structure: ../../config/scripts/check.sh
    FALLBACK_PATH="../../config/scripts/check.sh"
    if [ -f "${FALLBACK_PATH}" ]; then
        CHECK_SCRIPT="${FALLBACK_PATH}"
        echo "Wrapper: Found check.sh at fallback path: ${CHECK_SCRIPT}"
    else
        echo "Wrapper ERROR: Could not find check.sh at resolved path '${SCRIPT_DIR}' or fallback '${FALLBACK_PATH}'"
        echo "Wrapper: Directory listing of ../../config/scripts/:"
        ls -l ../../config/scripts/ || true
        exit 1
    fi
fi

# The last argument is the results directory - use it as-is
# The directory path already contains the timestamp from template expansion

# Get all arguments into an array
args=("$@")
# Get the number of arguments
num_args=${#args[@]}
# Get the last argument (results directory)
# Determine results_dir: if last arg looks like a guest OS, use second-to-last
last_arg="${args[$((num_args-1))]}"
if [[ "$last_arg" == "linux" || "$last_arg" == "windows" ]]; then
    results_dir="${args[$((num_args-2))]}"
else
    results_dir="${args[$((num_args-1))]}"
fi

# Ensure the results directory exists (may already exist from kube-burner)
mkdir -p "${results_dir}"

# Log file path
log_file="${results_dir}/validation.log"

echo "Wrapper: Executing validation with results in ${results_dir}"

# Execute check.sh using the resolved path
# Pipe output to tee to show in stdout AND save to file
"${CHECK_SCRIPT}" "${args[@]}" 2>&1 | tee "${log_file}"

# Capture exit code of check.sh (pipestatus[0]) to return correctly
exit ${PIPESTATUS[0]}
