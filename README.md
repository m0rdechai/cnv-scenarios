# CNV Scenarios - Kube-Burner

This repository contains perf and scale qe scenarios for OpenShift Virtualization (CNV) workloads.

Some of the flows in this repository are inspired by, or in cases such as [virt-capacity-benchmark](https://kube-burner.github.io/kube-burner-ocp/latest/) derived from existing workflows in [kube-burner-ocp](https://github.com/kube-burner/kube-burner-ocp/tree/main) modified to meet regression aims for perf/scale qe.

## Requirements

Before running tests, ensure you have the following installed and configured:

| Requirement | Purpose | Installation |
|-------------|---------|--------------|
| **kube-burner** | Test execution engine | [kube-burner releases](https://github.com/kube-burner/kube-burner/releases) |
| **jq** | JSON processing for validation and summary | `dnf install jq` or `brew install jq` |
| **sshpass** | Password-based SSH (minimal-resources test) | `dnf install sshpass` |
| **oc** and **kubectl** | Kubernetes CLI | [OpenShift CLI](https://docs.openshift.com/container-platform/latest/cli_reference/openshift_cli/getting-started-cli.html) |
| **OpenShift Virtualization** | CNV operator | Installed on cluster |
| **Storage Class** | PVC provisioning | Configured (default: `ocs-storagecluster-ceph-rbd`) |
| **SSH Keys** | VM access validation | Paths in vars files must be valid and accessible |

**SSH Key Setup:**
```bash
# Generate keys if needed
ssh-keygen -t rsa -b 4096 -f ~/.ssh/id_rsa -N ""

# Update vars files with your paths
# privateKey: '/path/to/your/id_rsa'
# publicKey: '/path/to/your/id_rsa.pub'
```

## Quick Start

**run-workloads.sh** (unified runner):
- Located at `cnv-scenarios/run-workloads.sh`
- See help: `./run-workloads.sh -h`
- Runs multiple tests (parallel or sequential)
- Two modes:
    - For validating changes with minimal resources: `--mode sanity` will load (`vars-sanity.yml`)
    - For running full workloads: `--mode full` will load (`vars.yml`)
- Overwrite vars files via CLI: `nicCount=10 ./run-workloads.sh nic-hotplug --mode sanity`
- Aggregated results and summary
```bash
================================================================================
                           CNV Test Suite Summary
================================================================================
MODE: sanity | EXECUTION: parallel | TESTS: 10
MAIN LOG: /tmp/kube-burner-results/cnv-test-20251208-184724.log

TEST                     STATUS     VALIDATION   DURATION
--------------------------------------------------------------------------------
cpu-limits               PASS       SUCCESS      3m 10s
  Results: /tmp/kube-burner-results/cpu-limits/run-20251208-184724/
  Validation: /tmp/kube-burner-results/cpu-limits/run-20251208-184724/iteration-1/validation-cpu-limits.json

memory-limits            PASS       SUCCESS      2m 36s
  Results: /tmp/kube-burner-results/memory-limits/run-20251208-184724/
  Validation: /tmp/kube-burner-results/memory-limits/run-20251208-184724/iteration-1/validation-memory-limits.json

disk-limits              PASS       SUCCESS      2m 36s
  Results: /tmp/kube-burner-results/disk-limits/run-20251208-184724/
  Validation: /tmp/kube-burner-results/disk-limits/run-20251208-184724/iteration-1/validation-disk-limits.json
```

```bash
cd cnv-scenarios

# Run single test
./run-workloads.sh cpu-limits

# Run with sanity mode (minimal resources)
./run-workloads.sh cpu-limits --mode sanity

# Override variables via environment
cpuCores=8 ./run-workloads.sh cpu-limits --log-level=debug

# Run all tests in parallel
./run-workloads.sh --all --mode sanity --parallel

# Run multiple specific tests
./run-workloads.sh cpu-limits memory-limits disk-limits --mode full
```

Results are automatically saved to timestamped directories:
```
/tmp/kube-burner-results/<test-name>/run-YYYYMMDD-HHMMSS/
├── kube-burner.log                    # Full test execution log
└── iteration-1/
    ├── jobSummary.json                # Job execution summary
    ├── vmiLatencyMeasurement-*.json   # VM lifecycle timing metrics
    ├── validation-*.json              # Structured validation report
    └── validation.log                 # Human-readable validation log
```

### Available Tests

| Category | Test | Command |
|----------|------|---------|
| Resource Limits | cpu-limits | `./run-workloads.sh cpu-limits` |
| Resource Limits | memory-limits | `./run-workloads.sh memory-limits` |
| Resource Limits | disk-limits | `./run-workloads.sh disk-limits` |
| Hot-plug | disk-hotplug | `./run-workloads.sh disk-hotplug` |
| Hot-plug | nic-hotplug | `./run-workloads.sh nic-hotplug` |
| Performance | high-memory | `./run-workloads.sh high-memory` |
| Performance | large-disk | `./run-workloads.sh large-disk` |
| Performance | minimal-resources | `./run-workloads.sh minimal-resources` |
| Scale | per-host-density | `./run-workloads.sh per-host-density` |
| Scale | virt-capacity-benchmark | `./run-workloads.sh virt-capacity-benchmark` |

## Directory Structure

```
cnv-scenarios/
├── config/                           # Shared configuration files
│   ├── scripts/
│   │   ├── check.sh                  # Main validation script (~2200 lines, 11 functions)
│   │   ├── wrapper.sh                # Validation wrapper for logging
│   │   ├── cleanup-nncp.sh           # NNCP cleanup for nic-hotplug
│   │   └── detect-available-interface.sh  # Auto-detect NIC for hot-plug
│   ├── metrics-profiles/
│   │   └── kubevirt-metrics.yaml     # Standard KubeVirt metrics
│   └── templates/
│       └── dummy-configmap.yml       # Trigger object for beforeCleanup
├── run-workloads.sh                  # Unified test runner (sanity/full modes)
├── README.md                         # This file
├── ARCHITECTURE.md                   # Technical architecture documentation
├── ISSUES.md                         # Known issues and future work
├── scale-testing/                    # VM scaling and density tests
│   ├── per-host-density/             # VMs per host with single/multi-node modes
│   │   └── config/scripts/check.sh   # Percentage-based SSH validation
│   └── virt-capacity-benchmark/      # Comprehensive capacity testing
│       └── config/scripts/check.sh   # Percentage-based SSH + resize validation
├── resource-limits/                  # Resource boundary testing
│   ├── cpu-limits/                   # CPU core limit testing
│   ├── memory-limits/                # Memory limit testing
│   └── disk-limits/                  # Disk size limit testing
├── hot-plug/                         # Hot-plug functionality tests
│   ├── disk-hotplug/                 # Disk hot-plug testing
│   └── nic-hotplug/                  # NIC hot-plug testing
└── performance/                      # Performance validation tests
    ├── high-memory/                  # High memory allocation
    ├── large-disk/                   # Large disk performance
    └── minimal-resources/            # Minimal resource efficiency
```

> **For Contributors:** See [ARCHITECTURE.md](ARCHITECTURE.md) for detailed documentation on how `run-workloads.sh` and validation scripts work.

## Resource Limits Testing

### CPU Limits

Test CPU core allocations per VM with OS-level verification.

```bash
# Default settings
./run-workloads.sh cpu-limits

# Sanity mode (minimal resources)
./run-workloads.sh cpu-limits --mode sanity

# Test with 8 CPU cores
cpuCores=8 ./run-workloads.sh cpu-limits

# Test maximum CPU (32 cores)
cpuCores=32 ./run-workloads.sh cpu-limits

# Run cleanup (counter=0 triggers namespace cleanup)
counter=0 ./run-workloads.sh cpu-limits
```

**Test Phases:**
1. Create VM with specified CPU cores and cloud-init running stress-ng
2. Wait for VM to reach Running state
3. Validate CPU configuration via SSH

**Validations:**
- VM spec CPU cores match expected value
- Guest OS reports correct CPU count via `nproc`
- stress-ng processes running (one per core)

### Memory Limits

Test memory allocations per VM with OS-level verification.

```bash
# Default settings
./run-workloads.sh memory-limits

# Sanity mode
./run-workloads.sh memory-limits --mode sanity

# Test with 64GB memory
memorySize=64Gi ./run-workloads.sh memory-limits

# Test maximum memory (450GB)
memorySize=450Gi ./run-workloads.sh memory-limits
```

**Test Phases:**
1. Create VM with specified memory and cloud-init running stress-ng
2. Wait for VM to reach Running state
3. Validate memory configuration via SSH

**Validations:**
- VM spec memory matches expected value
- Guest OS reports correct memory via `free -m` (within 15% tolerance for OS overhead)
- stress-ng processes running

### Disk Limits

Test disk count and sizes per VM with OS-level verification.

```bash
# Default settings
./run-workloads.sh disk-limits

# Sanity mode
./run-workloads.sh disk-limits --mode sanity

# Test with 4 disks of 25Gi each
diskCount=4 diskSize=25Gi ./run-workloads.sh disk-limits

# Test with different storage class
diskCount=2 storageClassName=my-storage ./run-workloads.sh disk-limits
```

**Test Phases:**
1. Create VM with multiple DataVolumes attached
2. Wait for VM and all PVCs to be ready
3. Validate disk configuration via SSH

**Validations:**
- VM spec disk count matches expected
- DataVolume sizes match expected
- Guest OS shows correct disk count (excluding rootdisk, cloudinitdisk, zram)
- Guest OS disk sizes match expected (within 5% tolerance)

## Hot-plug Testing

### Disk Hot-plug

Test hot-plugging up to 256 disks per VM with automated mounting.

```bash
# Default settings
./run-workloads.sh disk-hotplug

# Sanity mode
./run-workloads.sh disk-hotplug --mode sanity

# Hot-plug 10 disks of 1Gi each
diskCount=10 pvcSize=1Gi ./run-workloads.sh disk-hotplug

# Skip OS-level validation for faster testing
diskCount=50 validateHotplugFromOs=false ./run-workloads.sh disk-hotplug
```

**Test Phases:**
1. Create VM and PVCs
2. Attach all hot-plug disks
3. Validate via SSH (optional)
4. Detach all disks

**Validations:**
- Hot-plugged disk count in VM spec
- PVC sizes match expected (configurable via `validatePvcBySize`)
- Guest OS disk visibility and sizes (configurable via `validateHotplugFromOs`)
- Mount points at `/mnt/disk1`, `/mnt/disk2`, etc.

### NIC Hot-plug

Test adding up to 28 network interfaces per VM. Creates two VMs with different network types to test both simple bridges and VLAN-tagged bridges.

```bash
# Default settings (auto-detects available interface)
./run-workloads.sh nic-hotplug

# Sanity mode
./run-workloads.sh nic-hotplug --mode sanity

# Test with 28 NICs
nicCount=28 ./run-workloads.sh nic-hotplug

# Test with specific base interface
nicCount=12 baseInterface=ens2f0 ./run-workloads.sh nic-hotplug

# Cleanup NNCPs after test (recommended)
cleanupNncp=true ./run-workloads.sh nic-hotplug
```

**Auto Interface Detection:**
- If `baseInterface` is not specified, `detect-available-interface.sh` auto-detects an unused physical interface
- Checks all worker nodes for an interface that:
  - Has no IP address assigned
  - Is not part of a bridge
  - Has no default route
- Shows "(auto-detected)" in output when detected automatically

**Test Phases:**
1. Create Simple Bridge NNCPs (using local Linux bridges)
2. Create VLAN Bridge NNCPs (using `baseInterface` with VLAN tags 101+)
3. Create NetworkAttachmentDefinitions for both network types
4. Create 2 VMs with specified NIC count each:
   - `multi-nic-simple-vm`: Uses simple Linux bridges (no physical interface dependency)
   - `multi-nic-vlan-vm`: Uses VLAN-tagged bridges on `baseInterface` (requires physical NIC)
5. Validate NIC counts on both VMs
6. Cleanup NNCPs (if `cleanupNncp=true`)

**Two VM Types Explained:**
- **Simple Bridge VMs**: Connect to software bridges created by NMState without requiring physical interfaces. Good for basic network isolation testing.
- **VLAN Bridge VMs**: Connect to bridges with VLAN tagging on a physical interface. Tests real network segmentation scenarios.

**Validations:**
- NodeNetworkConfigurationPolicy count matches expected (2 × nicCount)
- NetworkAttachmentDefinition count matches expected
- Total NIC count in VM spec matches expected for both VMs
- Guest OS network interface visibility (optional, via SSH)

## Scale Testing

### Per-Host Density

Test VM density with single-node or multi-node distribution modes, with one or multiple namespaces.

```bash
# Single-node mode (default) - all VMs on one node
# NOTE: If targetNode is not specified, auto-selects the first worker node
./run-workloads.sh per-host-density

# Sanity mode
./run-workloads.sh per-host-density --mode sanity

# Single-node with specific target
vmsPerNamespace=400 targetNode=worker001 ./run-workloads.sh per-host-density

# Multi-node mode - distribute across all workers
scaleMode=multi-node vmsPerNamespace=400 ./run-workloads.sh per-host-density

# Multiple namespaces, single node (800 VMs: 2 ns × 400)
namespaceCount=2 vmsPerNamespace=400 targetNode=worker001 ./run-workloads.sh per-host-density

# Multiple namespaces, multi-node (1200 VMs across workers)
scaleMode=multi-node namespaceCount=3 vmsPerNamespace=400 ./run-workloads.sh per-host-density

# Preserve namespaces for debugging (disable cleanup)
cleanup=false ./run-workloads.sh per-host-density --mode sanity

# Skip VM shutdown/restart phases (only test VM creation)
skipVmShutdown=true skipVmRestart=true ./run-workloads.sh per-host-density
```

**Scale Mode Options:**
- `scaleMode=single-node` (default): All VMs pinned to `targetNode`
  - If `targetNode` is not specified, the first worker node is auto-selected
  - Output shows "(auto-selected first worker)" when auto-detected
- `scaleMode=multi-node`: VMs distributed across all worker nodes using pod anti-affinity

**Namespace Configuration:**
- `namespaceCount=N`: Create N namespaces (default: 1)
- `vmsPerNamespace=N`: VMs per namespace (default: 450)
- Total VMs = `namespaceCount` × `vmsPerNamespace`

**Test Phases:**
1. Create VMs (running) with SSH secret
2. Validate running state + SSH accessibility
3. Shutdown all VMs (skipped if `skipVmShutdown=true`)
4. Validate shutdown state (skipped if `skipVmShutdown=true`)
5. Restart all VMs (skipped if `skipVmRestart=true`)
6. Validate running state + SSH accessibility (skipped if `skipVmRestart=true`)
7. Cleanup namespaces (skipped if `cleanup=false`)

**Validation Configuration:**
- `percentage_of_vms_to_validate=25`: Percentage of VMs randomly selected for SSH validation
  - Example: 400 VMs × 25% = 100 VMs validated via SSH
  - Set to `0` to disable SSH validation entirely
  - Set to `100` for full validation (slower)
- `max_ssh_retries=8`: Retry attempts at 15-second intervals (~2 minutes max wait)
- Node distribution reporting shows VM placement across workers
- Phase duration tracking for performance analysis


### Virt-Capacity-Benchmark

Note that this flow is a modified version of [virt-capacity-benchmark](https://kube-burner.github.io/kube-burner-ocp/latest/)
This modified version of the virt-capacity-benchmark preserves the same overall workflow, but adds targeted validations to support regression aims. Like most of the flows here, it is heavily inspired by, and in many cases derived from, workflows that originated in kube-burner-ocp.

Comprehensive capacity testing with volume resize, VM restart, snapshot, and migration operations. Includes percentage-based SSH validation and structured JSON reporting.

```bash
cd scale-testing/virt-capacity-benchmark

# Remove leftover namespaces first
oc delete ns -l 'kube-burner.io/test-name=virt-capacity-benchmark'

# Run full test
runTimestamp="run-$(date +%Y%m%d-%H%M%S)" kube-burner init --config=virt-capacity-benchmark.yml --user-data=vars.yml

# Run sanity test (reduced VMs for quick validation)
runTimestamp="run-$(date +%Y%m%d-%H%M%S)" kube-burner init --config=virt-capacity-benchmark.yml --user-data=vars-sanity.yml

# Via run-workloads.sh
cd ..
./run-workloads.sh --mode sanity --tests virt-capacity-benchmark
./run-workloads.sh --mode full --tests virt-capacity-benchmark
```

**Test Phases:**
1. Create VMs with SSH keys and root/data volumes
2. Validate running state + SSH accessibility (percentage-based)
3. Volume resize (if `skipResizeJob=false`)
4. Validate resize completion via SSH (`lsblk`)
5. Restart VMs
6. Validate running state + SSH accessibility
7. Create VM snapshots
8. Migrate VMs (if `skipMigrationJob=false`)
9. Final validation

**Validation Configuration:**
- `percentage_of_vms_to_validate=25`: Percentage of VMs randomly selected for SSH validation
- `max_ssh_retries=8`: Retry attempts at 15-second intervals
- `vmUser=fedora`: SSH user for Fedora-based VMs
- Phase duration tracking for performance analysis
- Node distribution reporting shows VM placement

**Sanity vs Full Mode:**
| Aspect | Sanity (`vars-sanity.yml`) | Full (`vars.yml`) |
|--------|---------------------------|-------------------|
| VM Count | 2 | 5+ |
| Root Volume | 10Gi | 20Gi |
| Data Volumes | 1 × 5Gi | 2 × 10Gi |
| SSH Validation | 100% | 25% |
| Migration/Resize | Skipped | Configurable |

**Validation Reports:**
Results include structured JSON reports:
- `validation-vm-running.json`: VM running state and SSH validation
- `validation-resize.json`: Volume resize verification (when enabled)
- `validation.log`: Human-readable validation log

## Performance Testing

### High Memory

Test performance with high memory allocation and validate guest OS sees the expected memory.

```bash
# Default settings
./run-workloads.sh high-memory

# Sanity mode
./run-workloads.sh high-memory --mode sanity

# Test with 450GB memory
highMemory=450Gi ./run-workloads.sh high-memory
```

**Test Phases:**
1. Create cloud-init secret with system configuration
2. Create VM with specified high memory allocation
3. Wait for VM to reach Running state
4. Validate memory configuration via SSH

**Validations:**
- VM spec memory matches expected value
- Guest OS reports correct memory via `free -m` (within 15% tolerance for OS overhead)
- VM responsiveness check via SSH uptime

### Large Disk

Test performance with very large disks and validate guest OS sees the expected disk size.

```bash
# Default settings
./run-workloads.sh large-disk

# Sanity mode
./run-workloads.sh large-disk --mode sanity

# Test with 100TB disk
largeDiskSize=100Ti ./run-workloads.sh large-disk
```

**Test Phases:**
1. Create cloud-init secret with disk configuration
2. Create VM with specified large disk (as additional DataVolume)
3. Wait for VM and PVC to be ready
4. Validate disk configuration via SSH

**Validations:**
- Large disk visible in guest OS via `lsblk`
- Disk size matches expected value (within 5% tolerance)
- VM responsiveness check via SSH uptime

### Minimal Resources

Test with minimal resource allocation using CirrOS VMs with password-based SSH authentication and PV-backed storage.

```bash
# Default settings
./run-workloads.sh minimal-resources

# Sanity mode
./run-workloads.sh minimal-resources --mode sanity

# Test with 10 VMs
vmCount=10 ./run-workloads.sh minimal-resources

# Adjust memory (CirrOS minimum ~128Mi)
minMemory=256Mi ./run-workloads.sh minimal-resources
```

**VM Configuration:**
- **Image**: CirrOS (lightweight ~44MB image)
- **Authentication**: Password-based SSH via cloud-init (password: `gocubsgo`)
- **Storage**: PV-backed disk via `dataVolumeTemplates` (tests minimal storage requirements)
- **Default Resources**: 100m CPU, 128Mi memory, 1Gi storage

**Test Phases:**
1. Create VM with minimal CPU, memory, and PV-backed storage
2. Wait for VM and PVC to reach Ready state
3. Validate system boot and responsiveness via password-based SSH

**Validations:**
- VM boots successfully with minimal resources
- PVC provisioned and attached
- Guest OS accessible via SSH (`sshpass` with `virtctl ssh`)
- System responsiveness via `uptime`
- Memory verification via `free -m`
- OS identity confirmation via `uname -a`

**Note:** Uses `sshpass` for password-based SSH (no SSH keys required). Ensure `sshpass` is installed on the test runner.

## Validation and Results

### Validation JSON Reports

All tests produce structured validation reports in JSON format:

```json
{
    "test_name": "vm-running",
    "status": "SUCCESS",
    "timestamp": "2025-11-27T19:41:44+02:00",
    "namespace": "all",
    "params": {
        "total_vms": 63,
        "running_vms": 63,
        "nodes_used": 6,
        "phase_duration_seconds": 69,
        "ssh_validation": {
            "enabled": true,
            "percentage_configured": 25,
            "max_retries_configured": 240,
            "vms_validated": 15,
            "vms_passed": 15,
            "vms_failed": 0,
            "duration_seconds": 66
        }
    },
    "validations": [
        {"phase": "vm_discovery", "status": "PASS", "message": "Found 63 VMs"},
        {"phase": "vm_running_state", "status": "PASS", "message": "63/63 VMs running"},
        {"phase": "ssh_validation", "status": "PASS", "message": "15/15 VMs SSH accessible"}
    ]
}
```

### Viewing Results

```bash
# View kube-burner log
cat /tmp/kube-burner-results/<test>/run-YYYYMMDD-HHMMSS/kube-burner.log

# View validation JSON
cat /tmp/kube-burner-results/<test>/run-YYYYMMDD-HHMMSS/iteration-*/validation*.json

# View validation log
cat /tmp/kube-burner-results/<test>/run-YYYYMMDD-HHMMSS/iteration-*/validation.log

# List all results
ls -lh /tmp/kube-burner-results/<test>/run-YYYYMMDD-HHMMSS/iteration-*/
```

### Validation Functions

All validation functions are wrapped by a retry mechanism (up to 130 retries with configurable wait times). See [ARCHITECTURE.md](ARCHITECTURE.md) for detailed implementation.

| Function | What It Validates | SSH Required | Notes |
|----------|------------------|--------------|-------|
| `check_vm_running` | VMs running, SSH accessible, node distribution | Yes (key-based) | Percentage-based validation, JSON reports |
| `check_vm_shutdown` | VMs in Stopped state | No | JSON reports |
| `check_cpu_limits` | CPU cores in spec + guest OS (`nproc`) + stress-ng | Yes (key-based) | Multi-phase validation |
| `check_memory_limits` | Memory in spec + guest OS (`free -m`) + stress-ng | Yes (key-based) | 15% tolerance for OS overhead |
| `check_disk_limits` | Disk count/size in spec + guest OS (`lsblk`) | Yes (key-based) | Multi-phase validation |
| `check_disk_hotplug` | Hot-plugged disks in spec + guest OS + mounts | Yes (configurable) | |
| `check_nic_hotplug` | NNCPs, NADs, NIC count, VM running, guest interfaces | Yes (optional) | 5-phase validation |
| `check_resize` | Volume resize via SSH (`lsblk`) root + data volumes | Yes (key-based) | JSON reports, per-host-density/virt-capacity |
| `check_high_memory` | High memory allocation + guest OS (`free -m`) | Yes (key-based) | 15% tolerance |
| `check_large_disk` | Large disk visibility + size in guest OS (`lsblk`) | Yes (key-based) | 4-phase validation |
| `check_performance_metrics` | System responsiveness (`uptime`, `free -m`, `uname`) | Yes (password-based) | For CirrOS VMs via sshpass |

## Advanced Usage

### Using run-workloads.sh

The unified `run-workloads.sh` script is the recommended way to run all tests:

```bash
# Single test
./run-workloads.sh cpu-limits

# With mode selection
./run-workloads.sh cpu-limits --mode sanity     # Uses vars-sanity.yml
./run-workloads.sh cpu-limits --mode full       # Uses vars.yml (default)

# Override variables
cpuCores=8 ./run-workloads.sh cpu-limits --log-level=debug

# Multiple tests
./run-workloads.sh cpu-limits memory-limits disk-limits

# All tests in parallel
./run-workloads.sh --all --parallel --mode sanity

# List available tests
./run-workloads.sh --list
```

> **For detailed architecture information**, see [ARCHITECTURE.md](ARCHITECTURE.md).

### Direct kube-burner Commands

For custom automation:

```bash
cd <test-directory>

# Generate timestamp manually
export runTimestamp="run-$(date +%Y%m%d-%H%M%S)"

# Create results directory
mkdir -p "/tmp/kube-burner-results/<test>/${runTimestamp}"

# Run kube-burner directly
kube-burner init \
  --config=<test>.yml \
  --user-data=vars.yml \
  --log-level=debug \
  2>&1 | tee "/tmp/kube-burner-results/<test>/${runTimestamp}/kube-burner.log"
```

### Configuration Variables

All tests use `vars.yml` for configuration. Override via environment:

```bash
# Environment variables override vars.yml values
cpuCores=32 storageClassName=my-storage ./run-workloads.sh cpu-limits
```

**Common Parameters:**
- `storageClassName`: Storage class (default: `ocs-storagecluster-ceph-rbd`)
- `nodeSelector`: Node selector for VM placement
- `counter`: Test iteration counter (`0` triggers cleanup)
- `maxWaitTimeout`: Maximum resource wait time
- `resultsPath`: Base directory for results

### Prometheus Monitoring

Enable Prometheus metrics collection:

```bash
export PROM="https://$(oc get route -n openshift-monitoring prometheus-k8s -o jsonpath='{.spec.host}')"
export PROM_TOKEN="$(oc create token -n openshift-monitoring prometheus-k8s)"

./run-workloads.sh cpu-limits
```

### SSH Validation Configuration

For tests requiring SSH validation:

```yaml
# In vars.yml - Key-based authentication (most tests)
privateKey: '/path/to/id_rsa'           # SSH private key
vmUser: 'fedora'                        # VM user (fedora for Fedora, alpine for Alpine)
percentage_of_vms_to_validate: 25       # Percentage to validate (0 = disabled)
max_ssh_retries: 240                    # Max retries (15s interval)
```

**Password-based authentication** (minimal-resources test with CirrOS):
```yaml
# In vars.yml - Password authentication
vmUser: 'cirros'                        # CirrOS default user
vmPassword: 'gocubsgo'                  # Set via cloud-init in VM template
```

Note: Password-based SSH uses `sshpass` with `virtctl ssh`. Ensure `sshpass` is installed.

## Sanity and Full Testing with run-workloads.sh

The unified `run-workloads.sh` script supports both quick sanity tests and full regression tests.

### Overview

The test runner supports two modes:
- **Sanity mode** (`--mode sanity`): Uses `vars-sanity.yml` for quick validation
- **Full mode** (`--mode full`): Uses `vars.yml` for production regression testing

Sanity tests use minimal configurations:
- **Minimal resources**: 1-2 VMs, 1 CPU core, 512Mi-1Gi memory
- **Reduced timeouts**: 5m vs 30m
- **Monitoring disabled**: No Elasticsearch/Prometheus
- **Unique namespaces**: Timestamped for isolation

### Quick Start

```bash
cd cnv-scenarios

# Run all sanity tests in parallel (fastest)
./run-workloads.sh --all --mode sanity --parallel

# Run all full tests (production configs)
./run-workloads.sh --all --mode full --parallel

# Run specific tests
./run-workloads.sh cpu-limits disk-hotplug --mode sanity

# Sequential mode for debugging
./run-workloads.sh disk-limits --mode sanity
```

### Using Makefile Targets

```bash
# Run all tests in parallel (fastest)
make test-all-parallel

# Run all tests sequentially (safer)
make test-all-sequential

# Run specific test groups
make test-limits      # cpu, memory, disk limits
make test-hotplug     # disk and nic hot-plug
make test-performance # minimal, large-disk, high-memory
make test-scale       # per-host-density, virt-capacity-benchmark

# Run individual tests
make test-cpu-limits
make test-disk-hotplug

# Cleanup
make clean-sanity
```

### Sanity vs Full Tests

| Aspect | Sanity (`--mode sanity`) | Full (`--mode full`) |
|--------|--------------------------|----------------------|
| Config File | `vars-sanity.yml` | `vars.yml` |
| Resources | Minimal (1 VM, 1 CPU) | Full (configurable) |
| Timeout | 5 minutes | 30+ minutes |
| Monitoring | Disabled | Configurable |
| Results Path | `/tmp/kube-burner-results/sanity-*` | `/tmp/kube-burner-results/full-*` |
| Purpose | Quick validation | Full regression |

### Available Tests

```
cpu-limits, memory-limits, disk-limits
disk-hotplug, nic-hotplug
minimal-resources, large-disk, high-memory
per-host-density, virt-capacity-benchmark
```

## Cleanup

```bash
# Delete test namespace
oc delete namespace <test-namespace>

# Delete by test label
oc delete ns -l 'kube-burner.io/test-name=<test-name>'

# Using counter=0 (triggers cleanup job)
counter=0 ./run-workloads.sh cpu-limits

# Per-host-density: disable cleanup to preserve namespaces
cleanup=false ./run-workloads.sh per-host-density --mode sanity

# Per-host-density: manually cleanup after inspection
oc delete ns -l 'kube-burner.io/test-name=per-host-density'
```

## Troubleshooting

### Common Issues

1. **Storage provisioning timeouts**: Increase `maxWaitTimeout` or reduce VM count
2. **SSH validation failures**: Check `privateKey` and `vmUser` match VM image
3. **Resource limits**: Ensure cluster has sufficient CPU/memory
4. **Network policies**: Verify connectivity for multi-NIC tests
5. **Image pull failures**: Check registry access and image URLs

### Variable Case Sensitivity

Environment variables are **case-sensitive**:

```bash
# CORRECT
cpuCores=8 ./run-workloads.sh cpu-limits
vmsPerNamespace=100 ./run-workloads.sh per-host-density

# WRONG - will be ignored
CPUCORES=8 ./run-workloads.sh cpu-limits
vmspernamespace=100 ./run-workloads.sh per-host-density
```

### Debug Logging

```bash
# Enable debug output
./run-workloads.sh cpu-limits --log-level=debug

# Watch VM creation
oc get vms -n <namespace> --watch

# Check VM instance status
oc get vmis -n <namespace>
```

## Test Matrix Summary

| Category | Scenario | Config File | Key Parameters |
|----------|----------|-------------|----------------|
| Resource Limits | CPU | cpu-limits-test.yml | `cpuCores=32` |
| Resource Limits | Memory | memory-limits-test.yml | `memorySize=450Gi` |
| Resource Limits | Disk | disk-limits-test.yml | `diskCount=4 diskSize=100Gi` |
| Hot-plug | Disks | disk-hotplug-test.yml | `diskCount=256 pvcSize=1Gi` |
| Hot-plug | NICs | nic-hotplug-test.yml | `nicCount=28` |
| Scale | Per-Host | per-host-density.yml | `vmsPerNamespace=460 scaleMode=single-node cleanup=true` |
| Scale | Capacity | virt-capacity-benchmark.yml | `vmCount=5 percentage_of_vms_to_validate=25` |
| Performance | Large Disk | large-disk-performance.yml | `largeDiskSize=100Ti` |
| Performance | High Memory | high-memory-performance.yml | `highMemory=450Gi` |
| Performance | Minimal | minimal-resources-test.yml | `minMemory=128Mi minCpu=100m minStorage=1Gi` |
