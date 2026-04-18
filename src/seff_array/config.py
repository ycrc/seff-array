# Site-specific configuration for the Prometheus backend.
#
# If your Prometheus uses different metric names or label keys, edit this file.
# Everything else (query logic, fallback handling, CLI) lives in cli.py.

# --- Prometheus metric names ---

# Cumulative CPU time for a job cgroup on a single node (seconds)
CPU_METRIC = "cgroup_cpu_total_seconds"

# Resident set size for a job cgroup on a single node (bytes)
MEM_METRIC = "cgroup_memory_rss_bytes"

# Gauge whose *value* is the numeric Slurm job ID currently using each GPU
GPU_JOBID_METRIC = "nvidia_gpu_jobId"

# GPU duty cycle / utilization (percent, 0-100)
GPU_UTIL_METRIC = "nvidia_gpu_duty_cycle"

# --- Prometheus connection ---

# Default base URL for the Prometheus server.
# Can be overridden at runtime with --prometheus or the SEFF_ARRAY_PROM_URL
# environment variable (both take precedence over this value).
PROMETHEUS_URL = None

# --- Prometheus label names ---

# Label that identifies the Slurm cluster (must match sacct's Cluster field)
LABEL_CLUSTER = "cluster"

# Label that carries the numeric Slurm job ID on cgroup metrics
LABEL_JOBID = "jobid"

# Labels that together uniquely identify a physical GPU device
LABEL_INSTANCE = "instance"
LABEL_MINOR = "minor_number"
