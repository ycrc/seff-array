# seff-array

An extension of the Slurm `seff` command for job arrays, with histogram
reporting and optional Prometheus-backed metrics.

## Features

- Per-task histograms for CPU, memory, and time efficiency
- Color-coded bars by job state (green=COMPLETED, yellow=TIMEOUT, red=FAILED, magenta=CANCELLED, blue=RUNNING)
- GPU efficiency histograms when GPU data is available
- Optional Prometheus backend for accurate cgroup and GPU metrics
- Live metrics for in-progress jobs when Prometheus is configured

## Installation

```bash
git clone https://github.com/ycrc/seff-array
cd seff-array
pip install .
```

With [uv](https://github.com/astral-sh/uv):

```bash
git clone https://github.com/ycrc/seff-array
cd seff-array
uv pip install .
```

## Usage

```
seff-array [-h] [-c NAME] [--prometheus URL] [--version] jobid
```

```bash
seff-array 12345678                   # single job or array
seff-array 12345678_42                # specific array task
seff-array 12345678 -c grace          # specify cluster
seff-array 12345678 --prometheus http://prometheus:9090
```

For job arrays, per-task histograms are shown for CPU, memory, time, and
(when available) GPU efficiency. For single jobs, a summary similar to
`seff` is produced.

If run on a cluster that shares a single Slurm database, pass the cluster
name via `-c`. The `SLURM_CLUSTER_NAME` environment variable is used as a
default if set.

## Metrics source priority

seff-array tries each source in order, using the first that returns data:

| Priority | Source | Notes |
|----------|--------|-------|
| 1 | **Prometheus** | Accurate cgroup CPU/memory + GPU duty cycle. Requires `--prometheus`. |
| 2 | **jobstats AdminComment** | GPU data from [Princeton jobstats](https://github.com/PrincetonUniversity/jobstats) `store_jobstats`. Used at sites that populate the Slurm `AdminComment` field. |
| 3 | **sacct** | `TotalCPU` and `MaxRSS` from Slurm accounting. Always available but can be unreliable, especially for GPU jobs. |

The statistics panel shows a **Metrics source** row when `--prometheus` is
passed, indicating how many tasks were served from each backend.

## Prometheus backend

When `--prometheus` is set, seff-array queries your Prometheus server
directly for cgroup and NVIDIA GPU metrics. This is more reliable than
sacct, particularly for GPU jobs where sacct fields are often missing.

Expected metrics:

| Metric | Labels | Description |
|--------|--------|-------------|
| `cgroup_cpu_total_seconds` | `cluster`, `jobid` | Cumulative CPU time per job per node |
| `cgroup_memory_rss_bytes` | `cluster`, `jobid` | RSS memory per job per node |
| `nvidia_gpu_jobId` | `cluster`, `instance`, `minor_number` | Job ID currently occupying each GPU |
| `nvidia_gpu_duty_cycle` | `cluster`, `instance`, `minor_number` | GPU utilization (%) |

Set the base URL via flag or environment variable:

```bash
export SEFF_ARRAY_PROM_URL=http://monitor1.mycluster.example.com:9090
seff-array 12345678
```

Prometheus data is retained for a configurable window (typically 2 weeks).
Jobs older than the retention period fall back to sacct automatically.

Running jobs are included in the output when Prometheus is configured,
showing live CPU, memory, and GPU metrics alongside finished tasks.

### Adapting to other sites

Metric names and label keys are defined in `src/seff_array/config.py`.
If your Prometheus uses different names, edit that file — the query logic
in `cli.py` does not need to change.

This tool has been developed and tested at the
[Yale Center for Research Computing](https://research.computing.yale.edu)
with a [cgroup exporter](https://github.com/treydock/cgroup_exporter) and
the [NVIDIA GPU exporter](https://github.com/utkuozdemir/nvidia_gpu_exporter).

## Troubleshooting

Use `--debug` to print the raw PromQL queries and Prometheus responses:

```bash
seff-array 12345678 --prometheus http://prometheus:9090 --debug
```
