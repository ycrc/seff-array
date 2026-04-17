#!/usr/bin/env python3

import argparse
import base64
import gzip
import json
import os
import subprocess
import sys
from io import StringIO
from typing import Optional

import numpy as np
import pandas as pd
from rich import box
from rich.console import Console
from rich.rule import Rule
from rich.table import Table

from seff_array import __version__

console = Console()

# Slurm states that represent a finished job
FINISHED_STATES = {"COMPLETED", "FAILED", "OUT_OF_MEMORY", "TIMEOUT", "PREEMPTED"}


def time_to_float(time):
    """Convert a Slurm time string [dd-[hh:]]mm:ss to seconds.

    Special values "UNLIMITED" and "Partition_Limit" are treated as one year.
    """
    # fillna(0.) produces floats; pass them through unchanged
    if isinstance(time, float):
        return time

    # sacct reports these when no wall-time limit is set
    if time in ("UNLIMITED", "Partition_Limit"):
        return 365 * 86400

    days, hours = 0, 0

    if "-" in time:
        days = int(time.split("-")[0]) * 86400
        time = time.split("-")[1]

    parts = time.split(":")

    if len(parts) == 3:
        hours = int(parts[0]) * 3600
        mins = int(parts[1]) * 60
        secs = float(parts[2])
    elif len(parts) == 2:
        mins = int(parts[0]) * 60
        secs = float(parts[1])
    else:
        # Single bare-seconds value (shouldn't appear in practice)
        secs = float(parts[0])
        mins = 0

    return days + hours + mins + secs


def get_stats_dict(ss64: Optional[str]) -> dict:
    """Decode a Princeton jobstats AdminComment payload to a dict.

    The field is base64-encoded gzip-compressed JSON. Returns an empty dict
    if the value is absent, too short, or flagged as unavailable by jobstats.
    """
    if not ss64 or not isinstance(ss64, str) or len(ss64) < 10:
        return {}
    if ss64.startswith(("JS1:Short", "JS1:None")):
        return {}
    try:
        return json.loads(gzip.decompress(base64.b64decode(ss64[4:])))
    except Exception:
        return {}


def gpu_count(js: dict) -> int:
    """Return the total number of GPUs used across all nodes in a jobstats dict."""
    total = 0
    for node in js.get("nodes", {}).values():
        total += len(node.get("gpu_utilization", {}))
    return total


def gpu_util(js: dict) -> float:
    """Return the mean GPU utilization (0–100) across all GPUs in a jobstats dict."""
    utils = []
    for node in js.get("nodes", {}).values():
        utils.extend(node.get("gpu_utilization", {}).values())
    return float(np.mean(utils)) if utils else 0.0


def is_finished(state):
    """Return True for any finished state, including 'CANCELLED by <uid>'."""
    return state in FINISHED_STATES or str(state).startswith("CANCELLED")


def make_histogram_table(values, title, unit="%", bins=10, vmin=0, vmax=100):
    """Return a rich Table containing a horizontal histogram."""
    h, bin_edges = np.histogram(values, bins=np.linspace(vmin, vmax, num=bins + 1))
    max_count = max(h) if max(h) > 0 else 1
    bar_width = 30

    table = Table(
        title=title,
        box=box.SIMPLE,
        show_header=True,
        title_style="bold cyan",
        padding=(0, 1),
    )
    table.add_column(f"Range ({unit})", style="cyan", justify="right", min_width=12)
    table.add_column("Count", style="yellow", justify="right", min_width=6)
    table.add_column("Distribution", style="green", min_width=bar_width + 2)

    for i, count in enumerate(h):
        range_str = f"{bin_edges[i]:.0f}\u2013{bin_edges[i+1]:.0f}"
        bar = "\u2588" * int(count / max_count * bar_width)
        table.add_row(range_str, str(count), bar)

    return table


def run_sacct(fmt, job_id, cluster_flag, aggregate):
    """`aggregate=True` adds -X (one row per job, not per step)."""
    aggregate_flag = "-X" if aggregate else ""
    cmd = f"sacct {aggregate_flag} --units=G -P --format={fmt} -j {job_id} {cluster_flag}"
    try:
        result = subprocess.check_output(cmd, shell=True, stderr=subprocess.PIPE)
    except subprocess.CalledProcessError as e:
        console.print(f"[bold red]Error running sacct:[/bold red] {e.stderr.decode().strip()}")
        sys.exit(1)
    return pd.read_csv(StringIO(result.decode("utf-8")), sep="|")


def job_eff(job_id, cluster=None):
    # Prefer explicit argument; fall back to the Slurm env var
    if cluster is None:
        cluster = os.getenv("SLURM_CLUSTER_NAME")

    cluster_flag = f"--cluster {cluster}" if cluster else ""

    fmt_short = "JobID,JobName,Elapsed,ReqMem,ReqCPUS,Timelimit,State,TotalCPU,NNodes,User,Group,Cluster,AdminComment"
    fmt_long = "JobID,JobName,Elapsed,ReqMem,ReqCPUS,Timelimit,State,TotalCPU,NNodes,User,Group,Cluster,MaxRSS,AdminComment"

    df_short = run_sacct(fmt_short, job_id, cluster_flag, aggregate=True)
    df_long = run_sacct(fmt_long, job_id, cluster_flag, aggregate=False)

    # Check that at least one job has finished before doing any work
    if not df_long["State"].apply(is_finished).any():
        console.print(f"[yellow]No finished jobs found for {job_id}.[/yellow]")
        return

    # --- Data cleaning ---
    df_short = df_short.fillna(0.0)
    df_long = df_long.fillna(0.0)

    # Strip job-step suffixes (e.g. "12345.batch" → "12345")
    df_long["JobID"] = df_long["JobID"].astype(str).str.split(".").str[0]

    # Parse memory (sacct appends "G" when --units=G is used)
    df_long["MaxRSS"] = (
        pd.to_numeric(
            df_long["MaxRSS"].astype(str).str.replace("G", "", regex=False),
            errors="coerce",
        ).fillna(0.0)
    )
    df_long["ReqMem"] = (
        pd.to_numeric(
            df_long["ReqMem"].astype(str).str.replace("G", "", regex=False),
            errors="coerce",
        ).fillna(0.0)
    )

    df_long["TotalCPU"] = df_long["TotalCPU"].map(time_to_float)
    df_long["Elapsed"] = df_long["Elapsed"].map(time_to_float)
    df_long["Timelimit"] = df_long["Timelimit"].map(time_to_float)

    # --- Job metadata from the first row of the aggregate query ---
    first = df_short.iloc[0]
    job_id_str = str(first["JobID"])
    is_array_job = "_" in job_id_str
    base_job_id = job_id_str.split("_")[0]

    job_name = first["JobName"]
    cluster_name = first["Cluster"]
    user = first["User"]
    group = first["Group"]
    nodes = first["NNodes"]
    cores = first["ReqCPUS"]
    req_mem_raw = str(first["ReqMem"])   # e.g. "4G"
    req_time_raw = str(first["Timelimit"])  # e.g. "01:00:00"

    # --- Job info ---
    console.print(Rule("[bold]Job Information[/bold]", style="blue"))
    info_table = Table(box=None, show_header=False, padding=(0, 1))
    info_table.add_column(style="bold cyan", justify="right")
    info_table.add_column(style="white")
    info_table.add_row("Job ID:", base_job_id)
    info_table.add_row("Name:", str(job_name))
    info_table.add_row("Cluster:", str(cluster_name))
    info_table.add_row("User / Group:", f"{user} / {group}")
    info_table.add_row("Requested CPUs:", f"{cores} core(s) on {nodes} node(s)")
    info_table.add_row("Requested Memory:", req_mem_raw)
    info_table.add_row("Requested Time:", req_time_raw)
    console.print(info_table)

    # --- Job status ---
    console.print(Rule("[bold]Job Status[/bold]", style="blue"))
    status_table = Table(box=box.SIMPLE, show_header=True, padding=(0, 1))
    status_table.add_column("State", style="bold")
    status_table.add_column("Count", style="yellow", justify="right")
    for state, count in df_short["State"].value_counts().items():
        if state == "COMPLETED":
            color = "green"
        elif state in ("FAILED", "OUT_OF_MEMORY"):
            color = "red"
        elif str(state).startswith("CANCELLED"):
            color = "magenta"
        else:
            color = "yellow"
        status_table.add_row(f"[{color}]{state}[/{color}]", str(count))
    console.print(status_table)

    # --- Efficiency statistics for finished jobs ---
    mask = df_long["State"].apply(is_finished)
    df_finished = df_long[mask]

    cpu_use = df_finished.groupby("JobID")["TotalCPU"].max()
    time_use = df_finished.groupby("JobID")["Elapsed"].max()
    mem_use = df_finished.groupby("JobID")["MaxRSS"].max()

    req_time_secs = time_to_float(req_time_raw)
    req_mem_gb = float(req_mem_raw.replace("G", "")) if req_mem_raw.replace("G", "").replace(".", "").isdigit() else 0.0

    cpu_arr = cpu_use.to_numpy()
    time_arr = time_use.to_numpy()
    mem_arr = mem_use.to_numpy()
    n_cores = float(cores) if float(cores) > 0 else 1.0

    # CPU efficiency: (cpu time used) / (elapsed * cores)
    with np.errstate(divide="ignore", invalid="ignore"):
        cpu_eff = np.where(time_arr > 0, cpu_arr / (time_arr * n_cores), 0.0)
        mem_eff = np.where(req_mem_gb > 0, mem_arr / req_mem_gb, 0.0)
        time_eff = np.where(req_time_secs > 0, time_arr / req_time_secs, 0.0)

    # --- GPU stats from Princeton jobstats (AdminComment field) ---
    # Parse the AdminComment from the aggregate query (one row per array task).
    # Each row's AdminComment holds jobstats JSON for that task.
    gpu_counts = []
    gpu_utils = []
    for ac in df_short["AdminComment"]:
        js = get_stats_dict(ac)
        if js:
            n_gpus = gpu_count(js)
            if n_gpus > 0:
                gpu_counts.append(n_gpus)
                gpu_utils.append(gpu_util(js))

    has_gpu_data = len(gpu_counts) > 0

    # --- Statistics ---
    console.print(Rule("[bold]Finished Job Statistics[/bold] [dim](excludes pending, running, and cancelled)[/dim]", style="blue"))
    stats_table = Table(box=None, show_header=False, padding=(0, 1))
    stats_table.add_column(style="bold cyan", justify="right")
    stats_table.add_column(style="white")
    stats_table.add_row("Jobs analyzed:", str(len(cpu_use)))
    stats_table.add_row("Avg CPU Efficiency:", f"{cpu_eff.mean() * 100:.1f}%")
    stats_table.add_row("Avg Memory Usage:", f"{mem_arr.mean():.2f} GB")
    stats_table.add_row("Avg Runtime:", f"{time_arr.mean():.0f}s")
    if has_gpu_data:
        stats_table.add_row("Avg Requested GPUs:", f"{np.mean(gpu_counts):.1f}")
        stats_table.add_row("Avg GPU Efficiency:", f"{np.mean(gpu_utils):.1f}%")
    console.print(stats_table)

    # Histograms are only meaningful for array jobs with multiple tasks
    if is_array_job:
        console.print(make_histogram_table(cpu_eff * 100, "CPU Efficiency"))
        if has_gpu_data:
            console.print(make_histogram_table(gpu_utils, "GPU Efficiency"))
        console.print(make_histogram_table(mem_eff * 100, "Memory Efficiency"))
        console.print(make_histogram_table(time_eff * 100, "Time Efficiency"))


def main():
    parser = argparse.ArgumentParser(
        prog="seff-array",
        description=(
            "seff-array v{ver} — job efficiency report for Slurm jobs and job arrays.\n"
            "https://github.com/ycrc/seff-array\n"
            "\n"
            "Queries sacct and reports CPU, memory, and wall-time efficiency.\n"
            "For job arrays, per-task histograms are shown for each metric.\n"
            "For single jobs, a summary similar to `seff` is produced.\n"
            "\n"
            "If Princeton jobstats data is present in the AdminComment field,\n"
            "GPU utilization statistics and a histogram are also displayed.\n"
            "\n"
            "Examples:\n"
            "  seff-array 12345678          # single job or array\n"
            "  seff-array 12345678_42        # specific array task\n"
            "  seff-array 12345678 -c grace  # specify cluster explicitly\n"
        ).format(ver=__version__),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("jobid", help="Slurm job ID or array ID")
    parser.add_argument(
        "-c",
        "--cluster",
        dest="cluster",
        default=None,
        metavar="NAME",
        help=(
            "Slurm cluster name to pass to sacct. "
            "Defaults to the SLURM_CLUSTER_NAME environment variable if set."
        ),
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
    )
    args = parser.parse_args()

    job_eff(args.jobid, args.cluster)


if __name__ == "__main__":
    main()
