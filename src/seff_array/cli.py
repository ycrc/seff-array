#!/usr/bin/env python3

import argparse
import base64
import gzip
import json
import os
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
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

# Maximum Prometheus lookback window (13 days keeps us inside typical 2-week retention)
MAX_PROM_LOOKBACK = 13 * 86400


def time_to_float(time_str):
    """Convert a Slurm time string [dd-[hh:]]mm:ss to seconds.

    Special values "UNLIMITED" and "Partition_Limit" are treated as one year.
    """
    # fillna(0.) produces floats; pass them through unchanged
    if isinstance(time_str, float):
        return time_str

    # sacct reports these when no wall-time limit is set
    if time_str in ("UNLIMITED", "Partition_Limit"):
        return 365 * 86400

    days, hours = 0, 0

    if "-" in time_str:
        days = int(time_str.split("-")[0]) * 86400
        time_str = time_str.split("-")[1]

    parts = time_str.split(":")

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


def parse_timestamp(s) -> float:
    """Convert a sacct datetime string (e.g. '2024-01-15T10:23:45') to a unix timestamp.

    Returns the current time for unknown/missing values (e.g. still-running jobs).
    """
    if isinstance(s, float) or str(s) in ("0.0", "Unknown", "None", ""):
        return time.time()
    try:
        return datetime.fromisoformat(str(s)).timestamp()
    except ValueError:
        return time.time()


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
    """Return the mean GPU utilization (0-100) across all GPUs in a jobstats dict."""
    utils = []
    for node in js.get("nodes", {}).values():
        utils.extend(node.get("gpu_utilization", {}).values())
    return float(np.mean(utils)) if utils else 0.0


def finished_mask(state_series: pd.Series) -> pd.Series:
    """Return a boolean mask for rows in a finished state.

    Handles both exact matches (COMPLETED, FAILED, …) and the
    'CANCELLED by <uid>' variant that sacct sometimes emits.
    """
    return state_series.isin(FINISHED_STATES) | state_series.str.startswith("CANCELLED")


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
        range_str = f"{bin_edges[i]:.0f}\u2013{bin_edges[i + 1]:.0f}"
        bar = "\u2588" * int(count / max_count * bar_width)
        table.add_row(range_str, str(count), bar)

    return table


def run_sacct(fmt, job_id, cluster_flag, aggregate):
    """`aggregate=True` adds -X (one row per job, not per step)."""
    aggregate_flag = "-X" if aggregate else ""
    cmd = (
        f"sacct {aggregate_flag} --units=G -P --format={fmt} -j {job_id} {cluster_flag}"
    )
    try:
        result = subprocess.check_output(cmd, shell=True, stderr=subprocess.PIPE)
    except subprocess.CalledProcessError as e:
        console.print(
            f"[bold red]Error running sacct:[/bold red] {e.stderr.decode().strip()}"
        )
        sys.exit(1)
    return pd.read_csv(StringIO(result.decode("utf-8")), sep="|")


def query_prometheus(
    fin_short: pd.DataFrame, cluster: str, prom_url: str, debug: bool = False
) -> dict:
    """Query Prometheus for CPU, memory, and GPU metrics for a set of finished tasks.

    Uses a single bulk regex query per metric so the number of HTTP requests is
    constant (4) regardless of array size.

    Returns a dict mapping JobIDRaw (str) to a dict of:
        cpu_seconds  float  – total CPU time consumed
        rss_gb       float  – peak RSS memory in GB
        gpu_util     float  – mean GPU duty cycle 0-100 (only if GPU data found)
        gpu_count    int    – number of GPUs allocated (only if GPU data found)

    Returns {} on any error so callers fall back gracefully.

    GPU note: this implementation expects the NVIDIA exporter to expose
    `nvidia_gpu_jobId` as a gauge metric and `nvidia_gpu_duty_cycle` as a
    gauge, both labeled with `instance` and `minor_number`. Adjust the
    gpu_alloc / gpu_util queries below if your exporter uses a different schema.
    """
    if fin_short.empty:
        return {}

    raw_ids = set(fin_short["JobIDRaw"].astype(str).tolist())
    starts = fin_short["Start"].map(parse_timestamp).tolist()
    ends = fin_short["End"].map(parse_timestamp).tolist()

    query_time = int(max(ends))
    lookback = min(int(max(ends) - min(starts)), MAX_PROM_LOOKBACK)
    if lookback < 60:
        # Window too small to be meaningful
        return {}

    # For GPU, query at the midpoint of the job window rather than the end.
    # nvidia_gpu_jobId is a gauge that shows the current allocation — querying
    # at max(end) sees GPUs that may already be reassigned to newer jobs.
    gpu_query_time = int((min(starts) + max(ends)) / 2)

    id_regex = "|".join(raw_ids)
    url = f"{prom_url.rstrip('/')}/api/v1/query"

    if debug:
        console.print(
            Rule("[bold yellow]Prometheus debug[/bold yellow]", style="yellow")
        )
        console.print(f"[yellow]URL:[/yellow] {url}")
        console.print(
            f"[yellow]query_time:[/yellow] {query_time}  "
            f"[yellow]gpu_query_time:[/yellow] {gpu_query_time}  "
            f"[yellow]lookback:[/yellow] {lookback}s"
        )
        ids_preview = sorted(raw_ids)[:10]
        suffix = "..." if len(raw_ids) > 10 else ""
        console.print(
            f"[yellow]raw_ids ({len(raw_ids)}):[/yellow] {ids_preview}{suffix}"
        )

    # Each entry: (query_string, unix_timestamp_for_query)
    queries = {
        # Sum CPU seconds across nodes for multi-node jobs
        "cpu": (
            f"sum by (jobid) ("
            f"max_over_time("
            f"cgroup_cpu_total_seconds{{"
            f"cluster='{cluster}',jobid=~'{id_regex}'"
            f"}}[{lookback}s]))",
            query_time,
        ),
        # Peak RSS across nodes
        "mem": (
            f"max by (jobid) ("
            f"max_over_time("
            f"cgroup_memory_rss_bytes{{"
            f"cluster='{cluster}',jobid=~'{id_regex}'"
            f"}}[{lookback}s]))",
            query_time,
        ),
        # Instant snapshot of GPU→job allocation at the midpoint of the window.
        # max_over_time would return the highest (most recent) job ID per GPU,
        # which is wrong once GPUs are reassigned to newer jobs.
        "gpu_alloc": (
            f"nvidia_gpu_jobId{{cluster='{cluster}'}}",
            gpu_query_time,
        ),
        # Average GPU utilization per device; also centred on the midpoint
        "gpu_util": (
            f"avg by (instance, minor_number) ("
            f"avg_over_time("
            f"nvidia_gpu_duty_cycle{{cluster='{cluster}'}}[{lookback}s:60s]))",
            gpu_query_time,
        ),
    }

    def post_query(item):
        key, query, qt = item
        if debug:
            console.print(
                f"\n[yellow]--- query: {key} (time={qt}) ---[/yellow]\n{query}"
            )
        data = urllib.parse.urlencode({"query": query, "time": str(qt)}).encode()
        req = urllib.request.Request(url, data=data, method="POST")
        req.add_header("Content-Type", "application/x-www-form-urlencoded")
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                body = resp.read()
        except urllib.error.HTTPError as e:
            # Read the error body so Prometheus's message is visible
            err_body = e.read().decode(errors="replace")
            if debug:
                console.print(
                    f"[bold red]  → HTTP {e.code} on '{key}':[/bold red] {err_body}"
                )
            return key, {}
        parsed = json.loads(body)
        if debug:
            n = len(parsed.get("data", {}).get("result", []))
            console.print(f"[yellow]  → {n} result(s)[/yellow]")
            if n > 0:
                console.print(json.dumps(parsed["data"]["result"][:3], indent=2))
        return key, parsed

    try:
        with ThreadPoolExecutor(max_workers=4) as pool:
            # Pass (key, query, time) tuples to post_query
            items = [(k, q, t) for k, (q, t) in queries.items()]
            futures = {pool.submit(post_query, item): item[0] for item in items}
            raw_results = {}
            for future in as_completed(futures):
                key, data = future.result()
                raw_results[key] = data
    except Exception as e:
        if debug:
            console.print(f"[bold red]Prometheus connection failed:[/bold red] {e}")
        return {}

    output: dict = {}

    # CPU seconds per task
    for item in raw_results.get("cpu", {}).get("data", {}).get("result", []):
        rawid = item["metric"].get("jobid", "")
        if rawid in raw_ids:
            output.setdefault(rawid, {})["cpu_seconds"] = float(item["value"][1])

    # Peak RSS in GB per task
    for item in raw_results.get("mem", {}).get("data", {}).get("result", []):
        rawid = item["metric"].get("jobid", "")
        if rawid in raw_ids:
            output.setdefault(rawid, {})["rss_gb"] = float(item["value"][1]) / 1e9

    # GPU: join allocation map to utilization map by (instance, minor_number)
    gpu_alloc: dict = {}
    for item in raw_results.get("gpu_alloc", {}).get("data", {}).get("result", []):
        key = (
            item["metric"].get("instance", ""),
            item["metric"].get("minor_number", ""),
        )
        # nvidia_gpu_jobId stores the numeric job ID as a float value
        gpu_alloc[key] = str(int(float(item["value"][1])))

    gpu_util_map: dict = {}
    for item in raw_results.get("gpu_util", {}).get("data", {}).get("result", []):
        key = (
            item["metric"].get("instance", ""),
            item["metric"].get("minor_number", ""),
        )
        gpu_util_map[key] = float(item["value"][1])

    # Aggregate per-job GPU utilization
    gpu_by_job: dict = {}
    for gpu_key, rawid in gpu_alloc.items():
        if rawid in raw_ids and gpu_key in gpu_util_map:
            gpu_by_job.setdefault(rawid, []).append(gpu_util_map[gpu_key])

    for rawid, utils in gpu_by_job.items():
        output.setdefault(rawid, {})["gpu_util"] = float(np.mean(utils))
        output[rawid]["gpu_count"] = len(utils)

    return output


def job_eff(job_id, cluster=None, prom_url=None, debug=False):
    # Prefer explicit argument; fall back to the Slurm env var
    if cluster is None:
        cluster = os.getenv("SLURM_CLUSTER_NAME")

    cluster_flag = f"--cluster {cluster}" if cluster else ""

    # JobIDRaw and Start/End are needed to build Prometheus time windows.
    # AdminComment is kept for the jobstats fallback (used at sites running
    # store_jobstats even when Prometheus is not configured here).
    fmt_short = (
        "JobID,JobName,Elapsed,ReqMem,ReqCPUS,Timelimit,State,TotalCPU,"
        "NNodes,User,Group,Cluster,AdminComment,JobIDRaw,Start,End"
    )
    # fmt_long provides the sacct fallback for CPU time and memory.
    fmt_long = (
        "JobID,JobName,Elapsed,ReqMem,ReqCPUS,Timelimit,State,TotalCPU,"
        "NNodes,User,Group,Cluster,MaxRSS"
    )

    # Both sacct queries are independent — run them in parallel.
    with ThreadPoolExecutor(max_workers=2) as pool:
        future_short = pool.submit(run_sacct, fmt_short, job_id, cluster_flag, True)
        future_long = pool.submit(run_sacct, fmt_long, job_id, cluster_flag, False)
        df_short = future_short.result()
        df_long = future_long.result()

    # Check that at least one job has finished before doing any work
    fin = finished_mask(df_long["State"])
    if not fin.any():
        console.print(f"[yellow]No finished jobs found for {job_id}.[/yellow]")
        return

    # --- Data cleaning ---
    df_short = df_short.fillna(0.0)
    df_long = df_long.fillna(0.0)

    # Strip job-step suffixes (e.g. "12345.batch" → "12345")
    df_long["JobID"] = df_long["JobID"].astype(str).str.split(".").str[0]

    # Parse memory (sacct appends "G" when --units=G is used)
    df_long["MaxRSS"] = pd.to_numeric(
        df_long["MaxRSS"].astype(str).str.replace("G", "", regex=False),
        errors="coerce",
    ).fillna(0.0)
    df_long["ReqMem"] = pd.to_numeric(
        df_long["ReqMem"].astype(str).str.replace("G", "", regex=False),
        errors="coerce",
    ).fillna(0.0)

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
    req_mem_raw = str(first["ReqMem"])  # e.g. "4G"
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

    # --- sacct baseline efficiency values (always computed as fallback) ---
    df_finished = df_long[fin]
    sacct_cpu = df_finished.groupby("JobID")["TotalCPU"].max()
    sacct_time = df_finished.groupby("JobID")["Elapsed"].max()
    sacct_mem = df_finished.groupby("JobID")["MaxRSS"].max()

    req_time_secs = time_to_float(req_time_raw)
    req_mem_gb = (
        float(req_mem_raw.replace("G", ""))
        if req_mem_raw.replace("G", "").replace(".", "").isdigit()
        else 0.0
    )
    n_cores = float(cores) if float(cores) > 0 else 1.0

    # --- Prometheus query (if configured) ---
    # Filter df_short to only finished tasks so we only query jobs
    # with complete time windows.
    fin_short = df_short[finished_mask(df_short["State"])].copy()
    prom_data: dict = {}
    if prom_url:
        prom_data = query_prometheus(
            fin_short, str(cluster_name), prom_url, debug=debug
        )

    # --- Build a JobID → JobIDRaw lookup for Prometheus result joining ---
    id_map = dict(
        zip(fin_short["JobID"].astype(str), fin_short["JobIDRaw"].astype(str))
    )
    # AdminComment lookup for the jobstats GPU fallback
    ac_map = dict(zip(fin_short["JobID"].astype(str), fin_short["AdminComment"]))

    # --- Build per-task efficiency arrays ---
    # Source priority: Prometheus → sacct (CPU/memory)
    # GPU priority:    Prometheus → AdminComment (jobstats) → none
    cpu_list, time_list, mem_list = [], [], []
    gpu_utils: list = []
    gpu_counts_list: list = []

    for jid in sacct_cpu.index:
        jid_str = str(jid)
        raw_id = id_map.get(jid_str, "")

        time_list.append(float(sacct_time.get(jid, 0.0)))

        if raw_id in prom_data:
            entry = prom_data[raw_id]
            cpu_list.append(entry.get("cpu_seconds", float(sacct_cpu.get(jid, 0.0))))
            mem_list.append(entry.get("rss_gb", float(sacct_mem.get(jid, 0.0))))
        else:
            cpu_list.append(float(sacct_cpu.get(jid, 0.0)))
            mem_list.append(float(sacct_mem.get(jid, 0.0)))

        # GPU: try Prometheus first, then AdminComment (jobstats), regardless
        # of whether Prometheus had cpu/mem data for this job.
        if raw_id in prom_data and "gpu_util" in prom_data[raw_id]:
            entry = prom_data[raw_id]
            gpu_utils.append(entry["gpu_util"])
            gpu_counts_list.append(entry.get("gpu_count", 1))
        else:
            # AdminComment GPU fallback (Princeton jobstats store_jobstats)
            js = get_stats_dict(ac_map.get(jid_str, ""))
            if js:
                n = gpu_count(js)
                if n > 0:
                    gpu_utils.append(gpu_util(js))
                    gpu_counts_list.append(n)

    cpu_arr = np.array(cpu_list)
    time_arr = np.array(time_list)
    mem_arr = np.array(mem_list)

    with np.errstate(divide="ignore", invalid="ignore"):
        cpu_eff = np.where(time_arr > 0, cpu_arr / (time_arr * n_cores), 0.0)
        mem_eff = np.where(req_mem_gb > 0, mem_arr / req_mem_gb, 0.0)
        time_eff = np.where(req_time_secs > 0, time_arr / req_time_secs, 0.0)
    # TIMEOUT jobs run slightly over the limit; clamp so they appear at 100%
    time_eff = np.minimum(time_eff, 1.0)

    has_gpu_data = len(gpu_utils) > 0

    # --- Statistics panel ---
    console.print(
        Rule(
            "[bold]Finished Job Statistics[/bold] "
            "[dim](excludes pending, running, and cancelled)[/dim]",
            style="blue",
        )
    )
    stats_table = Table(box=None, show_header=False, padding=(0, 1))
    stats_table.add_column(style="bold cyan", justify="right")
    stats_table.add_column(style="white")
    stats_table.add_row("Jobs analyzed:", str(len(cpu_arr)))
    stats_table.add_row("Avg CPU Efficiency:", f"{cpu_eff.mean() * 100:.1f}%")
    stats_table.add_row("Avg Memory Usage:", f"{mem_arr.mean():.2f} GB")
    stats_table.add_row("Avg Runtime:", f"{time_arr.mean():.0f}s")
    if has_gpu_data:
        stats_table.add_row("Avg Requested GPUs:", f"{np.mean(gpu_counts_list):.1f}")
        stats_table.add_row("Avg GPU Efficiency:", f"{np.mean(gpu_utils):.1f}%")
    # Show which data source provided CPU/memory metrics
    if prom_url:
        n_prom = sum(
            1 for jid in sacct_cpu.index if id_map.get(str(jid), "") in prom_data
        )
        n_total = len(cpu_arr)
        if n_prom == n_total:
            src_label = "Prometheus"
        elif n_prom > 0:
            src_label = f"Prometheus ({n_prom}/{n_total} tasks) + sacct"
        else:
            src_label = "sacct (Prometheus returned no data)"
        stats_table.add_row("Metrics source:", src_label)
    console.print(stats_table)

    # --- Histograms (array jobs only) ---
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
            "Reports CPU, memory, and wall-time efficiency for completed jobs.\n"
            "For job arrays, per-task histograms are shown for each metric.\n"
            "For single jobs, a summary similar to `seff` is produced.\n"
            "\n"
            "Metrics source priority (first available wins):\n"
            "  1. Prometheus  — accurate cgroup + GPU metrics (requires --prometheus)\n"
            "  2. jobstats    — GPU data from AdminComment field (Princeton jobstats)\n"
            "  3. sacct       — built-in Slurm accounting (always available)\n"
            "\n"
            "Examples:\n"
            "  seff-array 12345678                          # single job or array\n"
            "  seff-array 12345678_42                       # specific array task\n"
            "  seff-array 12345678 -c grace                 # specify cluster\n"
            "  seff-array 12345678 --prometheus http://prometheus:9090\n"
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
        "--prometheus",
        dest="prometheus",
        default=os.getenv("SEFF_ARRAY_PROM_URL"),
        metavar="URL",
        help=(
            "Prometheus base URL for accurate cgroup and GPU metrics "
            "(e.g. http://prometheus:9090). "
            "Overrides the SEFF_ARRAY_PROM_URL environment variable."
        ),
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        default=False,
        help="Print raw Prometheus queries and responses for troubleshooting.",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
    )
    args = parser.parse_args()

    job_eff(args.jobid, args.cluster, args.prometheus, debug=args.debug)


if __name__ == "__main__":
    main()
