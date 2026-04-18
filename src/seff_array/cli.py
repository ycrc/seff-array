#!/usr/bin/env python3

import argparse
import base64
import gzip
import json
import os
import re
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
from rich.table import Table

from seff_array import __version__

console = Console()

# Slurm states that represent a finished job
FINISHED_STATES = {"COMPLETED", "FAILED", "OUT_OF_MEMORY", "TIMEOUT", "PREEMPTED"}

# Maximum Prometheus lookback window (13 days keeps us inside typical 2-week retention)
MAX_PROM_LOOKBACK = 13 * 86400

# Width of printed section rules (independent of terminal width)
RULE_WIDTH = 80


def print_rule(title="", style="blue"):
    """Print a fixed-width horizontal rule with an optional centred title.

    Using a fixed width keeps output readable when pasted into emails,
    tickets, or wide terminals.
    """
    # Strip Rich markup tags to get the visible title length for centering
    visible = re.sub(r"\[/?[^\]]*\]", "", title)
    if visible:
        side = max(1, (RULE_WIDTH - len(visible) - 2) // 2)
        left = "─" * side
        right = "─" * max(1, RULE_WIDTH - len(visible) - 2 - side)
        console.print(f"[{style}]{left}[/{style}] {title} [{style}]{right}[/{style}]")
    else:
        console.print(f"[{style}]{'─' * RULE_WIDTH}[/{style}]")


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


def state_color(state: str) -> str:
    """Return a Rich color name for a Slurm job state string."""
    if state == "COMPLETED":
        return "green"
    if state == "TIMEOUT":
        return "yellow"
    if state in ("FAILED", "OUT_OF_MEMORY"):
        return "red"
    if state.startswith("CANCELLED"):
        return "magenta"
    if state == "RUNNING":
        return "blue"
    return "white"


def finished_mask(state_series: pd.Series) -> pd.Series:
    """Return a boolean mask for rows in a finished state.

    Handles both exact matches (COMPLETED, FAILED, …) and the
    'CANCELLED by <uid>' variant that sacct sometimes emits.
    """
    return state_series.isin(FINISHED_STATES) | state_series.str.startswith("CANCELLED")


def make_histogram_table(
    values, title, unit="%", bins=10, vmin=0, vmax=100, states=None
):
    """Return a rich Table containing a horizontal histogram.

    If `states` is provided (an array parallel to `values`), each bar is
    rendered as a stacked color-coded breakdown by job state.
    """
    values = np.asarray(values)
    h, bin_edges = np.histogram(values, bins=np.linspace(vmin, vmax, num=bins + 1))
    max_count = max(h) if max(h) > 0 else 1
    bar_width = 30

    # Assign each value to a bin index (0-indexed, clamped)
    if states is not None:
        states = np.asarray(states)
        span = vmax - vmin if vmax != vmin else 1
        bin_idx = np.clip(
            np.floor((values - vmin) / span * bins).astype(int), 0, bins - 1
        )

    table = Table(
        title=title,
        box=box.SIMPLE,
        show_header=True,
        title_style="bold cyan",
        padding=(0, 1),
    )
    table.add_column(f"Range ({unit})", style="cyan", justify="right", min_width=12)
    table.add_column("Count", style="yellow", justify="right", min_width=6)
    table.add_column("Distribution", min_width=bar_width + 2, no_wrap=True)

    for i, count in enumerate(h):
        range_str = f"{bin_edges[i]:.0f}\u2013{bin_edges[i + 1]:.0f}"
        total_blocks = int(count / max_count * bar_width)

        if states is not None and count > 0:
            # Tally state counts for this bin
            bin_states = states[bin_idx == i]
            unique, counts = np.unique(bin_states, return_counts=True)
            # Build stacked colored bar; give any rounding remainder to the last segment
            bar = ""
            remaining = total_blocks
            for j, (st, sc) in enumerate(zip(unique, counts)):
                if j == len(unique) - 1:
                    w = remaining  # absorb rounding remainder in last segment
                else:
                    w = round(sc / count * total_blocks)
                w = min(w, remaining)
                remaining -= w
                color = state_color(str(st))
                bar += f"[{color}]\u2588" * w + f"[/{color}]" if w else ""
        else:
            bar = "[green]\u2588[/green]" * total_blocks

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
    gpu_start = int(min(starts))
    gpu_end = int(max(ends))
    lookback = min(gpu_end - gpu_start, MAX_PROM_LOOKBACK)
    if lookback < 60:
        # Window too small to be meaningful
        return {}

    # Step for GPU range queries: aim for ~500 time steps, minimum 60s.
    # This gives good attribution resolution without excessive data volume.
    gpu_step = max(60, lookback // 500)

    id_regex = "|".join(raw_ids)
    instant_url = f"{prom_url.rstrip('/')}/api/v1/query"
    range_url = f"{prom_url.rstrip('/')}/api/v1/query_range"

    if debug:
        print_rule("[bold yellow]Prometheus debug[/bold yellow]", style="yellow")
        console.print(f"[yellow]instant URL:[/yellow] {instant_url}")
        console.print(f"[yellow]range URL:[/yellow]   {range_url}")
        console.print(
            f"[yellow]query_time:[/yellow] {query_time}  "
            f"[yellow]lookback:[/yellow] {lookback}s  "
            f"[yellow]gpu_step:[/yellow] {gpu_step}s"
        )
        ids_preview = sorted(raw_ids)[:10]
        suffix = "..." if len(raw_ids) > 10 else ""
        console.print(
            f"[yellow]raw_ids ({len(raw_ids)}):[/yellow] {ids_preview}{suffix}"
        )

    # Instant queries: (url, {post_params})
    # Range queries use query_range endpoint with start/end/step instead of time.
    query_specs = {
        # Sum CPU seconds across nodes for multi-node jobs
        "cpu": (
            instant_url,
            {
                "query": (
                    f"sum by (jobid) ("
                    f"max_over_time("
                    f"cgroup_cpu_total_seconds{{"
                    f"cluster='{cluster}',jobid=~'{id_regex}'"
                    f"}}[{lookback}s]))"
                ),
                "time": str(query_time),
            },
        ),
        # Peak RSS across nodes
        "mem": (
            instant_url,
            {
                "query": (
                    f"max by (jobid) ("
                    f"max_over_time("
                    f"cgroup_memory_rss_bytes{{"
                    f"cluster='{cluster}',jobid=~'{id_regex}'"
                    f"}}[{lookback}s]))"
                ),
                "time": str(query_time),
            },
        ),
        # Range query: which job owned each GPU at each time step.
        # Using query_range gives full temporal coverage of the job window
        # so staggered array tasks are all correctly attributed.
        "gpu_alloc": (
            range_url,
            {
                "query": f"nvidia_gpu_jobId{{cluster='{cluster}'}}",
                "start": str(gpu_start),
                "end": str(gpu_end),
                "step": str(gpu_step),
            },
        ),
        # Range query: GPU utilization at the same time steps
        "gpu_util": (
            range_url,
            {
                "query": f"nvidia_gpu_duty_cycle{{cluster='{cluster}'}}",
                "start": str(gpu_start),
                "end": str(gpu_end),
                "step": str(gpu_step),
            },
        ),
    }

    def post_query(item):
        key, req_url, params = item
        if debug:
            console.print(f"\n[yellow]--- query: {key} ---[/yellow]\n{params['query']}")
        data = urllib.parse.urlencode(params).encode()
        req = urllib.request.Request(req_url, data=data, method="POST")
        req.add_header("Content-Type", "application/x-www-form-urlencoded")
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                body = resp.read()
        except urllib.error.HTTPError as e:
            err_body = e.read().decode(errors="replace")
            if debug:
                console.print(
                    f"[bold red]  → HTTP {e.code} on '{key}':[/bold red] {err_body}"
                )
            return key, {}
        parsed = json.loads(body)
        if debug:
            n = len(parsed.get("data", {}).get("result", []))
            console.print(f"[yellow]  → {n} series[/yellow]")
            if n > 0:
                # Show first series with up to 3 sample values
                s = parsed["data"]["result"][0]
                preview = s.get("values", [s.get("value")])[:3]
                console.print(
                    json.dumps({"metric": s["metric"], "samples": preview}, indent=2)
                )
        return key, parsed

    try:
        with ThreadPoolExecutor(max_workers=4) as pool:
            items = [(k, u, p) for k, (u, p) in query_specs.items()]
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

    # CPU seconds per task (instant query → "value")
    for item in raw_results.get("cpu", {}).get("data", {}).get("result", []):
        rawid = item["metric"].get("jobid", "")
        if rawid in raw_ids:
            output.setdefault(rawid, {})["cpu_seconds"] = float(item["value"][1])

    # Peak RSS in GB per task (instant query → "value")
    for item in raw_results.get("mem", {}).get("data", {}).get("result", []):
        rawid = item["metric"].get("jobid", "")
        if rawid in raw_ids:
            output.setdefault(rawid, {})["rss_gb"] = float(item["value"][1]) / 1e9

    # GPU: correlate allocation and utilization by (instance, minor_number, timestamp).
    # Both are range queries → "values": [[ts, val], ...] per series.
    # Build a lookup: (instance, minor_number, timestamp) → duty_cycle
    util_ts_map: dict = {}
    for series in raw_results.get("gpu_util", {}).get("data", {}).get("result", []):
        inst = series["metric"].get("instance", "")
        minor = series["metric"].get("minor_number", "")
        for ts, val in series.get("values", []):
            util_ts_map[(inst, minor, int(ts))] = float(val)

    # For each time step where a GPU was allocated to one of our jobs,
    # record the utilization sample and which GPU device it came from.
    gpu_utils_by_job: dict = {}  # rawid -> [duty_cycle, ...]
    gpu_devices_by_job: dict = {}  # rawid -> set of (inst, minor) pairs
    for series in raw_results.get("gpu_alloc", {}).get("data", {}).get("result", []):
        inst = series["metric"].get("instance", "")
        minor = series["metric"].get("minor_number", "")
        for ts, val in series.get("values", []):
            rawid = str(int(float(val)))
            if rawid not in raw_ids:
                continue
            util = util_ts_map.get((inst, minor, int(ts)))
            if util is not None:
                gpu_utils_by_job.setdefault(rawid, []).append(util)
                gpu_devices_by_job.setdefault(rawid, set()).add((inst, minor))

    for rawid, utils in gpu_utils_by_job.items():
        output.setdefault(rawid, {})["gpu_util"] = float(np.mean(utils))
        output[rawid]["gpu_count"] = len(gpu_devices_by_job.get(rawid, set()))

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
    print_rule("[bold]Job Information[/bold]")
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
    print_rule("[bold]Job Status[/bold]")
    status_table = Table(box=box.SIMPLE, show_header=True, padding=(0, 1))
    status_table.add_column("State", style="bold")
    status_table.add_column("Count", style="yellow", justify="right")
    for state, count in df_short["State"].value_counts().items():
        color = state_color(str(state))
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
    fin_short = df_short[finished_mask(df_short["State"])].copy()
    # When Prometheus is available, include running jobs so their live
    # cgroup metrics can be shown alongside finished tasks.
    run_short = (
        df_short[df_short["State"] == "RUNNING"].copy() if prom_url else pd.DataFrame()
    )

    prom_data: dict = {}
    if prom_url:
        prom_input = (
            pd.concat([fin_short, run_short], ignore_index=True)
            if not run_short.empty
            else fin_short
        )
        prom_data = query_prometheus(
            prom_input, str(cluster_name), prom_url, debug=debug
        )

    # --- Build lookups covering finished + running jobs ---
    all_short = (
        pd.concat([fin_short, run_short], ignore_index=True)
        if not run_short.empty
        else fin_short
    )
    id_map = dict(
        zip(all_short["JobID"].astype(str), all_short["JobIDRaw"].astype(str))
    )
    # AdminComment lookup for the jobstats GPU fallback
    ac_map = dict(zip(all_short["JobID"].astype(str), all_short["AdminComment"]))

    # State lookup for histogram coloring
    state_map = dict(zip(all_short["JobID"].astype(str), all_short["State"]))

    # --- Build per-task efficiency arrays ---
    # Source priority: Prometheus → sacct (CPU/memory)
    # GPU priority:    Prometheus → AdminComment (jobstats) → none
    cpu_list, time_list, mem_list, state_list = [], [], [], []
    gpu_utils: list = []
    gpu_counts_list: list = []
    gpu_states_list: list = []

    # Running jobs are handled separately below; skip them here to avoid
    # double-counting when a job's .batch step appears as COMPLETED in the
    # step-level sacct output while the main job is still RUNNING.
    run_job_ids = {str(row["JobID"]) for _, row in run_short.iterrows()}

    for jid in sacct_cpu.index:
        jid_str = str(jid)
        if jid_str in run_job_ids:
            continue
        raw_id = id_map.get(jid_str, "")
        task_state = str(state_map.get(jid_str, "UNKNOWN"))
        # Normalise "CANCELLED by <uid>" → "CANCELLED" for consistent coloring
        if task_state.startswith("CANCELLED"):
            task_state = "CANCELLED"

        time_list.append(float(sacct_time.get(jid, 0.0)))
        state_list.append(task_state)

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
            gpu_states_list.append(task_state)
        else:
            # AdminComment GPU fallback (Princeton jobstats store_jobstats)
            js = get_stats_dict(ac_map.get(jid_str, ""))
            if js:
                n = gpu_count(js)
                if n > 0:
                    gpu_utils.append(gpu_util(js))
                    gpu_counts_list.append(n)
                    gpu_states_list.append(task_state)

    # --- Running jobs (Prometheus only; sacct data is unreliable mid-job) ---
    for _, row in run_short.iterrows():
        jid_str = str(row["JobID"])
        raw_id = str(row["JobIDRaw"])
        if raw_id not in prom_data:
            continue  # no live data available — skip rather than show zeros
        entry = prom_data[raw_id]
        # Use elapsed time from sacct (updated by Slurm) as the time denominator
        elapsed = time_to_float(str(row["Elapsed"])) if row["Elapsed"] != 0.0 else 0.0
        time_list.append(elapsed)
        state_list.append("RUNNING")
        cpu_list.append(entry.get("cpu_seconds", 0.0))
        mem_list.append(entry.get("rss_gb", 0.0))
        if "gpu_util" in entry:
            gpu_utils.append(entry["gpu_util"])
            gpu_counts_list.append(entry.get("gpu_count", 1))
            gpu_states_list.append("RUNNING")

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
    n_running_shown = len(run_short) if prom_url and not run_short.empty else 0
    if n_running_shown:
        stats_rule = (
            "[bold]Job Statistics[/bold] "
            "[dim](finished + running; excludes pending and cancelled)[/dim]"
        )
    else:
        stats_rule = (
            "[bold]Finished Job Statistics[/bold] "
            "[dim](excludes pending, running, and cancelled)[/dim]"
        )
    print_rule(stats_rule)
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
        n_prom = (
            sum(1 for jid in sacct_cpu.index if id_map.get(str(jid), "") in prom_data)
            + n_running_shown
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
    state_arr = np.array(state_list)
    if is_array_job:
        console.print(
            make_histogram_table(cpu_eff * 100, "CPU Efficiency", states=state_arr)
        )
        if has_gpu_data:
            console.print(
                make_histogram_table(
                    gpu_utils, "GPU Efficiency", states=np.array(gpu_states_list)
                )
            )
        console.print(
            make_histogram_table(mem_eff * 100, "Memory Efficiency", states=state_arr)
        )
        console.print(
            make_histogram_table(time_eff * 100, "Time Efficiency", states=state_arr)
        )


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
