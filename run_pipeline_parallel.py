"""
Parallel pipeline runner for HIST_COMBINED rebuild.

Runs Steps 1-5.5 simultaneously using BigQuery Sessions, which allows
all parallel jobs to share the session-level TEMP TABLE ITEM_CUR created
in Step 0. Step 7 (HIST_COMBINED) runs after all steps complete.

Usage:
    python run_pipeline_parallel.py

Expected time:
    Sequential (original SQL):  ~45-70 min
    Parallel (this script):     ~15-25 min (bounded by STEP 5.5 On Yard)

Requirements:
    pip install google-cloud-bigquery db-dtypes
    GOOGLE_APPLICATION_CREDENTIALS or ADC configured
"""

import time
import os
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.cloud import bigquery
from google.oauth2 import service_account

# ── Config ────────────────────────────────────────────────────────────────────
PROJECT    = "wmt-execution-intel-prod"
DATASET    = "WM_AD_HOC"
CREDS_FILE = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")

# ── SQL for each step ─────────────────────────────────────────────────────────
# Each step SQL is read from the full pipeline file and split by step marker.
# Alternatively, embed them directly here.

PIPELINE_SQL_FILE = os.path.join(os.path.dirname(__file__), "historical_inventory_full_pipeline.sql")


def _get_client() -> bigquery.Client:
    creds_json = os.environ.get("GOOGLE_CREDENTIALS", "")
    if creds_json:
        info = json.loads(creds_json)
        credentials = service_account.Credentials.from_service_account_info(info)
        return bigquery.Client(project=PROJECT, credentials=credentials)
    return bigquery.Client(project=PROJECT)


def _split_pipeline_steps(sql_content: str) -> dict:
    """Split the combined SQL file into individual steps by STEP header comments."""
    import re
    # Match -- ===...=== / -- STEP N: markers
    pattern = r'(?=-- =+\n-- STEP (\d+(?:\.\d+)?):)'
    parts = re.split(pattern, sql_content)

    steps = {}
    # parts: ['', '0', '<sql0>', '1', '<sql1>', '1.5', '<sql1.5>', ...]
    i = 1
    while i < len(parts) - 1:
        step_num = parts[i]
        step_sql = parts[i + 1] if i + 1 < len(parts) else ""
        steps[step_num] = step_sql.strip()
        i += 2

    return steps


def run_step(client: bigquery.Client, step_name: str, sql: str,
             session_id: str = None) -> float:
    """Run a single BQ step (optionally in a session). Returns elapsed seconds."""
    t0 = time.time()
    print(f"[STEP {step_name}] Starting...", flush=True)

    job_config = bigquery.QueryJobConfig()
    if session_id:
        job_config.connection_properties = [
            bigquery.ConnectionProperty(key="session_id", value=session_id)
        ]
        job_config.create_session = False

    job = client.query(sql, job_config=job_config)
    job.result()  # blocks until complete

    elapsed = round(time.time() - t0, 1)
    print(f"[STEP {step_name}] Done in {elapsed}s", flush=True)
    return elapsed


def main():
    print("=" * 60, flush=True)
    print("Where's My Stuff — Parallel Pipeline Runner", flush=True)
    print("=" * 60, flush=True)

    if not os.path.exists(PIPELINE_SQL_FILE):
        raise FileNotFoundError(f"Pipeline SQL not found: {PIPELINE_SQL_FILE}")

    with open(PIPELINE_SQL_FILE, "r", encoding="utf-8") as f:
        sql_content = f.read()

    steps = _split_pipeline_steps(sql_content)
    print(f"Parsed steps: {sorted(steps.keys())}", flush=True)

    client = _get_client()
    total_start = time.time()

    # ── STEP 0: Create ITEM_CUR in a session so parallel steps can share it ──
    # BQ Sessions allow multiple parallel jobs to read the same session-level TEMP TABLE.
    print("\n[STEP 0] Creating ITEM_CUR temp table (session)...", flush=True)
    step0_sql = steps.get("0", "")
    if not step0_sql:
        raise ValueError("Could not parse STEP 0 SQL from pipeline file")

    session_config = bigquery.QueryJobConfig(create_session=True)
    session_job = client.query(step0_sql, job_config=session_config)
    session_job.result()
    session_id = session_job.session_info.session_id
    elapsed0 = round(time.time() - total_start, 1)
    print(f"[STEP 0] Done in {elapsed0}s — session_id={session_id}", flush=True)

    # ── STEPS 1-5.5: Run in parallel (all share ITEM_CUR via session) ─────────
    parallel_steps = ["1", "1.5", "2", "3", "4", "5", "5.5"]
    parallel_sqls = {}
    for s in parallel_steps:
        if s in steps:
            parallel_sqls[s] = steps[s]
        else:
            print(f"[WARN] Step {s} SQL not found — skipping", flush=True)

    print(f"\nRunning {len(parallel_sqls)} steps in parallel: {list(parallel_sqls.keys())}", flush=True)
    parallel_start = time.time()

    results = {}
    errors = {}
    with ThreadPoolExecutor(max_workers=len(parallel_sqls)) as executor:
        future_to_step = {
            executor.submit(run_step, client, step, sql, session_id): step
            for step, sql in parallel_sqls.items()
        }
        for future in as_completed(future_to_step):
            step = future_to_step[future]
            try:
                results[step] = future.result()
            except Exception as e:
                errors[step] = str(e)
                print(f"[STEP {step}] FAILED: {e}", flush=True)

    parallel_elapsed = round(time.time() - parallel_start, 1)
    print(f"\nAll parallel steps done in {parallel_elapsed}s", flush=True)

    if errors:
        print(f"[ERROR] Failed steps: {errors}", flush=True)
        raise RuntimeError(f"Pipeline failed on steps: {list(errors.keys())}")

    # ── STEP 7: HIST_COMBINED (depends on all above) ──────────────────────────
    step7_sql = steps.get("7", "")
    if not step7_sql:
        raise ValueError("Could not parse STEP 7 SQL from pipeline file")

    print("\n[STEP 7] Building HIST_COMBINED...", flush=True)
    t7 = time.time()
    job7 = client.query(step7_sql)
    job7.result()
    elapsed7 = round(time.time() - t7, 1)
    print(f"[STEP 7] Done in {elapsed7}s", flush=True)

    total_elapsed = round(time.time() - total_start, 1)
    print("\n" + "=" * 60, flush=True)
    print(f"Pipeline complete in {total_elapsed}s ({round(total_elapsed/60,1)} min)", flush=True)
    print(f"  Step 0 (ITEM_CUR):      {elapsed0}s", flush=True)
    for s, t in sorted(results.items()):
        print(f"  Step {s} (parallel):    {t}s", flush=True)
    print(f"  Step 7 (HIST_COMBINED): {elapsed7}s", flush=True)
    print("=" * 60, flush=True)


if __name__ == "__main__":
    main()
