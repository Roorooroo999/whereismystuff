"""
Fetch historical inventory data from BigQuery and save as JSON for local dashboard viewing.

Usage:
    python fetch_historical.py                     # uses ADC or GOOGLE_CREDENTIALS env
    python fetch_historical.py --output data.json  # custom output path

Output: ../dashboard/historical_data.json
No pandas required — uses BigQuery row iterator directly.
"""

from google.cloud import bigquery
from google.oauth2 import service_account
import json
import os
import argparse
from datetime import date, datetime
from decimal import Decimal


class BQEncoder(json.JSONEncoder):
    """Handle BigQuery types that aren't JSON-serializable by default."""
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, (date, datetime)):
            return obj.isoformat()
        return super().default(obj)


def get_client():
    """Get BigQuery client using same auth as server.py."""
    project = os.environ.get("PROJECT_ID", "wmt-execution-intel-prod")
    creds_json = os.environ.get("GOOGLE_CREDENTIALS", "")
    if creds_json:
        info = json.loads(creds_json)
        credentials = service_account.Credentials.from_service_account_info(info)
        return bigquery.Client(project=project, credentials=credentials)
    return bigquery.Client(project=project)


def rows_to_dicts(query_job):
    """Convert BigQuery RowIterator to list of dicts, serializing dates to strings."""
    rows = []
    for row in query_job:
        d = dict(row)
        # Convert date/datetime objects to ISO strings
        for k, v in d.items():
            if isinstance(v, (date, datetime)):
                d[k] = v.isoformat()
        rows.append(d)
    return rows


def fetch_data():
    """
    Pull three datasets from R0C0JUG_WMUS_HIST_COMBINED:
    1. Enterprise-level weekly totals (for trend charts)
    2. Weekly trend at SBU level (for SBU comparison)
    3. Weekly trend at Dept level (for drill-down table)
    """
    client = get_client()
    project = os.environ.get("PROJECT_ID", "wmt-execution-intel-prod")
    dataset = os.environ.get("DATASET", "WM_AD_HOC")
    table = "R0C0JUG_WMUS_HIST_COMBINED"

    # DC type + STO type columns from combined table
    dc_type_cols = """
        SUM(DC_OH_REGIONAL_UNITS) AS dc_oh_regional,
        SUM(DC_OH_GROCERY_UNITS) AS dc_oh_grocery,
        SUM(DC_OH_FASHION_UNITS) AS dc_oh_fashion,
        SUM(DC_OH_IMPORTS_UNITS) AS dc_oh_imports,
        SUM(DC_OH_GIDC_UNITS) AS dc_oh_gidc,
        SUM(DC_OH_MSC_UNITS) AS dc_oh_msc,
        SUM(DC_OH_SUPPORT_UNITS) AS dc_oh_support,
        SUM(DC_OH_OTHER_UNITS) AS dc_oh_other,
        SUM(STO_TO_REGIONAL_UNITS) AS sto_to_regional,
        SUM(STO_TO_GROCERY_UNITS) AS sto_to_grocery,
        SUM(STO_TO_FASHION_UNITS) AS sto_to_fashion,
        SUM(STO_TO_IMPORTS_UNITS) AS sto_to_imports,
        SUM(STO_TO_GIDC_UNITS) AS sto_to_gidc,
        SUM(STO_TO_MSC_UNITS) AS sto_to_msc,
        SUM(STO_TO_SUPPORT_UNITS) AS sto_to_support,
        SUM(STO_TO_OTHER_UNITS) AS sto_to_other
    """

    # ── Query 1: Enterprise-level weekly totals ──
    total_query = f"""
    SELECT
        BUS_DT,
        WM_YEAR,
        WM_WEEK,
        WM_YR_WK_NBR,
        SUM(STORE_OH_UNITS) AS store_oh,
        SUM(BACKROOM_UNITS) AS backroom,
        SUM(ON_FLOOR_UNITS) AS on_floor,
        SUM(DC_RESERVED_UNITS) AS dc_reserved,
        SUM(IN_TRANSIT_UNITS) AS in_transit,
        SUM(DC_OH_UNITS) AS dc_oh,
        SUM(DC_LABELED_UNITS) AS dc_labeled,
        SUM(DC_UNLABELED_UNITS) AS dc_unlabeled,
        SUM(STO_IN_TRANSIT_TO_DC_UNITS) AS sto_to_dc,
        SUM(TOTAL_NETWORK_UNITS) AS total_network,
        {dc_type_cols}
    FROM `{project}.{dataset}.{table}`
    GROUP BY BUS_DT, WM_YEAR, WM_WEEK, WM_YR_WK_NBR
    ORDER BY BUS_DT
    """

    # ── Query 2: SBU-level weekly aggregation ──
    sbu_query = f"""
    SELECT
        BUS_DT,
        WM_YEAR,
        WM_WEEK,
        WM_YR_WK_NBR,
        SBU,
        SUM(STORE_OH_UNITS) AS store_oh,
        SUM(BACKROOM_UNITS) AS backroom,
        SUM(ON_FLOOR_UNITS) AS on_floor,
        SUM(DC_RESERVED_UNITS) AS dc_reserved,
        SUM(IN_TRANSIT_UNITS) AS in_transit,
        SUM(DC_OH_UNITS) AS dc_oh,
        SUM(DC_LABELED_UNITS) AS dc_labeled,
        SUM(DC_UNLABELED_UNITS) AS dc_unlabeled,
        SUM(STO_IN_TRANSIT_TO_DC_UNITS) AS sto_to_dc,
        SUM(TOTAL_NETWORK_UNITS) AS total_network,
        {dc_type_cols}
    FROM `{project}.{dataset}.{table}`
    GROUP BY BUS_DT, WM_YEAR, WM_WEEK, WM_YR_WK_NBR, SBU
    ORDER BY BUS_DT, SBU
    """

    # ── Query 3: Dept-level weekly aggregation ──
    dept_query = f"""
    SELECT
        BUS_DT,
        WM_YEAR,
        WM_WEEK,
        WM_YR_WK_NBR,
        SBU,
        OMNI_DEPT_NBR AS dept_nbr,
        OMNI_DEPT_DESC AS department,
        SUM(STORE_OH_UNITS) AS store_oh,
        SUM(BACKROOM_UNITS) AS backroom,
        SUM(ON_FLOOR_UNITS) AS on_floor,
        SUM(DC_RESERVED_UNITS) AS dc_reserved,
        SUM(IN_TRANSIT_UNITS) AS in_transit,
        SUM(DC_OH_UNITS) AS dc_oh,
        SUM(DC_LABELED_UNITS) AS dc_labeled,
        SUM(DC_UNLABELED_UNITS) AS dc_unlabeled,
        SUM(STO_IN_TRANSIT_TO_DC_UNITS) AS sto_to_dc,
        SUM(TOTAL_NETWORK_UNITS) AS total_network,
        {dc_type_cols}
    FROM `{project}.{dataset}.{table}`
    GROUP BY BUS_DT, WM_YEAR, WM_WEEK, WM_YR_WK_NBR, SBU, dept_nbr, department
    ORDER BY BUS_DT, SBU, dept_nbr
    """

    print("[1/3] Fetching enterprise-level weekly totals...")
    total_rows = rows_to_dicts(client.query(total_query).result())
    print(f"       {len(total_rows)} rows")

    print("[2/3] Fetching SBU-level weekly data...")
    sbu_rows = rows_to_dicts(client.query(sbu_query).result())
    print(f"       {len(sbu_rows)} rows")

    print("[3/3] Fetching Dept-level weekly data...")
    dept_rows = rows_to_dicts(client.query(dept_query).result())
    print(f"       {len(dept_rows)} rows")

    # Extract date range and SBU list
    all_dates = sorted(set(r["BUS_DT"] for r in total_rows))
    sbu_list = sorted(set(r["SBU"] for r in sbu_rows))

    # Build output structure
    data = {
        "generated_at": datetime.now().isoformat(),
        "date_range": {
            "min": all_dates[0] if all_dates else None,
            "max": all_dates[-1] if all_dates else None,
            "weeks": len(all_dates)
        },
        "sbu_list": sbu_list,
        "enterprise": total_rows,
        "by_sbu": sbu_rows,
        "by_dept": dept_rows
    }

    return data


def main():
    parser = argparse.ArgumentParser(description="Fetch historical inventory data from BigQuery")
    parser.add_argument("--output", "-o", default=None,
                        help="Output JSON file path (default: ../dashboard/historical_data.json)")
    args = parser.parse_args()

    output_path = args.output or os.path.join(
        os.path.dirname(__file__), "..", "dashboard", "historical_data.json"
    )

    print("=" * 60)
    print("Historical Inventory Data Extraction")
    print("=" * 60)

    data = fetch_data()

    with open(output_path, "w") as f:
        json.dump(data, f, indent=2, cls=BQEncoder)

    size_mb = os.path.getsize(output_path) / (1024 * 1024)
    print(f"\nSaved to: {output_path}")
    print(f"File size: {size_mb:.1f} MB")
    print(f"Date range: {data['date_range']['min']} -> {data['date_range']['max']} ({data['date_range']['weeks']} weeks)")
    print(f"SBUs: {', '.join(data['sbu_list'])}")
    print("Done!")


if __name__ == "__main__":
    main()
