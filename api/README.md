# Where's My Stuff API v2.0

FastAPI backend + Dashboard for the Enterprise Inventory Rollup, deployed on Google Cloud Run.

## Quick Deploy to Cloud Run

### One-Command Deploy (from project root)
```bash
cd "Total Inventory"
gcloud run deploy wheres-my-stuff-api \
  --source . \
  --region us-central1 \
  --allow-unauthenticated \
  --project wmt-instockinventory-datamart
```

This builds and deploys the API + Dashboard together.

### Access After Deployment

| URL | Description |
|-----|-------------|
| `https://wheres-my-stuff-api-xxxxx-uc.a.run.app/` | Health check |
| `https://wheres-my-stuff-api-xxxxx-uc.a.run.app/dashboard` | **Main Dashboard** |
| `https://wheres-my-stuff-api-xxxxx-uc.a.run.app/docs` | Swagger API Docs |

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /` | Health check |
| `GET /dashboard` | Interactive Dashboard UI |
| `GET /api/health` | Detailed health status |
| `GET /api/summary` | Inventory summary with filters |
| `GET /api/item/{id}` | Specific item details |
| `GET /api/by-sbu` | Breakdown by SBU |
| `GET /api/by-division` | Breakdown by Division |
| `GET /api/by-replen` | Breakdown by Replen Type |
| `GET /api/search/items?q=` | Item search/autocomplete |
| `GET /docs` | Interactive Swagger UI |

## Data Columns (v2.0)

The API now includes DC Labeled and Unlabeled inventory:

| Column | Source | Description |
|--------|--------|-------------|
| `store_units` | repl_sku_ei_invt_dly | Store on-hand |
| `dc_reserved_units` | repl_sku_ei_invt_dly | DC allocated for stores |
| `transit_units` | repl_sku_ei_invt_dly | In-transit to stores |
| `dc_units` | WHSE_DLY_SKU | DC on-hand (type O) |
| `dc_labeled_units` | DC_DLY_INVT_TYPE | Labeled inventory (L+S) |
| `dc_unlabeled_units` | DC_DLY_INVT_TYPE | Unlabeled distribution (U) |

## Filter Parameters

All endpoints support these query parameters:
- `sbu` — Filter by SBU (multi-value)
- `division` — Filter by Division (multi-value)
- `department` — Filter by Department (multi-value)
- `replen_type` — Filter by REPLEN or NON-REPLEN
- `item_id` — Filter by MDS_FAM_ID or WALMART_NBR

## Local Development

```bash
cd api
pip install -r requirements.txt
uvicorn server:app --reload --port 8000
```

Then visit:
- http://localhost:8000/dashboard — Dashboard
- http://localhost:8000/docs — Swagger UI

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      Cloud Run                               │
│  ┌─────────────────┐    ┌──────────────────────────────┐   │
│  │   Dashboard     │    │     FastAPI Backend          │   │
│  │   (index.html)  │───▶│  • /api/summary              │   │
│  │                 │    │  • /api/by-sbu               │   │
│  └─────────────────┘    │  • /api/item/{id}            │   │
│                         └──────────────────────────────┘   │
│                                      │                      │
└──────────────────────────────────────│──────────────────────┘
                                       ▼
                              ┌────────────────┐
                              │    BigQuery    │
                              │ WHERES_MY_STUFF│
                              │    _ROLLUP     │
                              └────────────────┘
```

Cloud Run automatically handles:
- Auto-scaling (0 to 10 instances)
- HTTPS/TLS
- Authentication via ADC (Service Account)
- Cold start optimization
