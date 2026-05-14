# Where's My Stuff - Inventory Dashboard

Interactive dashboard for the Total Inventory "Where's My Stuff" rollup data.

## Features

- **SBU Filter** - Filter by segment (OMNI_SEG_DESC)
- **Division Filter** - Filter by OMNI_DIVISION
- **Department Filter** - Filter by OMNI_DEPT_DESC
- **Item Filter** - Search by MDS_FAM_ID or WALMART_NBR
- **Replen Type Filter** - Filter REPLEN vs NON-REPLEN

## Dashboard Pages

1. **Overview** - KPI cards and summary charts
2. **Detail View** - Item-level data table with export
3. **DC Breakdown** - DC inventory by type

## Local Development

### Prerequisites

1. Install R (4.0+)
2. Install Quarto CLI
3. Run package installation:

```r
source("install_packages.R")
```

### Authenticate with BigQuery

```r
library(bigrquery)
bq_auth()  # Opens browser for OAuth
```

### Run Locally

```bash
quarto preview wheres_my_stuff_dashboard.qmd
```

## Deploy to Posit Connect

### Option 1: Push Button Deploy (RStudio)

1. Open `wheres_my_stuff_dashboard.qmd` in RStudio
2. Click the "Publish" button (blue icon)
3. Select your Posit Connect server
4. Configure access permissions

### Option 2: Command Line

```bash
quarto publish connect wheres_my_stuff_dashboard.qmd
```

### Option 3: rsconnect Package

```r
library(rsconnect)

# Set up account (one-time)
rsconnect::setAccountInfo(
  name = "your_account",
  token = "your_token",
  secret = "your_secret",
  server = "posit.wal-mart.com"
)

# Deploy
rsconnect::deployDoc(
  doc = "wheres_my_stuff_dashboard.qmd",
  appTitle = "Where's My Stuff - Inventory Rollup"
)
```

## Auto-Refresh Configuration

On Posit Connect, configure scheduled refresh:

1. Go to your deployed content
2. Click "Schedule" tab
3. Set refresh interval (e.g., daily at 6 AM)

The dashboard queries BigQuery on each load, so data is always current.

## BigQuery Authentication on Posit Connect

Set environment variable in Posit Connect:
- **GOOGLE_APPLICATION_CREDENTIALS** = path to service account JSON key

Or use Workload Identity Federation if configured.

## Data Source

```
wmt-instockinventory-datamart.WM_AD_HOC.WHERES_MY_STUFF_ROLLUP
```

## Support

Contact: Total Inventory Team
