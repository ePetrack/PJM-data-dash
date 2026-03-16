# PJM-data-dash

A serverless analytics dashboard for PJM electricity market data, modeled after
[djouallah/analytics-as-code](https://github.com/djouallah/analytics-as-code).
Everything — ingest, transforms, tests, and the dashboard itself — lives in Git
and runs on GitHub Actions. No servers, no databases, no accounts required to
get started.

---

## Architecture overview

```
PJM Data Miner 2 API (DA/RT LMPs, load, forecasts)
  │
  ├─ GitHub Actions every 30 min (ingest.yml)
  │    └─ scripts/fetch_pjm.py  → Parquet files committed to `data` branch
  │
  ├─ GitHub Actions daily + after ingest (transform.yml)
  │    ├─ dbt run --target prod  → reads Parquet, builds mart tables (DuckDB in-memory)
  │    └─ scripts/export_duckdb.py  → writes optimised .duckdb files to dashboard/
  │         └─ deploys to GitHub Pages
  │
  └─ Browser: dashboard/index.html
       ├─ DuckDB-WASM: client-side SQL queries against .duckdb files
       ├─ ECharts: interactive charts (LMPs, load, regulation)
       └─ OPFS cache: avoids re-downloading files on revisit
```

### Inspired by analytics-as-code

| analytics-as-code (AEMO) | PJM-data-dash |
|---|---|
| AEMO SCADA + price feeds | PJM Data Miner 2 API feeds |
| Apache Iceberg REST catalog | Parquet files on `data` branch |
| dbt-duckdb incremental models | dbt-duckdb incremental models |
| GitHub Actions scheduler | GitHub Actions scheduler |
| DuckDB-WASM + ECharts dashboard | DuckDB-WASM + ECharts dashboard |

The main simplification: we skip the Iceberg catalog and store raw data as
Parquet files directly on a `data` git branch. Iceberg can be layered on later
if write throughput demands it.

---

## Data sources

### PJM Data Miner 2 — settings.json trick

PJM's web UI embeds a public subscription key in a config file. No account
registration needed:

```python
import requests

settings = requests.get("http://dataminer2.pjm.com/config/settings.json").json()
headers = {"Ocp-Apim-Subscription-Key": settings["subscriptionKey"]}

r = requests.get(
    "https://api.pjm.com/api/v1/da_hrl_lmps",
    headers=headers,
    params={
        "startRow": 1,
        "rowCount": 50000,
        "datetime_beginning_ept": "Yesterday",
        "zone": "DUQ",
        "type": "ZONE",
    },
)
```

**Rate limits**: anonymous key = 6 req/min. Register free at
`apiportal.pjm.com` for 600 req/min.

### Key feeds

| Feed | Grain | Retention | Notes |
|---|---|---|---|
| `da_hrl_lmps` | Hourly | 731 days | Day-ahead LMPs by pnode/zone |
| `rt_hrl_lmps` | Hourly | 731 days | Real-time LMPs |
| `rt_fivemin_hrl_lmps` | 5-min | 186 days | Intraday RT |
| `hrl_load_metered` | Hourly | 731 days | Metered zonal load |
| `load_frcstd_7_day` | 30-min updates | Rolling 7 days | Load forecast |
| `reg_market_results` | Hourly | 731 days | Regulation market |

### Date parameter formats

```
datetime_beginning_ept=1/15/2025 00:00 to 1/16/2025 23:00   # range
datetime_beginning_ept=Yesterday                              # keyword
datetime_beginning_ept=CurrentMonth                          # keyword
```

Max range: 366 days. Max rows per request: 50,000.

### Scope

- **Zone**: DUQ (Duquesne Light)
- **Hub**: Western Hub (`pnode_id=51217`)

### EIA bulk CSV (zero-auth fallback)

Full annual PJM zonal LMP files — no headers, no auth, no pagination:

```
https://www.eia.gov/electricity/wholesalemarkets/csv/pjm_lmp_da_hr_zones_{YYYY}.csv
https://www.eia.gov/electricity/wholesalemarkets/csv/pjm_lmp_da_hr_hubs_{YYYY}.csv
https://www.eia.gov/electricity/wholesalemarkets/csv/pjm_lmp_rt_5min_zones_{YYYY}Q{Q}.csv
```

Browse the full catalog: `https://www.eia.gov/electricity/wholesalemarkets/data.php?rto=pjm`

---

## Project structure

```
PJM-data-dash/
├── dbt_project.yml
├── profiles.yml
├── models/
│   ├── staging/
│   │   ├── stg_da_lmps.sql          # normalise DA LMP feed
│   │   ├── stg_rt_lmps.sql          # normalise RT LMP feed
│   │   ├── stg_load.sql             # normalise metered load feed
│   │   └── stg_load_forecast.sql    # normalise load forecast feed
│   ├── dimensions/
│   │   ├── dim_pnode.sql            # zone/hub lookup
│   │   └── dim_calendar.sql         # date/hour calendar
│   └── marts/
│       ├── mart_lmps.sql            # merged DA+RT fact with spread
│       └── mart_load.sql            # load actual vs forecast
├── tests/
│   └── schema.yml                   # uniqueness + not-null tests
├── scripts/
│   ├── fetch_pjm.py                 # PJM API → Parquet
│   └── export_duckdb.py             # dbt marts → dashboard .duckdb files
├── dashboard/
│   └── index.html                   # DuckDB-WASM + ECharts dashboard
└── .github/
    └── workflows/
        ├── ci.yml                   # dbt build on PR (in-memory, no live data)
        ├── ingest.yml               # every 30 min: fetch PJM → Parquet
        └── transform.yml            # daily + after ingest: dbt + export + deploy
```

---

## Dashboard features

| Chart | Description |
|---|---|
| LMP time series | DA vs RT hourly prices, Western Hub + DUQ zone |
| DA–RT spread | Hourly price difference (positive = DA > RT) |
| Load vs forecast | Metered MW vs 7-day forecast for DUQ zone |
| Price duration curve | Hours sorted by price for any date range |
| Regulation prices | Hourly regulation market clearing prices |

**Adaptive resolution** (mirrors analytics-as-code pattern):
- ≤ 7 days selected → 5-min grain (RT only)
- 8–30 days → hourly grain
- > 30 days → daily aggregates

---

## Dashboard .duckdb file tiers

| File | Content | Refresh |
|---|---|---|
| `pjm_dim.duckdb` | Calendar + pnode dimensions | Daily |
| `pjm_today.duckdb` | Last 7 days, 5-min RT grain | Every 30 min |
| `pjm_data_{YYYY}H{1,2}.duckdb` | Historical half-yearly, hourly | Daily |
| `pjm_daily_agg.duckdb` | Full history, daily grain | Daily |

---

## Local development

```bash
# Install dependencies
pip install dbt-duckdb pandas pyarrow requests

# Run CI build (no live data needed — uses synthetic seed data)
dbt build --target ci

# Fetch live PJM data for yesterday
python scripts/fetch_pjm.py --date yesterday --output data/

# Run transforms against local Parquet files
dbt run --target prod

# Export dashboard files
python scripts/export_duckdb.py --data-dir data/ --output dashboard/

# Open dashboard
open dashboard/index.html
```

---

## GitHub Actions setup

1. Fork this repo
2. Enable GitHub Pages (Settings → Pages → source: `gh-pages` branch)
3. Optionally add `PJM_API_KEY` secret (from `apiportal.pjm.com`) for higher
   rate limits — the pipeline works without it using the anonymous key

The three workflows run automatically:
- **ci.yml** — validates SQL on every PR
- **ingest.yml** — fetches fresh PJM data every 30 minutes
- **transform.yml** — runs dbt and deploys dashboard daily

---

## Implementation roadmap

| Phase | Goal |
|---|---|
| 1 | Scaffold (this commit): dbt models, ingest script, CI workflow |
| 2 | Historical backfill: EIA CSV → Parquet bootstrap for 2+ years of DA LMPs |
| 3 | Dashboard v1: LMP time series + load vs forecast |
| 4 | Dashboard v2: price duration curves, regulation prices, date range drill-down |
| 5 | Optional: migrate Parquet storage to Apache Iceberg for scalability |

---

## Technology credits

- [djouallah/analytics-as-code](https://github.com/djouallah/analytics-as-code) — architecture reference
- [dbt-duckdb](https://github.com/duckdb/dbt-duckdb) — SQL transforms
- [DuckDB-WASM](https://duckdb.org/docs/api/wasm/overview) — browser-side SQL
- [Apache ECharts](https://echarts.apache.org/) — visualisation
- [PJM Data Miner 2](https://dataminer2.pjm.com/) — market data source
- [rzwink/pjm_dataminer](https://github.com/rzwink/pjm_dataminer) — settings.json technique
