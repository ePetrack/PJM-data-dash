"""
backfill_eia.py — Download EIA wholesale market CSVs and write to Parquet

EIA hosts pre-built annual CSV files of PJM market data at no-auth URLs:

  DA hourly LMPs by zone:  pjm_lmp_da_hr_zones_{YYYY}.csv
  DA hourly LMPs by hub:   pjm_lmp_da_hr_hubs_{YYYY}.csv
  RT 5-min LMPs by zone:   pjm_lmp_rt_5min_zones_{YYYY}Q{Q}.csv
  Hourly metered load:      pjm_load_act_hr_{YYYY}.csv

These are normalised to the same Parquet schema that fetch_pjm.py produces
so dbt staging models can read both sources from the same glob pattern.

Usage:
    # Backfill all available years (2022–current) — DA LMPs + load
    python scripts/backfill_eia.py --output data/

    # Specific years
    python scripts/backfill_eia.py --years 2023 2024 --output data/

    # Include RT 5-min data (larger files, quarterly)
    python scripts/backfill_eia.py --include-rt --output data/

    # Hubs only (Western Hub etc.)
    python scripts/backfill_eia.py --types hubs --output data/

    # Refresh current-year files (for daily ingest — re-downloads even if exists)
    python scripts/backfill_eia.py --years 2024 --refresh --output data/

EIA URL catalog: https://www.eia.gov/electricity/wholesalemarkets/data.php?rto=pjm
"""

import argparse
import io
import sys
import time
from datetime import date
from pathlib import Path

import pandas as pd
import requests

# ---------------------------------------------------------------------------
# EIA URL templates
# ---------------------------------------------------------------------------

EIA_BASE = "https://www.eia.gov/electricity/wholesalemarkets/csv"

URL_TEMPLATES = {
    "da_zones": f"{EIA_BASE}/pjm_lmp_da_hr_zones_{{year}}.csv",
    "da_hubs":  f"{EIA_BASE}/pjm_lmp_da_hr_hubs_{{year}}.csv",
    "rt_zones": f"{EIA_BASE}/pjm_lmp_rt_5min_zones_{{year}}Q{{quarter}}.csv",
    "load":     f"{EIA_BASE}/pjm_load_act_hr_{{year}}.csv",
}

# EIA portal launched March 2024, but initial data coverage starts 2022
FIRST_EIA_YEAR = 2022


# ---------------------------------------------------------------------------
# Column normalisers
# ---------------------------------------------------------------------------

def normalise_da_zones(df: pd.DataFrame) -> pd.DataFrame:
    """
    Map EIA DA zone CSV columns to PJM API schema.

    EIA column candidates (inspect your actual CSV to confirm):
      period, respondent, respondent-name, type, type-name, value, value-units
      — or —
      datetime_beginning_ept, pnode_name, zone, total_lmp_da, ...

    We attempt a flexible detection and fall back to raw renaming.
    """
    df.columns = [c.strip().lower().replace(" ", "_").replace("-", "_") for c in df.columns]

    # If it already looks like PJM API format, return as-is
    if "datetime_beginning_ept" in df.columns and "total_lmp_da" in df.columns:
        return df

    # EIA API v2 wide-format (period, respondent, value columns)
    if "period" in df.columns and "value" in df.columns:
        return _normalise_eia_api_format(df, price_col_suffix="_da")

    # EIA flat CSV format (varies by release — best-effort mapping)
    rename_map = {}
    for col in df.columns:
        if "datetime" in col or "period" in col or col in ("date", "hour"):
            rename_map[col] = "datetime_beginning_ept"
        elif "lmp" in col and "da" in col:
            rename_map[col] = "total_lmp_da"
        elif "energy" in col and ("da" in col or "price" in col):
            rename_map[col] = "system_energy_price_da"
        elif "congestion" in col:
            rename_map[col] = "congestion_price_da"
        elif "loss" in col or "marginal" in col:
            rename_map[col] = "marginal_loss_price_da"
        elif "zone" in col or "respondent_name" in col or "pnode_name" in col:
            rename_map[col] = "pnode_name"
        elif col in ("respondent", "pnode_id"):
            rename_map[col] = "pnode_id"

    df = df.rename(columns=rename_map)

    # Ensure required columns exist (fill with None if missing)
    for required in ["datetime_beginning_ept", "pnode_name", "total_lmp_da"]:
        if required not in df.columns:
            df[required] = None

    # Add PJM-API-compatible columns that EIA omits
    if "pnode_type" not in df.columns:
        df["pnode_type"] = "ZONE"
    if "zone" not in df.columns:
        df["zone"] = df.get("pnode_name")
    if "type" not in df.columns:
        df["type"] = "ZONE"

    # Source tag so dbt can filter if needed
    df["source"] = "eia"

    return df


def normalise_da_hubs(df: pd.DataFrame) -> pd.DataFrame:
    df = normalise_da_zones(df)
    df["pnode_type"] = "HUB"
    df["type"] = "HUB"
    return df


def normalise_rt_zones(df: pd.DataFrame) -> pd.DataFrame:
    """Normalise RT 5-min EIA CSV to match rt_hrl_lmps schema."""
    df.columns = [c.strip().lower().replace(" ", "_").replace("-", "_") for c in df.columns]

    if "datetime_beginning_ept" in df.columns and "total_lmp_rt" in df.columns:
        df["source"] = "eia"
        return df

    rename_map = {}
    for col in df.columns:
        if "datetime" in col or "period" in col:
            rename_map[col] = "datetime_beginning_ept"
        elif "lmp" in col and "rt" in col:
            rename_map[col] = "total_lmp_rt"
        elif "energy" in col:
            rename_map[col] = "system_energy_price_rt"
        elif "congestion" in col:
            rename_map[col] = "congestion_price_rt"
        elif "loss" in col or "marginal" in col:
            rename_map[col] = "marginal_loss_price_rt"
        elif "zone" in col or "respondent_name" in col or "pnode_name" in col:
            rename_map[col] = "pnode_name"
        elif col in ("respondent", "pnode_id"):
            rename_map[col] = "pnode_id"

    df = df.rename(columns=rename_map)

    for required in ["datetime_beginning_ept", "pnode_name", "total_lmp_rt"]:
        if required not in df.columns:
            df[required] = None

    if "pnode_type" not in df.columns:
        df["pnode_type"] = "ZONE"
    if "zone" not in df.columns:
        df["zone"] = df.get("pnode_name")
    if "type" not in df.columns:
        df["type"] = "ZONE"

    df["source"] = "eia"
    return df


def normalise_load(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalise EIA hourly load CSV to match hrl_load_metered schema.

    Expected downstream columns (stg_load.sql):
      datetime_beginning_ept, zone, load_area, mw, is_verified, nerc_region, mkt_region
    """
    df.columns = [c.strip().lower().replace(" ", "_").replace("-", "_") for c in df.columns]

    # If it already looks like PJM API format, return as-is
    if "datetime_beginning_ept" in df.columns and "mw" in df.columns:
        df["source"] = "eia"
        return df

    # Flexible column mapping for varying EIA CSV formats
    rename_map = {}
    for col in df.columns:
        if col in rename_map.values():
            continue
        if "datetime" in col or "period" in col or col in ("date", "hour"):
            rename_map[col] = "datetime_beginning_ept"
        elif col in ("mw", "megawatts") or ("load" in col and "mw" in col):
            rename_map[col] = "mw"
        elif col == "value":
            rename_map[col] = "mw"
        elif "zone" in col or "respondent_name" in col:
            rename_map[col] = "zone"
        elif "load_area" in col or col == "area":
            rename_map[col] = "load_area"
        elif "nerc" in col:
            rename_map[col] = "nerc_region"
        elif "mkt_region" in col or "market_region" in col:
            rename_map[col] = "mkt_region"
        elif "verified" in col:
            rename_map[col] = "is_verified"

    df = df.rename(columns=rename_map)

    # Ensure required columns exist
    for required in ["datetime_beginning_ept", "zone", "mw"]:
        if required not in df.columns:
            df[required] = None

    # Fill optional columns
    for optional in ["load_area", "nerc_region", "mkt_region", "is_verified"]:
        if optional not in df.columns:
            df[optional] = None

    df["source"] = "eia"
    return df


def _normalise_eia_api_format(df: pd.DataFrame, price_col_suffix: str) -> pd.DataFrame:
    """Handle EIA API v2 long/wide format where value is in a 'value' column."""
    out = pd.DataFrame()
    out["datetime_beginning_ept"] = df.get("period")
    out["pnode_name"] = df.get("respondent_name", df.get("respondent"))
    out["pnode_id"]   = df.get("respondent")
    out["pnode_type"] = "ZONE"
    out["zone"]       = df.get("respondent_name", df.get("respondent"))
    out["type"]       = "ZONE"

    price_key = f"total_lmp{price_col_suffix}"
    out[price_key]   = pd.to_numeric(df.get("value"), errors="coerce")
    out["source"]    = "eia"
    return out


# ---------------------------------------------------------------------------
# Download helpers
# ---------------------------------------------------------------------------

def _detect_header_row(content: bytes, max_lines: int = 20) -> int:
    """Return the index of the most likely header row (the one with the most commas)."""
    lines = content.split(b"\n", max_lines)[:max_lines]
    if not lines:
        return 0
    best_row = 0
    best_count = 0
    for i, line in enumerate(lines):
        count = line.count(b",")
        if count > best_count:
            best_count = count
            best_row = i
    return best_row


def fetch_csv(url: str, retries: int = 3) -> pd.DataFrame | None:
    for attempt in range(retries):
        try:
            resp = requests.get(url, timeout=120, stream=True)
            if resp.status_code == 404:
                return None  # file doesn't exist yet (future year/quarter)
            resp.raise_for_status()
            content = resp.content
            # EIA CSVs may have metadata rows before the real CSV header.
            header_row = _detect_header_row(content)
            return pd.read_csv(
                io.BytesIO(content), skiprows=header_row, low_memory=False
            )
        except requests.RequestException as e:
            if attempt < retries - 1:
                wait = 2 ** attempt
                print(f"    retrying in {wait}s ({e})")
                time.sleep(wait)
            else:
                print(f"    FAILED: {e}")
                return None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill PJM LMPs from EIA CSV files")
    parser.add_argument(
        "--years", nargs="+", type=int,
        default=list(range(FIRST_EIA_YEAR, date.today().year + 1)),
        help="Years to download (default: 2022 to current year)",
    )
    parser.add_argument(
        "--types", nargs="+",
        choices=["zones", "hubs", "rt_zones", "load"],
        default=["zones", "hubs", "load"],
        help="Which file types to download (default: zones hubs load)",
    )
    parser.add_argument(
        "--include-rt", action="store_true",
        help="Also download RT 5-min zone files (adds --types rt_zones)",
    )
    parser.add_argument(
        "--refresh", action="store_true",
        help="Re-download files even if they already exist (for daily ingest of current-year data)",
    )
    parser.add_argument("--output", default="data", help="Output directory")
    args = parser.parse_args()

    if args.include_rt and "rt_zones" not in args.types:
        args.types.append("rt_zones")

    output_dir = Path(args.output)

    print(f"EIA backfill: years={args.years}, types={args.types}")

    for year in sorted(args.years):
        for file_type in args.types:
            if file_type == "rt_zones":
                # Quarterly files
                quarters = range(1, 5)
                for q in quarters:
                    url = URL_TEMPLATES["rt_zones"].format(year=year, quarter=q)
                    out_dir = output_dir / "rt_5min"
                    out_dir.mkdir(parents=True, exist_ok=True)
                    out_file = out_dir / f"eia_{year}Q{q}.parquet"

                    if out_file.exists() and not args.refresh:
                        print(f"  skip (exists): {out_file.name}")
                        continue

                    print(f"  [{year}Q{q} RT zones] {url}")
                    df = fetch_csv(url)
                    if df is None:
                        print(f"    not available — skipping")
                        continue

                    df = normalise_rt_zones(df)
                    df.to_parquet(out_file, index=False)
                    print(f"    {len(df):,} rows → {out_file}")

            elif file_type == "load":
                # Annual load files
                url      = URL_TEMPLATES["load"].format(year=year)
                out_dir  = output_dir / "load"
                out_dir.mkdir(parents=True, exist_ok=True)
                out_file = out_dir / f"eia_load_{year}.parquet"

                if out_file.exists() and not args.refresh:
                    print(f"  skip (exists): {out_file.name}")
                    continue

                print(f"  [{year} load] {url}")
                df = fetch_csv(url)
                if df is None:
                    print(f"    not available — skipping")
                    continue

                df = normalise_load(df)
                df.to_parquet(out_file, index=False)
                print(f"    {len(df):,} rows → {out_file}")

            else:
                # Annual DA LMP files (zones / hubs)
                url_key  = "da_zones" if file_type == "zones" else "da_hubs"
                url      = URL_TEMPLATES[url_key].format(year=year)
                out_dir  = output_dir / "da_lmps"
                out_dir.mkdir(parents=True, exist_ok=True)
                out_file = out_dir / f"eia_{file_type}_{year}.parquet"

                if out_file.exists() and not args.refresh:
                    print(f"  skip (exists): {out_file.name}")
                    continue

                print(f"  [{year} DA {file_type}] {url}")
                df = fetch_csv(url)
                if df is None:
                    print(f"    not available — skipping")
                    continue

                normaliser = normalise_da_hubs if file_type == "hubs" else normalise_da_zones
                df = normaliser(df)
                df.to_parquet(out_file, index=False)
                print(f"    {len(df):,} rows → {out_file}")

    print("Backfill complete.")
    print()
    print("NOTE: If column mapping looks wrong, inspect a raw CSV and update")
    print("      the normalise_*() functions in this script.")
    print(f"      EIA catalog: https://www.eia.gov/electricity/wholesalemarkets/data.php?rto=pjm")


if __name__ == "__main__":
    main()
