"""
export_duckdb.py — Build dashboard .duckdb files from dbt output

After `dbt run --target prod` writes mart tables to a persistent DuckDB file
(DBT_PATH), this script attaches that file and packages the data into tiered
files that the browser dashboard downloads on demand:

  pjm_dim.duckdb          — dimension tables (calendar, pnode)
  pjm_today.duckdb        — last 7 days at 5-min RT grain (or hourly fallback)
  pjm_data_{YYYY}H{1,2}.duckdb  — historical half-yearly hourly files
  pjm_daily_agg.duckdb    — full history aggregated to daily grain

Usage:
    python scripts/export_duckdb.py \\
        --dbt-path /tmp/pjm_dash.duckdb \\
        --data-dir data/ \\
        --output dashboard/
"""

import argparse
from datetime import date
from pathlib import Path

import duckdb


def connect(path: str | None = None) -> duckdb.DuckDBPyConnection:
    if path:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(path or ":memory:")


def _size_kb(path: str) -> str:
    p = Path(path)
    return f"{p.stat().st_size / 1024:.0f} KB" if p.exists() else "0 KB"


# ---------------------------------------------------------------------------
# Dimension export — reads directly from raw Parquet (not dbt output)
# ---------------------------------------------------------------------------

def export_dim(dbt_path: str, data_dir: Path, output_dir: Path) -> None:
    """Build pjm_dim.duckdb — calendar and pnode dimensions from dbt tables."""
    out = str(output_dir / "pjm_dim.duckdb")
    print(f"  Building {out}...")
    con = connect(out)
    dbt = duckdb.connect(dbt_path, read_only=True)

    # dim_pnode
    try:
        rows = dbt.execute("select * from dim_pnode").fetchdf()
        con.execute("create or replace table dim_pnode as select * from rows")
    except Exception as e:
        print(f"    warn: dim_pnode not found in dbt file ({e}) — skipping")

    # dim_calendar
    try:
        rows = dbt.execute("select * from dim_calendar").fetchdf()
        con.execute("create or replace table dim_calendar as select * from rows")
    except Exception as e:
        print(f"    warn: dim_calendar not found in dbt file ({e}) — skipping")

    dbt.close()
    con.close()
    print(f"    → {_size_kb(out)}")


# ---------------------------------------------------------------------------
# Today export — last 7 days, reads from raw Parquet (intraday grain)
# ---------------------------------------------------------------------------

def export_today(data_dir: Path, output_dir: Path) -> None:
    """
    Build pjm_today.duckdb — last 7 days.
    Uses 5-min RT LMPs if available; falls back to hourly.
    """
    out = str(output_dir / "pjm_today.duckdb")
    print(f"  Building {out}...")

    fivemin = list((data_dir / "rt_5min").glob("*.parquet")) if (data_dir / "rt_5min").exists() else []
    if fivemin:
        parquet_glob = str(data_dir / "rt_5min/*.parquet")
        grain = "5min"
    else:
        parquet_glob = str(data_dir / "rt_lmps/*.parquet")
        grain = "hourly"

    print(f"    RT grain: {grain}")

    con = connect(out)
    try:
        con.execute(f"""
            create or replace table rt_recent as
            select
                datetime_beginning_ept,
                cast(pnode_id as integer)            as pnode_id,
                trim(pnode_name)                     as pnode_name,
                coalesce(trim(zone), trim(pnode_name)) as zone,
                cast(total_lmp_rt as real)           as total_lmp_rt,
                try_cast(congestion_price_rt as real) as congestion_rt,
                try_cast(marginal_loss_price_rt as real) as loss_rt
            from read_parquet('{parquet_glob}')
            where try_strptime(datetime_beginning_ept, '%m/%d/%Y %H:%M')
                  >= current_timestamp - interval '7 days'
        """)
    except Exception as e:
        print(f"    warn: RT Parquet not available ({e})")

    da_glob = str(data_dir / "da_lmps/*.parquet")
    try:
        con.execute(f"""
            create or replace table da_recent as
            select
                datetime_beginning_ept,
                try_cast(pnode_id as integer)           as pnode_id,
                trim(pnode_name)                        as pnode_name,
                coalesce(trim(zone), trim(pnode_name))  as zone,
                cast(total_lmp_da as real)              as total_lmp_da
            from read_parquet('{da_glob}')
            where try_strptime(datetime_beginning_ept, '%m/%d/%Y %H:%M')
                  >= current_timestamp - interval '7 days'
        """)
    except Exception as e:
        print(f"    warn: DA Parquet not available ({e})")

    con.close()
    print(f"    → {_size_kb(out)}")


# ---------------------------------------------------------------------------
# Historical export — half-yearly files, hourly grain from dbt mart
# ---------------------------------------------------------------------------

def export_historical(dbt_path: str, output_dir: Path) -> None:
    """
    Build pjm_data_{YYYY}H{1,2}.duckdb — half-yearly files at hourly grain.
    Reads mart_lmps from the dbt DuckDB file.
    """
    dbt = duckdb.connect(dbt_path, read_only=True)
    try:
        result = dbt.execute("""
            select
                min(hour_beginning_utc)::date as min_date,
                max(hour_beginning_utc)::date as max_date
            from mart_lmps
        """).fetchone()
    except Exception as e:
        print(f"  mart_lmps not found in dbt file ({e}) — skipping historical export")
        dbt.close()
        return

    dbt.close()

    if not result or result[0] is None:
        print("  mart_lmps is empty — skipping historical export")
        return

    min_date, max_date = result
    year = min_date.year
    half = 1 if min_date.month <= 6 else 2

    while True:
        h_start = date(year, 1, 1)  if half == 1 else date(year, 7, 1)
        h_end   = date(year, 6, 30) if half == 1 else date(year, 12, 31)

        if h_start > max_date:
            break

        out = str(output_dir / f"pjm_data_{year}H{half}.duckdb")
        print(f"  Building {out}...")

        dbt = duckdb.connect(dbt_path, read_only=True)
        out_con = connect(out)

        try:
            df = dbt.execute(f"""
                select
                    hour_beginning_utc,
                    pnode_id,
                    pnode_name,
                    zone,
                    cast(total_lmp_da  as real) as total_lmp_da,
                    cast(total_lmp_rt  as real) as total_lmp_rt,
                    cast(da_rt_spread  as real) as da_rt_spread
                from mart_lmps
                where hour_beginning_utc::date between '{h_start}' and '{h_end}'
            """).fetchdf()

            out_con.execute("create or replace table lmps as select * from df")
        except Exception as e:
            print(f"    warn: {e}")
        finally:
            dbt.close()
            out_con.close()

        print(f"    → {_size_kb(out)}")

        if half == 1:
            half = 2
        else:
            half = 1
            year += 1


# ---------------------------------------------------------------------------
# Daily aggregate export — full history at daily grain
# ---------------------------------------------------------------------------

def export_daily_agg(dbt_path: str, output_dir: Path) -> None:
    """Build pjm_daily_agg.duckdb — full history at daily grain."""
    out = str(output_dir / "pjm_daily_agg.duckdb")
    print(f"  Building {out}...")

    dbt = duckdb.connect(dbt_path, read_only=True)
    out_con = connect(out)

    try:
        df = dbt.execute("""
            select
                hour_beginning_utc::date          as date_utc,
                pnode_id,
                pnode_name,
                zone,
                cast(avg(total_lmp_da) as real)   as avg_lmp_da,
                cast(avg(total_lmp_rt) as real)    as avg_lmp_rt,
                cast(min(total_lmp_da) as real)    as min_lmp_da,
                cast(max(total_lmp_da) as real)    as max_lmp_da,
                cast(avg(da_rt_spread) as real)    as avg_spread,
                count(*)                           as hours
            from mart_lmps
            group by 1, 2, 3, 4
            order by 1, 2
        """).fetchdf()
        out_con.execute("create or replace table lmps_daily as select * from df")
    except Exception as e:
        print(f"    warn: mart_lmps not available ({e})")

    try:
        df = dbt.execute("""
            select
                hour_beginning_utc::date              as date_utc,
                zone,
                cast(avg(actual_load_mw)    as real)  as avg_load_mw,
                cast(max(actual_load_mw)    as real)  as peak_load_mw,
                cast(avg(forecast_load_mw)  as real)  as avg_forecast_mw,
                cast(avg(abs(forecast_error_pct)) as real) as avg_forecast_err_pct
            from mart_load
            group by 1, 2
            order by 1, 2
        """).fetchdf()
        out_con.execute("create or replace table load_daily as select * from df")
    except Exception as e:
        print(f"    warn: mart_load not available ({e})")

    dbt.close()
    out_con.close()
    print(f"    → {_size_kb(out)}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Export dbt marts to dashboard .duckdb files")
    parser.add_argument(
        "--dbt-path",
        default="/tmp/pjm_dash.duckdb",
        help="Path to the persistent DuckDB file written by dbt (DBT_PATH)",
    )
    parser.add_argument("--data-dir", default="data", help="Directory with raw Parquet from fetch_pjm.py")
    parser.add_argument("--output",   default="dashboard", help="Output directory for .duckdb files")
    args = parser.parse_args()

    dbt_path  = args.dbt_path
    data_dir  = Path(args.data_dir)
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Exporting dashboard DuckDB files from {dbt_path}...")
    export_dim(dbt_path, data_dir, output_dir)
    export_today(data_dir, output_dir)
    export_historical(dbt_path, output_dir)
    export_daily_agg(dbt_path, output_dir)
    print("Done.")


if __name__ == "__main__":
    main()
