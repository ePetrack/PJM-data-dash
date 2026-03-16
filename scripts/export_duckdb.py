"""
export_duckdb.py — Build dashboard .duckdb files from dbt mart Parquet output

Reads dbt-produced Parquet files and packages them into tiered DuckDB files
that the browser dashboard can download on demand:

  pjm_dim.duckdb          — dimension tables (calendar, pnode)
  pjm_today.duckdb        — last 7 days at 5-min RT grain
  pjm_data_{YYYY}H{1,2}.duckdb  — historical half-yearly hourly files
  pjm_daily_agg.duckdb    — full history aggregated to daily grain

Usage:
    python scripts/export_duckdb.py --data-dir data/ --output dashboard/
"""

import argparse
from pathlib import Path

import duckdb


def connect(path: str | None = None) -> duckdb.DuckDBPyConnection:
    if path:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(path or ":memory:")


def export_dim(data_dir: Path, output_dir: Path) -> None:
    """Build pjm_dim.duckdb — calendar and pnode dimensions."""
    out = str(output_dir / "pjm_dim.duckdb")
    print(f"  Building {out}...")
    con = connect(out)

    con.execute(f"""
        create or replace table dim_pnode as
        select * from read_parquet('{data_dir}/dim_pnode/*.parquet')
    """)

    con.execute(f"""
        create or replace table dim_calendar as
        select * from read_parquet('{data_dir}/dim_calendar/*.parquet')
    """)

    con.close()
    print(f"    → {Path(out).stat().st_size / 1024:.0f} KB")


def export_today(data_dir: Path, output_dir: Path) -> None:
    """
    Build pjm_today.duckdb — last 7 days of data.
    Uses 5-min RT LMPs if available; falls back to hourly.
    """
    out = str(output_dir / "pjm_today.duckdb")
    print(f"  Building {out}...")
    con = connect(out)

    # Try 5-min RT first, fall back to hourly
    fivemin_parquet = list((data_dir / "rt_5min").glob("*.parquet"))
    if fivemin_parquet:
        parquet_glob = str(data_dir / "rt_5min/*.parquet")
        grain = "5min"
    else:
        parquet_glob = str(data_dir / "rt_lmps/*.parquet")
        grain = "hourly"

    print(f"    grain: {grain}")

    con.execute(f"""
        create or replace table rt_recent as
        select
            datetime_beginning_ept,
            pnode_id,
            pnode_name,
            zone,
            cast(total_lmp_rt as real) as total_lmp_rt,
            cast(congestion_price_rt as real) as congestion_rt,
            cast(marginal_loss_price_rt as real) as loss_rt
        from read_parquet('{parquet_glob}')
        where strptime(datetime_beginning_ept, '%m/%d/%Y %H:%M')
              >= current_timestamp - interval '7 days'
    """)

    con.execute(f"""
        create or replace table da_recent as
        select
            datetime_beginning_ept,
            pnode_id,
            pnode_name,
            zone,
            cast(total_lmp_da as real) as total_lmp_da
        from read_parquet('{data_dir}/da_lmps/*.parquet')
        where strptime(datetime_beginning_ept, '%m/%d/%Y %H:%M')
              >= current_timestamp - interval '7 days'
    """)

    con.close()
    print(f"    → {Path(out).stat().st_size / 1024:.0f} KB")


def export_historical(data_dir: Path, output_dir: Path) -> None:
    """
    Build pjm_data_{YYYY}H{1,2}.duckdb — half-yearly historical files at hourly grain.
    Mirrors analytics-as-code's strategy for keeping files under 100 MB.
    """
    import duckdb as _duckdb

    # Scan available date range from mart_lmps parquet
    parquet_glob = str(data_dir / "mart_lmps/*.parquet")
    con = _duckdb.connect(":memory:")
    try:
        result = con.execute(f"""
            select
                min(hour_beginning_utc)::date as min_date,
                max(hour_beginning_utc)::date as max_date
            from read_parquet('{parquet_glob}')
        """).fetchone()
    except Exception:
        print("  No mart_lmps parquet found — skipping historical export")
        con.close()
        return

    con.close()
    min_date, max_date = result

    # Iterate half-years
    from datetime import date

    year = min_date.year
    half = 1 if min_date.month <= 6 else 2

    while True:
        h_start = date(year, 1, 1) if half == 1 else date(year, 7, 1)
        h_end   = date(year, 6, 30) if half == 1 else date(year, 12, 31)

        if h_start > max_date:
            break

        out = str(output_dir / f"pjm_data_{year}H{half}.duckdb")
        print(f"  Building {out}...")
        out_con = connect(out)

        out_con.execute(f"""
            create or replace table lmps as
            select
                hour_beginning_utc,
                pnode_id,
                pnode_name,
                zone,
                cast(total_lmp_da as real) as total_lmp_da,
                cast(total_lmp_rt as real) as total_lmp_rt,
                cast(da_rt_spread as real) as da_rt_spread
            from read_parquet('{parquet_glob}')
            where hour_beginning_utc::date between '{h_start}' and '{h_end}'
        """)

        out_con.close()
        print(f"    → {Path(out).stat().st_size / 1024:.0f} KB")

        if half == 1:
            half = 2
        else:
            half = 1
            year += 1


def export_daily_agg(data_dir: Path, output_dir: Path) -> None:
    """Build pjm_daily_agg.duckdb — full history aggregated to daily grain."""
    out = str(output_dir / "pjm_daily_agg.duckdb")
    print(f"  Building {out}...")
    con = connect(out)

    con.execute(f"""
        create or replace table lmps_daily as
        select
            hour_beginning_utc::date as date_utc,
            pnode_id,
            pnode_name,
            zone,
            cast(avg(total_lmp_da) as real) as avg_lmp_da,
            cast(avg(total_lmp_rt) as real) as avg_lmp_rt,
            cast(min(total_lmp_da) as real) as min_lmp_da,
            cast(max(total_lmp_da) as real) as max_lmp_da,
            cast(avg(da_rt_spread) as real) as avg_spread,
            count(*) as hours
        from read_parquet('{data_dir}/mart_lmps/*.parquet')
        group by 1, 2, 3, 4
        order by 1, 2
    """)

    con.execute(f"""
        create or replace table load_daily as
        select
            hour_beginning_utc::date as date_utc,
            zone,
            cast(avg(actual_load_mw)   as real) as avg_load_mw,
            cast(max(actual_load_mw)   as real) as peak_load_mw,
            cast(avg(forecast_load_mw) as real) as avg_forecast_mw,
            cast(avg(forecast_error_pct) as real) as avg_forecast_err_pct
        from read_parquet('{data_dir}/mart_load/*.parquet')
        group by 1, 2
        order by 1, 2
    """)

    con.close()
    print(f"    → {Path(out).stat().st_size / 1024:.0f} KB")


def main() -> None:
    parser = argparse.ArgumentParser(description="Export dbt marts to dashboard .duckdb files")
    parser.add_argument("--data-dir", default="data", help="Directory containing dbt Parquet output")
    parser.add_argument("--output", default="dashboard", help="Output directory for .duckdb files")
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    print("Exporting dashboard DuckDB files...")
    export_dim(data_dir, output_dir)
    export_today(data_dir, output_dir)
    export_historical(data_dir, output_dir)
    export_daily_agg(data_dir, output_dir)
    print("Done.")


if __name__ == "__main__":
    main()
