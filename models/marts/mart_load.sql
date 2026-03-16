-- Mart: load actual vs forecast
-- One row per (hour_beginning_utc, zone).
-- Joins metered actuals to the most recent forecast for that hour.

{{
    config(
        materialized = 'incremental',
        unique_key = ['hour_beginning_utc', 'zone'],
        on_schema_change = 'append_new_columns'
    )
}}

with actual as (
    select
        hour_beginning_utc,
        hour_beginning_ept,
        zone,
        load_area,
        load_mw,
        is_verified
    from {{ ref('stg_load') }}

    {% if is_incremental() %}
    where hour_beginning_utc > (select max(hour_beginning_utc) from {{ this }})
    {% endif %}
),

forecast as (
    select
        forecast_hour_utc,
        forecast_area,
        forecast_load_mw
    from {{ ref('stg_load_forecast') }}

    {% if is_incremental() %}
    where forecast_hour_utc > (select max(hour_beginning_utc) from {{ this }})
    {% endif %}
),

joined as (
    select
        a.hour_beginning_utc,
        a.hour_beginning_ept,
        a.zone,
        a.load_area,
        a.load_mw                   as actual_load_mw,
        a.is_verified,
        f.forecast_load_mw,
        -- forecast error (MW and %)
        case
            when f.forecast_load_mw is not null
            then a.load_mw - f.forecast_load_mw
        end as forecast_error_mw,
        case
            when f.forecast_load_mw is not null and f.forecast_load_mw != 0
            then (a.load_mw - f.forecast_load_mw) / f.forecast_load_mw * 100.0
        end as forecast_error_pct

    from actual a
    left join forecast f
        on  upper(a.zone) = upper(f.forecast_area)
        and a.hour_beginning_utc = f.forecast_hour_utc
)

select * from joined
