-- Staging: 7-day load forecast
-- Source: PJM feed load_frcstd_7_day, written to data/load_forecast/*.parquet by fetch_pjm.py
--
-- Key fields from the API:
--   evaluated_at_datetime_ept, forecast_area,
--   forecast_datetime_beginning_ept, forecast_load_mw

with source as (
    select * from read_parquet('{{ env_var("DATA_DIR", "data") }}/load_forecast/*.parquet')
),

renamed as (
    select
        -- when the forecast was published
        strptime(evaluated_at_datetime_ept, '%m/%d/%Y %H:%M')
            as evaluated_at_ept,

        -- what hour the forecast is for
        strptime(forecast_datetime_beginning_ept, '%m/%d/%Y %H:%M')
            as forecast_hour_ept,
        strptime(forecast_datetime_beginning_ept, '%m/%d/%Y %H:%M') at time zone 'America/New_York'
            as forecast_hour_utc,

        -- area (e.g. DUQ, AEP, RTO_COMBINED)
        trim(forecast_area) as forecast_area,

        -- forecasted load
        cast(forecast_load_mw as double) as forecast_load_mw

    from source
),

-- keep only the most recent forecast version for each area + hour
deduped as (
    select *,
        row_number() over (
            partition by forecast_area, forecast_hour_utc
            order by evaluated_at_ept desc
        ) as rn
    from renamed
)

select * exclude (rn) from deduped where rn = 1
