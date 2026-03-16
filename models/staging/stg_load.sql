-- Staging: Hourly metered load
-- Source: PJM feed hrl_load_metered, written to data/load/*.parquet by fetch_pjm.py
--
-- Key fields from the API:
--   datetime_beginning_ept, zone, load_area, mw, is_verified, nerc_region, mkt_region

with source as (
    select * from read_parquet('{{ env_var("DATA_DIR", "data") }}/load/*.parquet')
),

renamed as (
    select
        -- timestamps
        strptime(datetime_beginning_ept, '%m/%d/%Y %H:%M') as hour_beginning_ept,
        strptime(datetime_beginning_ept, '%m/%d/%Y %H:%M') at time zone 'America/New_York' as hour_beginning_utc,

        -- location
        trim(zone)        as zone,
        trim(load_area)   as load_area,
        trim(nerc_region) as nerc_region,
        trim(mkt_region)  as mkt_region,

        -- load
        cast(mw as double)            as load_mw,
        cast(is_verified as boolean)  as is_verified

    from source
)

select * from renamed
