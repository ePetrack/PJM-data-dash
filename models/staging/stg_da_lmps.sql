-- Staging: Day-ahead hourly LMPs
-- Sources (both written to data/da_lmps/*.parquet, normalised to same schema):
--   • fetch_pjm.py  — PJM Data Miner 2 API (ongoing daily ingest)
--   • backfill_eia.py — EIA bulk CSVs (historical bootstrap, adds `source=eia`)
--
-- PJM API key fields:
--   datetime_beginning_ept, pnode_id, pnode_name, type, zone,
--   system_energy_price_da, congestion_price_da, marginal_loss_price_da, total_lmp_da
--
-- EIA files use the same column names after normalisation in backfill_eia.py.
-- The `source` column is present in EIA rows and absent in PJM rows.

with source as (
    select * from read_parquet('{{ env_var("DATA_DIR", "data") }}/da_lmps/*.parquet')
),

renamed as (
    select
        -- timestamps
        -- Support both 'M/D/YYYY H:MM' (PJM API) and ISO formats (EIA may vary)
        case
            when strptime(datetime_beginning_ept, '%m/%d/%Y %H:%M') is not null
            then strptime(datetime_beginning_ept, '%m/%d/%Y %H:%M')
            else cast(datetime_beginning_ept as timestamp)
        end as hour_beginning_ept,
        case
            when strptime(datetime_beginning_ept, '%m/%d/%Y %H:%M') is not null
            then strptime(datetime_beginning_ept, '%m/%d/%Y %H:%M') at time zone 'America/New_York'
            else cast(datetime_beginning_ept as timestamptz)
        end as hour_beginning_utc,

        -- location
        try_cast(pnode_id as integer) as pnode_id,
        trim(pnode_name)              as pnode_name,
        -- `type` is the PJM API field name; `pnode_type` is what backfill_eia writes
        coalesce(trim(type), trim(pnode_type)) as pnode_type,
        trim(zone)                    as zone,

        -- price components ($/MWh) — EIA may omit component columns
        try_cast(system_energy_price_da as double) as energy_price_da,
        try_cast(congestion_price_da    as double) as congestion_price_da,
        try_cast(marginal_loss_price_da as double) as loss_price_da,
        try_cast(total_lmp_da           as double) as total_lmp_da,

        -- source tag (eia | null for PJM API rows)
        coalesce(source, 'pjm') as source

    from source
)

select * from renamed
where total_lmp_da is not null  -- drop rows that failed price mapping
