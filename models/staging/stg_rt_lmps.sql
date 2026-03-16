-- Staging: Real-time hourly LMPs
-- Source: PJM feed rt_hrl_lmps, written to data/rt_lmps/*.parquet by fetch_pjm.py
--
-- Key fields from the API:
--   datetime_beginning_ept, pnode_id, pnode_name, type, zone,
--   system_energy_price_rt, congestion_price_rt, marginal_loss_price_rt, total_lmp_rt

with source as (
    select * from read_parquet('{{ env_var("DATA_DIR", "data") }}/rt_lmps/*.parquet')
),

renamed as (
    select
        -- timestamps
        strptime(datetime_beginning_ept, '%m/%d/%Y %H:%M') as hour_beginning_ept,
        strptime(datetime_beginning_ept, '%m/%d/%Y %H:%M') at time zone 'America/New_York' as hour_beginning_utc,

        -- location
        cast(pnode_id as integer)  as pnode_id,
        trim(pnode_name)           as pnode_name,
        trim(type)                 as pnode_type,
        trim(zone)                 as zone,

        -- price components ($/MWh)
        cast(system_energy_price_rt as double) as energy_price_rt,
        cast(congestion_price_rt    as double) as congestion_price_rt,
        cast(marginal_loss_price_rt as double) as loss_price_rt,
        cast(total_lmp_rt           as double) as total_lmp_rt

    from source
)

select * from renamed
