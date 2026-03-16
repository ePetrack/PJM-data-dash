-- Mart: merged DA + RT hourly LMP fact
-- One row per (hour_beginning_utc, pnode_id).
-- Includes DA–RT spread for price convergence analysis.
--
-- Incremental strategy: append new hours as they arrive.

{{
    config(
        materialized = 'incremental',
        unique_key = ['hour_beginning_utc', 'pnode_id'],
        on_schema_change = 'append_new_columns'
    )
}}

with da as (
    select
        hour_beginning_utc,
        hour_beginning_ept,
        pnode_id,
        pnode_name,
        pnode_type,
        zone,
        energy_price_da,
        congestion_price_da,
        loss_price_da,
        total_lmp_da
    from {{ ref('stg_da_lmps') }}

    {% if is_incremental() %}
    where hour_beginning_utc > (select max(hour_beginning_utc) from {{ this }})
    {% endif %}
),

rt as (
    select
        hour_beginning_utc,
        pnode_id,
        energy_price_rt,
        congestion_price_rt,
        loss_price_rt,
        total_lmp_rt
    from {{ ref('stg_rt_lmps') }}

    {% if is_incremental() %}
    where hour_beginning_utc > (select max(hour_beginning_utc) from {{ this }})
    {% endif %}
),

joined as (
    select
        da.hour_beginning_utc,
        da.hour_beginning_ept,
        da.pnode_id,
        da.pnode_name,
        da.pnode_type,
        da.zone,

        -- DA prices
        da.energy_price_da,
        da.congestion_price_da,
        da.loss_price_da,
        da.total_lmp_da,

        -- RT prices (may be null if RT not yet published)
        rt.energy_price_rt,
        rt.congestion_price_rt,
        rt.loss_price_rt,
        rt.total_lmp_rt,

        -- DA–RT spread (positive = DA priced above RT)
        case
            when rt.total_lmp_rt is not null
            then da.total_lmp_da - rt.total_lmp_rt
        end as da_rt_spread

    from da
    left join rt
        on  da.pnode_id           = rt.pnode_id
        and da.hour_beginning_utc = rt.hour_beginning_utc
)

select * from joined
