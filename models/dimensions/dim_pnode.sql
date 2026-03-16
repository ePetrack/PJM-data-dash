-- Dimension: PNode reference (zones and hubs)
-- Populated from the DA LMP staging model — all distinct pnodes seen in history.
-- Enriched with well-known aliases.

with pnodes_from_lmps as (
    select distinct
        pnode_id,
        pnode_name,
        pnode_type,
        zone
    from {{ ref('stg_da_lmps') }}
    where pnode_id is not null
),

enriched as (
    select
        pnode_id,
        pnode_name,
        pnode_type,
        zone,
        -- friendly label used in the dashboard
        case
            when pnode_id = 51217                   then 'Western Hub'
            when pnode_type = 'ZONE' and zone = 'DUQ' then 'DUQ Zone'
            else pnode_name
        end as display_name,
        -- group for filter dropdowns
        case
            when pnode_type = 'HUB'  then 'Hub'
            when pnode_type = 'ZONE' then 'Zone'
            else 'Other'
        end as pnode_group
    from pnodes_from_lmps
)

select * from enriched
