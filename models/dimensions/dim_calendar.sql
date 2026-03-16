-- Dimension: calendar spine at hourly grain (EPT)
-- Covers the full range of hours present in the mart tables.

with spine as (
    select unnest(
        generate_series(
            (select min(hour_beginning_ept) from {{ ref('stg_da_lmps') }}),
            (select max(hour_beginning_ept) from {{ ref('stg_da_lmps') }}),
            interval '1 hour'
        )
    ) as hour_beginning_ept
),

enriched as (
    select
        hour_beginning_ept,
        cast(hour_beginning_ept as date)          as date_ept,
        extract('year'  from hour_beginning_ept)  as year,
        extract('month' from hour_beginning_ept)  as month,
        extract('day'   from hour_beginning_ept)  as day,
        extract('hour'  from hour_beginning_ept)  as hour_of_day,
        extract('dow'   from hour_beginning_ept)  as day_of_week,  -- 0=Sunday
        dayname(hour_beginning_ept)               as day_name,
        monthname(hour_beginning_ept)             as month_name,
        -- PJM peak/off-peak (HE07-HE22 on weekdays = on-peak)
        case
            when extract('dow' from hour_beginning_ept) in (0, 6) then false
            when extract('hour' from hour_beginning_ept) between 6 and 21  then true
            else false
        end as is_peak
    from spine
)

select * from enriched
