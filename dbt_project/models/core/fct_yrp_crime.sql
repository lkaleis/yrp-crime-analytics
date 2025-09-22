with historical as (
    select * from {{ ref('stg_yrp_crime_historical') }}
),
ytd as (
    select * from {{ ref('stg_yrp_crime_ytd') }}
)

select * from historical
union all
select * from ytd
