{{ config(materialized='table') }}

-- models/core/fct_crime_per_capita.sql
with york_population as (
    select population_count
    from dim_population
    where municipality = 'York'
),

crime as (
    select
        municipality,
        occ_type,
        extract(year from occ_date) as year,
        count (distinct uniqueidentifier) as crime_count
    from {{ ref('fct_yrp_crime') }}
    where extract(year from occ_date) = 2021
    group by municipality, occ_type, extract(year from occ_date)
),

population as (
    select
        municipality,
        population_count,
        2021 as census_year
    from {{ ref('dim_population') }}
),

crime_per_capita as (

    select
        c.year,
        c.municipality,
        c.occ_type,
        c.crime_count as total_crimes,
        p.population_count,
        round(c.crime_count::numeric / p.population_count::numeric * 100000::numeric,2) as crimes_per_100k
    from crime c
    join population p
      on c.municipality = p.municipality
    group by 1,2,3,4,5

),

region_summary as (

    select
        year,
        'York Region' as municipality,
        occ_type,
        sum(total_crimes) as total_crimes,
        p.population_count,
        round(sum(total_crimes)::numeric / p.population_count::numeric * 100000::numeric,2) as crimes_per_100k 
    from crime_per_capita c 
    cross join york_population p
    group by 1,3,5

)

select * from crime_per_capita
union all
select * from region_summary
order by municipality, crimes_per_100k desc