{{ config(
    materialized='table',
    unique_key='uniqueidentifier'
) }}

with historical as (
    select 
        "uniqueidentifier",
        occ_date,
        case_type_pubtrans,
        "locationcode",
        municipality,
        "special_grouping",
        "objectid",
        shooting,
        occ_id,
        hate_crime,
        case_status,
        occ_type,
        rep_date,
        run_date
        from {{ ref('stg_yrp_crime_historical') }}
),
ytd as (
    select 
        "uniqueidentifier",
        occ_date,
        case_type_pubtrans,
        "locationcode",
        municipality,
        "special_grouping",
        "objectid",
        shooting,
        occ_id,
        hate_crime,
        case_status,
        occ_type,
        rep_date,
        run_date
    from {{ ref('stg_yrp_crime_ytd') }}
),

unioned as (
    select * from historical
    union all
    select * from ytd
),

deduped as (
    select
        *,
        row_number() over (
            partition by uniqueidentifier, occ_date
            order by 
                occ_date desc  
        ) as rn
    from unioned
)

select * from deduped
where rn = 1