{{ config(
    materialized='table',
    unique_key='uniqueidentifier'
) }}

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
    from {{ ref('stg_yrp_crime_union') }}