{{ config(materialized='table') }}

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
    cast(null as date) as rep_date,  -- historical doesn't have this col
    cast(null as char) as run_date  -- historical doesn't have this col
from {{ source('staging', 'stg_yrp_crime_historical') }}
