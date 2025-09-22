select
    "UniqueIdentifier",
    occ_date,
    case_type_pubtrans,
    "LocationCode",
    municipality,
    "Special_grouping",
    "OBJECTID",
    Shooting,
    occ_id,
    hate_crime,
    case_status,
    occ_type,
    cast(null as date) as rep_date  -- historical doesn't have this col
    cast(null as date) as run_date  -- historical doesn't have this col
from {{ source('york_crime', 'stg_yrp_crime_historical') }}
