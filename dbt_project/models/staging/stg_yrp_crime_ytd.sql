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
    rep_date,
    run_date
from {{ source('york_crime', 'stg_yrp_crime_ytd') }}
