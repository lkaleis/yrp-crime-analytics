{{ config(materialized='table') }}

select
  "GEO" as municipality,
  "Population and dwelling counts (13)" as population_and_dwelling_counts,
  "VALUE" as population_count
from {{ ref('censusdata2021_yorkregion') }}
where "Population and dwelling counts (13)" in ('Population, 2021')