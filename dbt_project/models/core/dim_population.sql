{{ config(materialized='table') }}

-- models/core/dim_population.sql
select *
from {{ ref('stg_yr_population') }}
