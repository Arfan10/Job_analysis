with staging_jobs as (

    select * from {{ ref('stg_jobs') }}

),

unique_locations as (

    select distinct
        trim(primary_location) as primary_location
    from staging_jobs
    where primary_location is not null

),

final_dim as (

    select
        -- Generate a reliable hash key based on the unique location string
        md5(primary_location) as location_key,
        primary_location

    from unique_locations

)

select * from final_dim