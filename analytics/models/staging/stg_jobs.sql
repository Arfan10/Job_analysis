with source as (

    select * from {{ source('Job_Analysis', 'raw_job_data') }}

),

renamed_and_cast as (

    select
        -- Identifiers
        cast(id as varchar) as job_id,
        cast(source_id as varchar) as source,

        -- Job Core Details
        cast(job_title as varchar) as job_title,
        cast(company as varchar) as company_name,
        cast(job_descr as varchar) as job_description,

        -- Location Data
        cast(location as varchar) as primary_location,

        -- Salary Data
        cast(salary_min as numeric) as salary_min,
        cast(salary_max as numeric) as salary_max,

        -- Temporal Data
        -- FIXED: date_posted (when the job was listed) and created_at
        -- (when we first ingested the row) were previously collapsed into
        -- one column, which silently discarded the real created_at.
        cast(date_posted as timestamp) as date_posted,
        cast(created_at as timestamp) as created_at,
        cast(last_seen_at as timestamp) as last_seen_at,
        cast(updated_at as timestamp) as updated_at,

        -- Availability tracking (added for is_active support in gold)
        is_active

    from source

),

filtered as (

    select *
    from renamed_and_cast
    -- Filter out test records or incomplete API pushes missing core fields
    where job_id is not null
      and job_title is not null
      and primary_location is not null

)

select * from filtered