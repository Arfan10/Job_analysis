{{
    config(
        materialized='incremental',
        unique_key='job_id',
        on_schema_change='sync_all_columns'
    )
}}

with staging_jobs as (

    select * from {{ ref('stg_jobs') }}

    {% if is_incremental() %}
    -- Only look at listings fresher than the latest record we already processed
    where created_at > (select max(created_at) from {{ this }})
    {% endif %}

),

final_mart as (

    select
        job_id,
        job_title,
        company_name,
        md5(company_name) as company_key,
        md5(trim(primary_location)) as location_key,
        job_description,
        primary_location,
        
        -- Compute remote flag inline
        case 
            when lower(job_title) like '%remote%' or lower(primary_location) like '%remote%' then true
            else false
        end as is_remote,
        
        salary_min,
        salary_max,
        
        -- Compute salary band based on an average calculation from min and max
        case
            when salary_min is null and salary_max is null then 'Not Disclosed'
            when (coalesce(salary_min, 0) + coalesce(salary_max, 0)) / 2 < 45000 then 'Junior'
            when (coalesce(salary_min, 0) + coalesce(salary_max, 0)) / 2 between 45000 and 75000 then 'Mid'
            when (coalesce(salary_min, 0) + coalesce(salary_max, 0)) / 2 > 75000 then 'Senior'
            else 'Unclassified'
        end as salary_band,
        
        created_at,
        current_timestamp as ingestion_date
    from staging_jobs

)

select * from final_mart