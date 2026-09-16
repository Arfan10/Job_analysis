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
    -- CHANGED: was filtering on created_at, which only catches brand-new
    -- rows and would never re-select a job whose is_active status flipped
    -- (its created_at never changes, only updated_at does). Filtering on
    -- updated_at picks up new rows, still-active re-confirmations, and
    -- deactivations in the same incremental pass.
    where updated_at > (select max(updated_at) from {{ this }})
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

        -- Salary band: only average min/max when BOTH are present.
        -- Falling back to coalesce(...,0) silently drags the average down
        -- when only one side is disclosed, which misclassifies roles
        -- (e.g. salary_min NULL, salary_max 70000 was previously bucketed
        -- as 'Junior' instead of 'Mid'/'Senior').
        case
            when salary_min is null and salary_max is null then 'Not Disclosed'

            when salary_min is null then
                case
                    when salary_max < 45000 then 'Junior'
                    when salary_max between 45000 and 75000 then 'Mid'
                    when salary_max > 75000 then 'Senior'
                    else 'Unclassified'
                end

            when salary_max is null then
                case
                    when salary_min < 45000 then 'Junior'
                    when salary_min between 45000 and 75000 then 'Mid'
                    when salary_min > 75000 then 'Senior'
                    else 'Unclassified'
                end

            else
                case
                    when (salary_min + salary_max) / 2 < 45000 then 'Junior'
                    when (salary_min + salary_max) / 2 between 45000 and 75000 then 'Mid'
                    when (salary_min + salary_max) / 2 > 75000 then 'Senior'
                    else 'Unclassified'
                end
        end as salary_band,

        is_active,
        last_seen_at,
        date_posted,
        created_at,
        updated_at,
        current_timestamp as ingestion_date

    from staging_jobs

)

select * from final_mart