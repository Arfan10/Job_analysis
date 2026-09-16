with source as (
    select
        source_id,
        job_title,
        unnest(skills) as skill
    from {{ source('Job_Analysis', 'raw_job_data') }}
    where skills is not null and array_length(skills, 1) > 0
)

select
    source_id,
    job_title,
    lower(trim(skill)) as skill
from source