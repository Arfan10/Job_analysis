select
    job_title,
    count(distinct skill) as skill_count,
    array_agg(distinct skill order by skill) as skills
from {{ ref('stg_job_skills') }}
group by job_title
order by skill_count desc