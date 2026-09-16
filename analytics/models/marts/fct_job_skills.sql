select
    skill,
    count(distinct source_id) as posting_count,
    count(distinct job_title) as distinct_titles
from {{ ref('stg_job_skills') }}
group by skill
order by posting_count desc