SELECT
    company,
    job_title,
    salary_max
FROM {{ ref('stg_jobs') }}