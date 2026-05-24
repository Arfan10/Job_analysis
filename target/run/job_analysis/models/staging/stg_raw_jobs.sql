
  create view "Job_Analysis"."public"."stg_raw_jobs__dbt_tmp"
    
    
  as (
    SELECT
    source_id,
    job_title,
    company,
    location,
    salary_min,
    salary_max,
    date_posted
FROM raw_job_data
WHERE job_title IS NOT NULL
  );