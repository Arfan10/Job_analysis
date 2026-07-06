with staging_jobs as (

    select * from {{ ref('stg_jobs') }}

),

unique_companies as (

    select distinct
        company_name
    from staging_jobs
    where company_name is not null

),

final_dim as (

    select
        -- Generate a reliable hash key based on the company name
        md5(company_name) as company_key,
        company_name

    from unique_companies

)

select * from final_dim