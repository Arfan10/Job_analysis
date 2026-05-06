create Table raw_job_data (
    id SERIAL PRIMARY KEY,
    source_id VARCHAR(255) UNIQUE NOT NULL,
    job_title VARCHAR(255),
    company VARCHAR(255),
    location VARCHAR(255),
    salary_min INTEGER(255),
    salary_max INTEGER(255),
    job_description TEXT,
    date_posted TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
);



select * from raw_job_data;