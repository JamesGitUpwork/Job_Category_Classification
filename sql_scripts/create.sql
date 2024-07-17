CREATE TABLE local.job_post_tb (
    job_id INT NOT NULL,
    title VARCHAR,
    link VARCHAR,
    salary_summary VARCHAR,
    school_id INT NOT NULL,
    description_md TEXT,
    created_at timestamp,
    datetime timestamp default current_timestamp
);

CREATE TABLE local.extract_text_tb (
    text_id serial primary key,
    job_id integer,
    extract_text text,
    datetime timestamp default current_timestamp  
);


