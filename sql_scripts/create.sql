create table test_sch.job_post_tb (
    job_id int not null,
    title varchar,
    link varchar,
    salary_summary varchar,
    school_id int not null,
    description_md text,
    created_at timestamp,
    datetime timestamp default current_timestamp
);

create table test_sch.extract_text_tb (
    text_id serial primary key,
    job_id integer not null,
    extract_text text,
    datetime timestamp default current_timestamp  
);

