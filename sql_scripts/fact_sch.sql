create table fact_sch.job_run_id_tb (
    job_run_id serial primary key,
    text_classification_threshold float,
    job_classification_threshold float,
    status int not null,
    datetime timestamp default current_timestamp,
    constraint status_check check (status in (0,1,2))
);

create table fact_sch.text_classification_model_tb (
    version int,
    name varchar unique
);

create table fact_sch.job_classification_model_tb (
    category varchar,
    version int,
    name varchar unique
);

create table fact_sch.vectorization_model_tb (
    category varchar,
    version int,
    name varchar unique
);