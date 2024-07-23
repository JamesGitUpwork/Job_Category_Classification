create table data_sch.job_post_tb (
    job_run_id int references fact_sch.job_run_id_tb(job_run_id),
    job_id int not null,
    title varchar,
    description_md text,
    created_at timestamp,
    datetime timestamp default current_timestamp
);

create table data_sch.extract_text_tb (
    job_run_id int references fact_sch.job_run_id_tb(job_run_id),
    text_id int not null,
    job_id integer not null,
    title varchar,
    extract_text text,
    datetime timestamp default current_timestamp  
);

create table data_sch.extract_text_prediction_tb (
    job_run_id int references fact_sch.job_run_id_tb(job_run_id),
    text_id int,
    job_id integer,
    title varchar,
    extract_text text,
    prediction integer,
    probability float,
    text_model varchar references fact_sch.vectorization_model_tb(name),
    datetime timestamp default current_timestamp
);

create table data_sch.job_description_tb (
    job_run_id int references fact_sch.job_run_id_tb(job_run_id),
    description_id int,
    job_id integer,
    description text,
    datetime timestamp default current_timestamp
);

create table data_sch.job_category_prediction_tb (
    job_run_id int references fact_sch.job_run_id_tb(job_run_id),
    predict_id int,
    job_id integer,
    description_id int,
    prediction integer,
    category varchar,
    probability float,
    description_run_id integer,
    vec_model varchar references fact_sch.vectorization_model_tb(name),
    category_model varchar references fact_sch.vectorization_model_tb(name),
    datetime timestamp default current_timestamp
);
