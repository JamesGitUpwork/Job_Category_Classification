create table current_sch.current_job_post_tb (
    job_run_id int references fact_sch.job_run_id_tb(job_run_id),
    job_id int not null,
    title varchar,
    description_md text,
    created_at timestamp,
    datetime timestamp default current_timestamp
);

create table current_sch.current_extract_text_tb (
    job_run_id int references fact_sch.job_run_id_tb(job_run_id),
    text_id serial primary key,
    job_id integer not null,
    title varchar,
    extract_text text,
    datetime timestamp default current_timestamp
);

create table current_sch.current_extract_text_prediction_tb (
    job_run_id int references fact_sch.job_run_id_tb(job_run_id),
    text_id int references current_sch.current_extract_text_tb(text_id),
    job_id integer,
    title varchar,
    extract_text text,
    prediction integer,
    probability float,
    text_model varchar references fact_sch.text_classification_model_tb(name),
    datetime timestamp default current_timestamp
);

create table current_sch.current_job_description_tb (
    job_run_id int references fact_sch.job_run_id_tb(job_run_id),
    description_id serial primary key,
    job_id integer,
    description text,
    datetime timestamp default current_timestamp
);

create table current_sch.current_job_category_prediction_tb (
    job_run_id int references fact_sch.job_run_id_tb(job_run_id),
    predict_id serial primary key,
    job_id integer,
    description_id int references current_sch.current_job_description_tb(description_id),
    prediction integer,
    category varchar,
    probability float,
    description_run_id integer,
    vec_model varchar references fact_sch.vectorization_model_tb(name),
    category_model varchar references fact_sch.job_classification_model_tb(name),
    datetime timestamp default current_timestamp
);
