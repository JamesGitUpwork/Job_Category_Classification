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

create table test_sch.extract_text_prediction_tb (
    id serial primary key,
    text_id int references test_sch.extract_text_tb(text_id),
    job_id integer,
    extract_text text,
    prediction integer,
    probability float,
    text_model varchar,
    run_predict_text_id int,
    datetime timestamp default current_timestamp
);

create table test_sch.extract_text_tb (
    text_id serial primary key,
    job_id integer not null,
    extract_text text,
    datetime timestamp default current_timestamp  
);

create table test_sch.text_classification_model_tb (
    version int,
    name varchar
);

create table test_sch.job_classification_model_tb (
    category varchar,
    version int,
    name varchar
);

create table test_sch.vectorization_model_tb (
    category varchar,
    version int,
    name varchar
);

create table test_sch.run_predict_text_id_tb (
    run_predict_text_id int unique
);

create table test_sch.latest_job_post_tb (
    job_id int not null,
    title varchar,
    link varchar,
    salary_summary varchar,
    school_id int not null,
    description_md text,
    created_at timestamp,
    datetime timestamp default current_timestamp
);

create table test_sch.job_description_tb (
    description_id serial primary key,
    job_id integer,
    description text,
    text_run_id integer,
    run_create_description_id int,
    datetime timestamp default current_timestamp
);

create table test_sch.run_create_description_id_tb (
    run_create_description_id int unique
);

create table test_sch.run_predict_job_id_tb (
    run_predict_job_id int unique
);

create table test_sch.job_category_prediction_tb (
    predict_id serial primary key,
    job_id integer,
    description_id int references local.job_description_tb(description_id),
    prediction integer,
    category varchar,
    probability float,
    description_run_id integer,
    run_predict_job_id int,
    datetime timestamp default current_timestamp
);
