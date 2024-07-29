create table data_sch.job_post_tb (
    job_run_id int,
    job_id int not null,
    title varchar,
    description_md text,
    created_at timestamp,
    datetime timestamp
);

create table data_sch.extract_text_tb (
    job_run_id int,
    text_id int not null,
    job_id integer not null,
    title varchar,
    extract_text text,
    datetime timestamp
);

create table data_sch.extract_text_prediction_tb (
    job_run_id int,
    text_id int,
    job_id integer,
    title varchar,
    extract_text text,
    prediction integer,
    probability float,
    text_model varchar,
    datetime timestamp
);

create table data_sch.job_description_tb (
    job_run_id int,
    description_id int,
    job_id integer,
    description text,
    datetime timestamp default current_timestamp
);

create table data_sch.job_category_prediction_tb (
    job_run_id int,
    predict_id int,
    job_id integer,
    description_id int,
    prediction integer,
    category varchar,
    probability float,
    vec_model varchar,
    category_model varchar,
    datetime timestamp
);

create table data_sch.data_error_logs_tb (
    id serial primary key,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    log_level VARCHAR(10),
    job_run_id int,
    message TEXT
)

create table data_sch.job_category_prediction_verification_tb (
    job_run_id int,
    predict_id int,
    job_id integer,
    description_id int,
    prediction integer,
    category varchar,
    probability float,
    vec_model varchar,
    category_model varchar,
    correct_category_prediction int,
    correct_category varchar,
    correct_description int,
    datetime timestamp
);