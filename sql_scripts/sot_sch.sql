create table sot_sch.health_and_medical_services_tb (
    id serial primary key,
    job_id integer,
    category varchar,
    prediction int,
    description text,
    datetime timestamp
);

create table sot_sch.laboratory_and_research_tb (
    id serial primary key,
    job_id integer,
    category varchar,
    prediction int,
    description text,
    datetime timestamp
);