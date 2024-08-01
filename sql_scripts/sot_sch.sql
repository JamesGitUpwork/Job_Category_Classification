create table sot_sch.health_and_medical_services_tb (
    id serial primary key,
    job_id integer,
    category varchar,
    prediction int,
    description text,
    datetime timestamp
);

select
	0 as job_run_id,
	job_id,
	predicted_category as category,
	tag as prediction,
	concat(title,'. ', description) as description
	from public.corrected_categorization_tb
	where predicted_category in ('Laboratory and Research','Health and Medical Services')
	and tag = 1

select
	0 as job_run_id,
	job_id,
	predicted_category as category,
	1 as prediction,
	concat(title,'. ', description) as description
	from public.job_categorization_vw 
	where predicted_category not in ('Laboratory and Research','Health and Medical Services')