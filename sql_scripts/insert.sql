insert into test_sch.job_classification_model_tb (
	category, version
) values('Health and Medical Services',2);

CREATE OR REPLACE PROCEDURE test_sch.store_latest_job_post(schema_name TEXT)
LANGUAGE plpgsql
AS $$
BEGIN
    -- Construct the dynamic SQL statement
    EXECUTE format(
        'INSERT INTO %I.job_post_tb
         SELECT *
         FROM %I.latest_job_post_tb',
        schema_name, schema_name
    );
END;
$$;

-- Inserting seed data for each verification table in sot_sch
insert into sot_sch.health_and_medical_services_tb (
	job_id, category, prediction, description
)
select * from 
	(
		select
		job_id,
		predicted_category as category,
		1 as prediction,
		concat(title,'. ', description) as description
		from public.corrected_categorization_tb
		where predicted_category in ('Health and Medical Services')
		and tag = 1
		union all
		select
		job_id,
		'Health and Medical Services' as category,
		0 as prediction,
		concat(title,'. ', description) as description
		from public.job_categorization_vw 
		where predicted_category not in ('Health and Medical Services')
	)