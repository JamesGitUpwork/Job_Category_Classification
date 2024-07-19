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