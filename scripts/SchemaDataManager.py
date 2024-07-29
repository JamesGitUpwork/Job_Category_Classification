from sqlalchemy import MetaData, Table, insert, text
import pandas as pd

from ErrorHandler import ErrorHandler

class SchemaDataManager(ErrorHandler):

    def __init__(self,job_run_id,log_type):
        super().__init__(log_type)
        self.job_run_id = job_run_id

    def transfer_prediction_for_verification(self,engine):
        queries = {
            '''
            insert into data_sch.job_category_prediction_verification_tb (
                job_run_id,
                predict_id,
                job_id,
                description_id,
                prediction,
                category,
                probability,
                vec_model,
                category_model,
                datetime
            ) select 
                job_run_id,
                predict_id,
                job_id,
                description_id,
                prediction,
                category,
                probability,
                vec_model,
                category_model,
                datetime
            from data_sch.job_category_prediction_tb;
            ''': ('Job Category Prediction transferred successfully.','Failed to copy job category predictions for verification.')
        }

        with engine.connect() as conn:
            with conn.begin() as trans:
                for query, (success_message, failure_message) in queries.items():
                    try:
                        conn.execute(text(query))
                    except Exception as e:
                        self.store_current_job_run_data_handle_exception(engine, self.job_run_id, e, failure_message)

    def store_current_job_run_data(self,engine):
        queries = {
            '''
            INSERT INTO data_sch.job_post_tb
            SELECT * FROM current_sch.current_job_post_tb;
            ''': ('Job posts data transferred successfully.', 'Failed to transfer job posts data.'),
            
            '''
            INSERT INTO data_sch.extract_text_tb
            SELECT * FROM current_sch.current_extract_text_tb;
            ''': ('Extract text data transferred successfully.', 'Failed to transfer extract text data.'),
            
            '''
            INSERT INTO data_sch.extract_text_prediction_tb
            SELECT * FROM current_sch.current_extract_text_prediction_tb;
            ''': ('Extract text prediction data transferred successfully.', 'Failed to transfer extract text prediction data.'),
            
            '''
            INSERT INTO data_sch.job_description_tb
            SELECT * FROM current_sch.current_job_description_tb;
            ''': ('Job description data transferred successfully.', 'Failed to transfer job description data.'),
            
            '''
            INSERT INTO data_sch.job_category_prediction_tb
            SELECT * FROM current_sch.current_job_category_prediction_tb;
            ''': ('Job category prediction data transferred successfully.', 'Failed to transfer job category prediction data.')
        }

        with engine.connect() as conn:
            with conn.begin() as trans:
                for query, (success_message, failure_message) in queries.items():
                    try:
                        conn.execute(text(query))
                    except Exception as e:
                        self.store_current_job_run_data_handle_exception(engine, self.job_run_id, e, failure_message)

