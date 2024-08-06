from sqlalchemy import text
import pandas as pd
from ErrorHandler import ErrorHandler

class RedundantStorage(ErrorHandler):

    def __init__(self,log_type):
        super().__init__(log_type)

    def storeVerifiedData(self,engine):
        pass

    def storeDataSch(self,engine):
        queries = {
            '''
            INSERT INTO redundant_storage_sch.job_post_tb
            SELECT * FROM data_sch.job_post_tb;
            ''': ('Job posts data transferred successfully.', 'Failed to transfer job posts data.'),
            
            '''
            INSERT INTO redundant_storage_sch.extract_text_tb
            SELECT * FROM data_sch.extract_text_tb;
            ''': ('Extract text data transferred successfully.', 'Failed to transfer extract text data.'),
            
            '''
            INSERT INTO redundant_storage_sch.extract_text_prediction_tb
            SELECT * FROM data_sch.extract_text_prediction_tb;
            ''': ('Extract text prediction data transferred successfully.', 'Failed to transfer extract text prediction data.'),
            
            '''
            INSERT INTO redundant_storage_sch.job_description_tb
            SELECT * FROM data_sch.job_description_tb;
            ''': ('Job description data transferred successfully.', 'Failed to transfer job description data.'),
            
            '''
            INSERT INTO redundant_storage_sch.job_category_prediction_tb
            SELECT * FROM data_sch.job_category_prediction_tb;
            ''': ('Job category prediction data transferred successfully.', 'Failed to transfer job category prediction data.')
        }
        
        with engine.connect() as conn:
            with conn.begin() as trans:
                for query, (success_message, failure_message) in queries.items():
                    try:
                        conn.execute(text(query))
                    except Exception as e:
                        self.store_current_job_run_data_handle_exception(engine, self.job_run_id, e, failure_message)
