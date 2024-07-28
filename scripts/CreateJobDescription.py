from sqlalchemy import text
from ErrorHandler import ErrorHandler

import pandas as pd
import numpy as np

class CreateJobDescription(ErrorHandler):

    def __init__(self,job_run_id,log_type):
        super().__init__(log_type)
        self.job_run_id = job_run_id
        self.job_description_df = None

    # Creates job description 
    def createAndInsertJobDescription(self,engine):
        try:
            with engine.connect() as conn:
                with conn.begin() as trans:
                    query = text('''
                        insert into current_sch.current_job_description_tb (job_run_id, job_id, description)
                        (
                            select 
                            job_run_id,
                            job_id,
                            title as description
                            from current_sch.current_extract_text_prediction_tb
                            group by
                            job_run_id, job_id, title
                            having sum(prediction) = 0
                        )
                        union
                        (
                            select 
                                job_run_id,
                                job_id,
                                string_agg(title || '. ' || extract_text,' ') as description
                            from current_sch.current_extract_text_prediction_tb
                            where prediction = 1
                            group by
                            job_run_id, job_id
                        )
                         ''')
                    conn.execute(query)
        except Exception as e:
            message = "Create Job Description Error"
            self.create_job_description_handle_exception(engine,self.job_run_id,e,message)

    def getJobDescription(self,engine):
        query = '''
        select job_run_id, description_id, job_id, description
        from current_sch.current_job_description_tb 
        ''' 
        with engine.connect() as con:
            text_query = text(query)
            rs = con.execute(text_query)
            rows = rs.fetchall()

        df = pd.DataFrame(rows,columns=['job_run_id','description_id','job_id','description'])
        return df


