from sqlalchemy import text
import pandas as pd

class JobRunControl:

    def __init__(self):
        self.current_job_run_id = None

    def getJobRunId(self,engine,text_threshold,class_threshold):
        id_query = '''
        select max(job_run_id) from fact_sch.job_run_id_tb
        '''
        with engine.connect() as con:
            query = text(id_query)
            rs = con.execute(query)
            rows = rs.fetchall()
        
        id_list = pd.DataFrame(rows,columns=['job_run_id'])

        job_run_id = id_list['job_run_id'].max()

        if pd.isna(job_run_id):
            self.current_job_run_id = 1
        else:
            self.current_job_run_id = self.current_job_run_id + 1

        self.__insertJobRunId(engine,self.current_job_run_id,text_threshold,class_threshold)

        return self.current_job_run_id

    def __insertJobRunId(self,engine,job_run_id,text_threshold,class_threshold):
        data = {
            'job_run_id': [job_run_id],
            'text_classification_threshold': [text_threshold],
            'job_classification_threshold': [class_threshold],
            'status': [2]
        }

        df = pd.DataFrame(data)

        df.to_sql('job_run_id_tb',
                engine,
                schema='fact_sch',
                if_exists='append',
                index=False
                )
