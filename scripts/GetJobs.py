from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd

class GetJobs:

    def __init__(self,job_run_id):
        self.job_run_id = job_run_id
        self.latest_job_post_df = None
     
    # Need ability to retrieve job posts from CI and store in latest_job_post_tb
    def fetchLatestJobs(self,engine):
        temp = '''
        select distinct on (job_id) 
            {} as job_run_id, 
            job_id, title, description_md, created_at 
            from test_sch.latest_job_post_tb
        '''
        query = temp.format(self.job_run_id)
        with engine.connect() as con:
            text_query = text(query)
            rs = con.execute(text_query)

            rows = rs.fetchall()
        
        columns = ['job_run_id','job_id','title','description_md','created_at']
        self.latest_job_post_df = pd.DataFrame(rows,columns=columns)
    
    def getCurrentJobPosts(self):
        return self.latest_job_post_df

    def insertLatestJobs(self,engine):
        self.latest_job_post_df.to_sql('current_job_post_tb',
                                    engine,
                                    schema='current_sch',
                                    if_exists='append',
                                    index=False)