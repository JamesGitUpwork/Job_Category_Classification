from sqlalchemy import text
from ErrorHandler import ErrorHandler
import pandas as pd

class GetJobs(ErrorHandler):

    def __init__(self,job_run_id,log_type):
        super().__init__(log_type)
        self.job_run_id = job_run_id
        self.latest_job_post_df = None
     
    # Need ability to retrieve job posts from CI and store in latest_job_post_tb
    def fetchLatestJobs(self,engine):
        try:
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

                if not rows:
                    raise ValueError("No job posts retreived.")
            
            columns = ['job_run_id','job_id','title','description_md','created_at']
            self.latest_job_post_df = pd.DataFrame(rows,columns=columns)
        except Exception as e:
            message = "Job Posts Fetching Error"
            self.get_jobs_handle_exception(engine,self.job_run_id,e,message)
    
    def getCurrentJobPosts(self):
        return self.latest_job_post_df

    def insertLatestJobs(self,engine):
        self.latest_job_post_df.to_sql('current_job_post_tb',
                                    engine,
                                    schema='current_sch',
                                    if_exists='append',
                                    index=False)