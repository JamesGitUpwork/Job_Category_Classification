from sqlalchemy import text
from ErrorHandler import ErrorHandler
import pandas as pd
from winthrop_client_python.models.job_post_collection import JobPostCollection
from WinthropConfig import WinthropConfig

class GetJobs(ErrorHandler):

    def __init__(self,job_run_id,ci_host,log_type):
        super().__init__(log_type)
        self.job_run_id = job_run_id
        self.ci_host = ci_host
        self.latest_job_post_df = None
    
    def fetchLatestJobs(self):
        ransack_query = {
                    "categories_ancestry_start": "/1/", 
                    "s": "updated_at desc"  
                    }
        ci_instance = WinthropConfig.create_instance(self.ci_host)
        api_response = ci_instance.get_job_posts(q=ransack_query)
        columns = ['id','title','description_md','created_at']
        api_response_dict = JobPostCollection.to_dict(api_response)
        job_post_df = pd.DataFrame(api_response_dict['data'])
        temp = job_post_df[columns]
        temp.insert(0,'job_run_id',self.job_run_id)
        temp = temp.rename(columns={'id':'job_id'})
        
        self.latest_job_post_df = temp

    # Need ability to retrieve job posts from CI and store in latest_job_post_tb
    def fetchTestJobs(self,engine):
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

    def wipePreviousJobPosts(self,engine):
        tables = [
            'current_sch.current_job_category_prediction_tb',
            'current_sch.current_job_description_tb',
            'current_sch.current_extract_text_prediction_tb',
            'current_sch.current_extract_text_tb',
            'current_sch.current_job_post_tb'
        ]

        try:
            with engine.connect() as conn:
                with conn.begin() as trans:
                    for table in tables:
                        query = text(f'truncate table {table} restart identity cascade;')
                        conn.execute(query)
        except Exception as e:
            message = "Wipe previous job run data error"
            self.wipe_previous_job_posts_handle_exception(engine,self.job_run_id,e,message)

    def insertLatestJobs(self,engine):
        self.latest_job_post_df.to_sql('current_job_post_tb',
                                    engine,
                                    schema='current_sch',
                                    if_exists='append',
                                    index=False)