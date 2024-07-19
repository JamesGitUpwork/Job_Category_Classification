from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd

class GetJobs:

    def __init__(self,schema):
        self.schema = schema
        self.latest_job_post_df = None
     
    # Need ability to retrieve job posts from CI and store in latest_job_post_tb
    def fetchLatestJobs(self,engine):
        temp = '''
        select distinct on (job_id) * from {}.latest_job_post_tb
        '''
        query = temp.format(self.schema)
        with engine.connect() as con:
            text_query = text(query)
            rs = con.execute(text_query)

            rows = rs.fetchall()

        columns = ['job_id','title','link','salary_summary','school_id',
                   'description_md','created_at','datetime']
        self.latest_job_post_df = pd.DataFrame(rows,columns=columns)

    def getLatestJobs(self):
        return self.latest_job_post_df

    def insertLatestJobs(self,engine):
        self.latest_job_post_df.to_sql('job_post_tb',
                                    engine,
                                    schema=self.schema,
                                    if_exists='append',
                                    index=False)