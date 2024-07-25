from sqlalchemy import text

import pandas as pd
import numpy as np

class CreateJobDescription:

    def __init__(self):
        self.job_description_df = None

    def createJobDescription(self,text_extract_df):

        df = text_extract_df[text_extract_df['prediction']==1]
        grouped_df = df.groupby('job_id').apply(
            lambda x: f"{x['title'].iloc[0]}. " + " ".join(x['extract_text'])
        ).reset_index(name='description')

        job_run_id = text_extract_df['job_run_id'].unique()[0]
        grouped_df.insert(0,'job_run_id',job_run_id)

        self.job_description_df = grouped_df

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

    def insertJobDescription(self,engine):
        self.job_description_df.to_sql('current_job_description_tb',
                                       engine,
                                       schema='current_sch',
                                       if_exists='append',index=False)


