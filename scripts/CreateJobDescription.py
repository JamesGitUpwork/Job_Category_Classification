from sqlalchemy import text

import pandas as pd
import numpy as np

class CreateJobDescription:

    def __init__(self,schema):
        self.schema = schema
        self.job_description_df = None
    
    def getMaxClassifyTextRunId(self,engine):
        temp = '''
        select max(run_predict_text_id) from {}.run_predict_text_id_tb
        '''
        model_query = temp.format(self.schema)
        with engine.connect() as con:
            query = text(model_query)
            rs = con.execute(query)

            maxRunId = rs.fetchone()[0]
        return str(maxRunId)

    def __setRunId(self,engine):
        temp = '''
        select distinct(run_create_description_id) from {}.run_create_description_id_tb
        '''
        model_query = temp.format(self.schema)
        # Extract job id, title, and description
        with engine.connect() as con:
            query = text(model_query)
            rs = con.execute(query)

            rows = rs.fetchall()

        id_list = pd.DataFrame(rows,columns=['run_create_description_id'])

        max_id = id_list['run_create_description_id'].max()

        if pd.isna(max_id):
            new_id = 1
        else:
            new_id = max_id + 1

        df = pd.DataFrame({'run_create_description_id':[new_id]})
        df.to_sql('run_create_description_id_tb',engine,schema=self.schema,if_exists='append',index=False)

        return new_id

    def createJobDescription(self,engine,text_extract_df):

        df = text_extract_df[text_extract_df['prediction']==1]
        grouped_df = df.groupby('job_id').apply(
            lambda x: f"{x['title'].iloc[0]}. " + " ".join(x['extract_text'])
        ).reset_index(name='description')

        maxRunId = self.getMaxClassifyTextRunId(engine)        
        run_create_description_id = self.__setRunId(engine)
        grouped_df['text_run_id'] = int(maxRunId)
        grouped_df['run_create_description_id'] = run_create_description_id

        self.job_description_df = grouped_df

    def getJobDescription(self):
        return self.job_description_df

    def insertJobDescription(self,engine):
        self.job_description_df.to_sql('job_description_tb',engine,schema=self.schema,if_exists='append',index=False)


