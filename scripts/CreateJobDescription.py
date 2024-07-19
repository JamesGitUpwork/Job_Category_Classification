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

    def createJobDescription(self,engine,probability_threshold):
        temp = '''
        select job_id, extract_text, probability, text_model from {}.extract_text_prediction_tb where run_predict_text_id = {}
        '''
        maxRunId = self.getMaxClassifyTextRunId(engine)
        text_description_query = temp.format(self.schema,maxRunId)

        # Extract job id, title, and description
        with engine.connect() as con:
            query = text(text_description_query)
            rs = con.execute(query)

            rows = rs.fetchall()

        predicted_job_description_df = pd.DataFrame(rows,columns=['job_id','extract_text','probability','text_model'])

        # Sort within each group
        temp = predicted_job_description_df.groupby('job_id').apply(
            lambda x: x.sort_values(by='probability',ascending=False)).reset_index(drop=True)

        sorted_testing_data = temp[temp['probability'] >= probability_threshold]

        grouped_testing_data = sorted_testing_data.groupby('job_id').agg(
            {'extract_text': lambda x: ' '.join(x)}).reset_index()
        
        run_create_description_id = self.__setRunId(engine)
        grouped_testing_data.set_axis(['job_id','description'],axis=1,inplace=True)
        grouped_testing_data['text_run_id'] = int(maxRunId)
        grouped_testing_data['run_create_description_id'] = run_create_description_id

        self.job_description_df = grouped_testing_data

        print(grouped_testing_data.head())

    def insertJobDescription(self,engine):
        self.job_description_df.to_sql('job_description_tb',engine,schema=self.schema,if_exists='append',index=False)


