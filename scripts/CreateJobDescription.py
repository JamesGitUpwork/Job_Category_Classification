from sqlalchemy import text

import pandas as pd
import numpy as np

class CreateJobDescription:

    def getMaxClassifyTextRunId(self,engine):
        model_query = '''
        select max(run_predict_text_id) from local.run_predict_text_id_tb
        '''
        with engine.connect() as con:
            query = text(model_query)
            rs = con.execute(query)

            maxRunId = rs.fetchone()[0]
        return str(maxRunId)

    def __setRunId(self,engine):
        model_query = '''
        select distinct(run_create_description_id) from local.run_create_description_id_tb
        '''
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
        df.to_sql('run_create_description_id_tb',engine,schema='local',if_exists='append',index=False)

        return new_id

    def getJobDescription(self,engine,probability_threshold):
        temp = '''
        select job_id, extract_text, probability, text_model from local.extract_text_prediction_tb where run_predict_text_id = {}
        '''
        maxRunId = self.getMaxClassifyTextRunId(engine)
        text_description_query = temp.format(maxRunId)

        # Extract job id, title, and description
        with engine.connect() as con:
            query = text(text_description_query)
            rs = con.execute(query)

            rows = rs.fetchall()

        predicted_job_description_df = pd.DataFrame(rows,columns=['job_id','extract_text','probability','text_model'])

        job_title_query = '''
        select distinct(job_id), title from local.job_post_tb
        '''

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

        print(grouped_testing_data.head())

        grouped_testing_data.to_sql('job_description_tb',engine,schema='local',if_exists='append',index=False)

        print('Completed group description script with run_create_description_id: {run_create_description_id}')




