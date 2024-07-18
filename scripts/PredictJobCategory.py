from sqlalchemy import create_engine
from sqlalchemy import text

import pandas as pd
import numpy as np

import joblib

class PredictJobCategory:

    def getMaxDescriptionRunId(self,engine):
        model_query = '''
        select max(run_create_description_id) from local.run_create_description_id_tb
        '''
        with engine.connect() as con:
            query = text(model_query)
            rs = con.execute(query)

            maxRunId = rs.fetchone()[0]
        return str(maxRunId)

    def __setRunId(self,engine):
        model_query = '''
        select distinct(run_predict_job_id) from local.run_predict_job_id_tb
        '''
        # Extract job id, title, and description
        with engine.connect() as con:
            query = text(model_query)
            rs = con.execute(query)

            rows = rs.fetchall()

        id_list = pd.DataFrame(rows,columns=['run_predict_job_id'])

        max_id = id_list['run_predict_job_id'].max()

        if pd.isna(max_id):
            new_id = 1
        else:
            new_id = max_id + 1

        df = pd.DataFrame({'run_predict_job_id':[new_id]})
        df.to_sql('run_predict_job_id_tb',engine,schema='local',if_exists='append',index=False)

        return new_id
    
    def predictCategorization(self,engine):
        temp = '''
        select 
        des.description_id,
        des.job_id,
        concat(job.title,'. ',des.description) as description
        from local.job_description_tb as des
        left join local.job_post_tb as job
        on des.job_id = job.job_id
        where des.run_create_description_id = {}
        '''
        maxRunId = self.getMaxDescriptionRunId(engine)
        description_query = temp.format(maxRunId)

        # Extract job id, title, and description
        with engine.connect() as con:
            query = text(description_query)
            rs = con.execute(query)

            rows = rs.fetchall()

        job_description_df = pd.DataFrame(rows,columns=['description_id','job_id','description'])

        to_predict_categories = [
            'Laboratory and Research',
            'Health and Medical Services'
        ]

        column_name = ['job_id','description_id','prediction','category','probability']
        prediction_df = pd.DataFrame(columns=column_name)

        for category in to_predict_categories:
            temp = 'C:/Users/yimin/upwork/winthrop/job_classification_analysis/Job_Posting_Classification/models/{}_classification_model_v2.pkl'
            modelFile = temp.format(category)

            temp = 'C:/Users/yimin/upwork/winthrop/job_classification_analysis/Job_Posting_Classification/models/{}_classification_vec_v2.pkl'
            vecFile = temp.format(category)

            count_vectorizer = joblib.load(vecFile)
            model = joblib.load(modelFile)

            test_count = count_vectorizer.transform(job_description_df['description'])
            #predictions = model.predict(test_count)

            # Decision Threshold Control
            threshold = 0.3
            y_pred_prob = model.predict_proba(test_count)
            predictions = (y_pred_prob[:,1]>threshold).astype(int)
            probability = y_pred_prob[:,1]

            num_rows = len(predictions)

            cat = np.full(num_rows,category)

            job_id = job_description_df['job_id'].to_numpy()

            description_id = job_description_df['description_id'].to_numpy()

            temp = np.column_stack((job_id,description_id,predictions,cat,probability))

            temp_df = pd.DataFrame(temp,columns=column_name)

            prediction_df = pd.concat([prediction_df,temp_df],axis=0)

            prediction_run_id = self.__setRunId(engine)
            prediction_model = 'category_prediction_v1.0'
            prediction_df['category_model'] = prediction_model
            prediction_df['description_run_id'] = maxRunId
            prediction_df['run_predict_job_id'] = prediction_run_id

        print(prediction_df.head())

        schema = 'local'

        prediction_df.to_sql('job_category_prediction_tb',engine,schema=schema,if_exists='append',index=False)

        print(f"Completed Job Category Prediction. The run_predict_job_id: {prediction_run_id}")