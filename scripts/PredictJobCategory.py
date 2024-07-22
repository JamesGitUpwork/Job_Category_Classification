from sqlalchemy import create_engine
from sqlalchemy import text

import pandas as pd
import numpy as np

import joblib

class PredictJobCategory:

    def __init__(self,schema):
        self.schema = schema
        column_names = ['job_id','description_id','prediction','category','probability','description_run_id','run_predict_job_id']
        self.job_cat_prediction_df = pd.DataFrame(columns=column_names)

    def getMaxDescriptionRunId(self,engine):
        temp = '''
        select max(run_create_description_id) from {}.run_create_description_id_tb
        '''
        model_query = temp.format(self.schema)

        with engine.connect() as con:
            query = text(model_query)
            rs = con.execute(query)

            maxRunId = rs.fetchone()[0]
        return str(maxRunId)

    def __setRunId(self,engine):
        temp = '''
        select distinct(run_predict_job_id) from {}.run_predict_job_id_tb
        '''
        model_query = temp.format(self.schema)
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
        df.to_sql('run_predict_job_id_tb',engine,schema=self.schema,if_exists='append',index=False)

        return new_id
    
    def __getModels(self,engine,cat_version=0,vec_version=0):
        if cat_version and vec_version == 0:
            temp = '''
            select vec_tb.name as vec_name, cat_tb.name as cat_name, vec_tb.category as category
            from {schema}.vectorization_model_tb as vec_tb
            inner join {schema}.job_classification_model_tb as cat_tb
            on vec_tb.category = cat_tb.category
            where vec_tb.version = (select max(version) from {schema}.vectorization_model_tb)
            and cat_tb.version = (select max(version) from {schema}.job_classification_model_tb)
            '''
            model_query = temp.format(schema = self.schema)
        else:
            temp = '''
            select vec_tb.name as vec_name, cat_tb.name as cat_name, vec_tb.category as category
            from {schema}.vectorization_model_tb as vec_tb
            inner join {schema}.job_classification_model_tb as cat_tb
            on vec_tb.category = cat_tb.category
            where vec_tb.version = {vec}
            and cat_tb.version = {cat}
            '''
            model_query = temp.format(schema = self.schema,vec = vec_version,cat = cat_version)

        with engine.connect() as con:
            query = text(model_query)
            rs = con.execute(query)

            rows = rs.fetchall()
        
        model_name_df = pd.DataFrame(rows,columns=['vec_name','cat_name','category'])

        return model_name_df

    def classifyJobDescription(self,engine,job_description_df,category_threshold=0.3,cat_ver=0,vec_ver=0):
        # Get model names
        model_name_df = self.__getModels(engine,cat_version=cat_ver,vec_version=vec_ver)

        column_name = ['job_id','description_id','prediction','category','probability']
        prediction_df = pd.DataFrame(columns=column_name)

        for index, row in model_name_df.iterrows():
            
            # Get category name
            category = row['category']

            # Create job classification model file name
            cat_name = row['cat_name']
            temp = '/job_classification_model/{}.pkl'
            modelFile = temp.format(cat_name)

            # Create vectorization model file name
            vec_name = row['vec_name']
            temp = './vectorization_model/{}.pkl'
            vecFile = temp.format(vec_name)

            # Load models
            count_vectorizer = joblib.load(vecFile)
            model = joblib.load(modelFile)

            # Vectorization
            test_count = count_vectorizer.transform(job_description_df['description'])

            # Predict job category
            predictions = model.predict(test_count)

            # Adjust prediction based on threshold
            y_pred_prob = model.predict_proba(test_count)
            predictions = (y_pred_prob[:,1]>category_threshold).astype(int)
            probability = y_pred_prob[:,1]

            num_rows = len(predictions)

            cat = np.full(num_rows,category)

            job_id = job_description_df['job_id'].to_numpy()

            description_id = job_description_df['description_id'].to_numpy()

            temp = np.column_stack((job_id,description_id,predictions,cat,probability))

            temp_df = pd.DataFrame(temp,columns=column_name)

            prediction_df = pd.concat([prediction_df,temp_df],axis=0)

            prediction_run_id = self.__setRunId(engine)

            prediction_df['description_run_id'] = maxRunId
            prediction_df['run_predict_job_id'] = prediction_run_id

            self.job_cat_prediction_df = pd.concat([self.job_cat_prediction_df,prediction_df],ignore_index=True)

    def getJobCategoryDescription(self):
        return self.job_cat_prediction_df

    def insertJobCategoryPrediction(self,engine):
        self.job_cat_prediction_df.to_sql('job_category_prediction_tb',
                                          engine,schema=self.schema,if_exists='append',index=False)