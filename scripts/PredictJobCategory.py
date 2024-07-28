from sqlalchemy import create_engine
from sqlalchemy import text
from ErrorHandler import ErrorHandler
from SchemaDataManager import SchemaDataManager

import pandas as pd
import numpy as np

import joblib

class PredictJobCategory(ErrorHandler):

    def __init__(self,job_run_id,log_type):
        column_names = ['job_run_id','job_id','description_id','prediction',
                       'category','probability','vec_model','category_model']
        self.job_cat_prediction_df = pd.DataFrame(columns=column_names)
        super().__init__(log_type)
        self.job_run_id = job_run_id
    
    def __getModels(self,engine,cat_version=0,vec_version=0):
        if cat_version == 0 and vec_version == 0:
            model_query = '''
            select vec_tb.name as vec_name, cat_tb.name as cat_name, vec_tb.category as category
            from fact_sch.vectorization_model_tb as vec_tb
            inner join fact_sch.job_classification_model_tb as cat_tb
            on vec_tb.category = cat_tb.category
            where vec_tb.version = (select max(version) from fact_sch.vectorization_model_tb)
            and cat_tb.version = (select max(version) from fact_sch.job_classification_model_tb)
            '''
        else:
            temp = '''
            select vec_tb.name as vec_name, cat_tb.name as cat_name, vec_tb.category as category
            from fact_sch.vectorization_model_tb as vec_tb
            inner join fact_sch.job_classification_model_tb as cat_tb
            on vec_tb.category = cat_tb.category
            where vec_tb.version = {vec}
            and cat_tb.version = {cat}
            '''
            model_query = temp.format(vec = vec_version,cat = cat_version)

        with engine.connect() as con:
            query = text(model_query)
            rs = con.execute(query)

            rows = rs.fetchall()
        
        model_name_df = pd.DataFrame(rows,columns=['vec_name','cat_name','category'])

        return model_name_df

    def classifyJobDescription(self,engine,job_description_df,category_threshold=0.3,cat_ver=0,vec_ver=0):
        try:
            # Get model names
            model_name_df = self.__getModels(engine,cat_version=cat_ver,vec_version=vec_ver)

            column_name = ['job_run_id','job_id','description_id','prediction',
                        'category','probability','vec_model','category_model']

            for index, row in model_name_df.iterrows():
                
                # Get category name
                category = row['category']

                # Create job classification model file name
                cat_name = row['cat_name']
                temp = './job_classification_model/{}.pkl'
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

                vec_col = np.full(num_rows,vec_name)

                model_col = np.full(num_rows,cat_name)

                job_run_id = job_description_df['job_run_id'].to_numpy()

                job_id = job_description_df['job_id'].to_numpy()

                description_id = job_description_df['description_id'].to_numpy()

                temp = np.column_stack((job_run_id,job_id,
                                        description_id,predictions,
                                        cat,probability,
                                        vec_col,model_col))

                temp_df = pd.DataFrame(temp,columns=column_name)

                self.job_cat_prediction_df = pd.concat([self.job_cat_prediction_df,temp_df],axis=0)
        except Exception as e:
            message = "Predict Job Category Error"
            self.pred_job_category_handle_exception(engine,self.job_run_id,e,message)

    def getJobCategoryDescription(self,engine):
        query = '''
        select job_run_id, job_id, description_id, prediction,
        category, probability, vec_model, category_model
        from current_sch.current_job_category_prediction_tb 
        ''' 
        with engine.connect() as con:
            text_query = text(query)
            rs = con.execute(text_query)
            rows = rs.fetchall()

        df = pd.DataFrame(rows,columns=['job_run_id','job_id','description_id','prediction',
                                        'category','probability','vec_model','category_model'])
        return df

    def insertJobCategoryPrediction(self,engine):
        self.job_cat_prediction_df.to_sql('current_job_category_prediction_tb',
                                          engine,
                                          schema='current_sch',
                                          if_exists='append',index=False)