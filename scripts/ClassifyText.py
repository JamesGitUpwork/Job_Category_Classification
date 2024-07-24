from sqlalchemy import text
import pandas as pd
import joblib

class ClassifyText:

    def __init__(self):
        self.predicted_text_description_df = None

    # Choose decision threshold
    def __recall_bias_predict(self,y_pred_prob,threshold):
        custom_predictions = (y_pred_prob >= threshold).astype(int)
        return custom_predictions
    
    # Set the new run id and return the value
    def __setRunId(self,engine):
        temp = '''
        select distinct(run_predict_text_id) from {}.run_predict_text_id_tb
        '''
        model_query = temp.format(self.schema)
        # Extract job id, title, and description
        with engine.connect() as con:
            query = text(model_query)
            rs = con.execute(query)

            rows = rs.fetchall()

        id_list = pd.DataFrame(rows,columns=['run_predict_text_id'])

        max_id = id_list['run_predict_text_id'].max()

        if pd.isna(max_id):
            new_id = 1
        else:
            new_id = max_id + 1

        df = pd.DataFrame({'run_predict_text_id':[new_id]})
        df.to_sql('run_predict_text_id_tb',engine,schema=self.schema,if_exists='append',index=False)

        return new_id

    def classifyText(self,text_extract_df,model,engine,threshold=0.8):
        temp = './text_classification_model/{}.pkl'
        job_description_extract_file = temp.format(model)

        # Load the pipeline from the file
        job_description_model = joblib.load(job_description_extract_file)

        predicted_job_description = []
        for index, value in text_extract_df.iterrows():
            y_pred_prob = job_description_model.predict_proba([value['extract_text']])[:,1]
            y_pred = self.__recall_bias_predict(y_pred_prob,threshold)
            predicted_job_description.append([value['text_id'],value['job_id'],value['title'],
                                            value['extract_text'],y_pred,y_pred_prob])
        
        columns = ['text_id','job_id','title','extract_text','prediction','probability']
        self.predicted_text_description_df = pd.DataFrame(predicted_job_description,columns=columns)
        self.predicted_text_description_df['prediction'] = self.predicted_text_description_df['prediction'].astype(int)
        self.predicted_text_description_df['probability'] = round(self.predicted_text_description_df['probability'].astype(float),2)
        self.predicted_text_description_df['text_model'] = model
        self.predicted_text_description_df['run_predict_text_id'] = self.__setRunId(engine)

    def getTextPrediction(self,engine):
        temp = '''
        select description_id, job_id, description from {schema}.job_description_tb 
        where job_id in (select distinct(job_id) from {schema}.latest_job_post_tb)
        ''' 
        query = temp.format(schema=self.schema)
        with engine.connect() as con:
            text_query = text(query)
            rs = con.execute(text_query)
            rows = rs.fetchall()

        df = pd.DataFrame(rows,columns=['description_id','job_id','description'])
        return df
    
    def insertTextPrediction(self,engine):
        self.predicted_text_description_df.to_sql('extract_text_prediction_tb',engine,schema=self.schema,if_exists='append',index=False)