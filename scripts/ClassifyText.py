from sqlalchemy import text
import pandas as pd
import joblib

from ErrorHandler import ErrorHandler

class ClassifyText(ErrorHandler):

    def __init__(self,job_run_id,log_type):
        super().__init__(log_type)
        self.job_run_id = job_run_id
        self.predicted_text_description_df = None

    # Choose decision threshold
    def __recall_bias_predict(self,y_pred_prob,threshold):
        custom_predictions = (y_pred_prob >= threshold).astype(int)
        return custom_predictions
    
    def classifyText(self,engine,text_extract_df,model,threshold):
        try: 
            temp = './text_classification_model/{}.pkl'
            job_description_extract_file = temp.format(model)

            # Load the pipeline from the file
            job_description_model = joblib.load(job_description_extract_file)

            predicted_job_description = []
            for index, value in text_extract_df.iterrows():
                y_pred_prob = job_description_model.predict_proba([value['extract_text']])[:,1]
                y_pred = self.__recall_bias_predict(y_pred_prob,threshold)
                predicted_job_description.append([value['job_run_id'],value['text_id'],
                                                value['job_id'],value['title'],
                                                value['extract_text'],y_pred,y_pred_prob])
            
            columns = ['job_run_id','text_id','job_id','title','extract_text','prediction','probability']
            self.predicted_text_description_df = pd.DataFrame(predicted_job_description,columns=columns)
            self.predicted_text_description_df['prediction'] = self.predicted_text_description_df['prediction'].astype(int)
            self.predicted_text_description_df['probability'] = round(self.predicted_text_description_df['probability'].astype(float),2)
            self.predicted_text_description_df['text_model'] = model
        except Exception as e:
            message = "Classify Text Error"
            self.extract_classify_text_exception(engine,self.job_run_id,e,message)

    def getTextPrediction(self,engine):
        query = '''
        select job_run_id, text_id, job_id, title, extract_text, prediction
        from current_sch.current_extract_text_prediction_tb 
        ''' 
        with engine.connect() as con:
            text_query = text(query)
            rs = con.execute(text_query)
            rows = rs.fetchall()

        df = pd.DataFrame(rows,columns=['job_run_id','text_id','job_id','title','extract_text','prediction'])
        return df
    
    def insertTextPrediction(self,engine):
        self.predicted_text_description_df.to_sql('current_extract_text_prediction_tb',
                                                  engine,
                                                  schema='current_sch',
                                                  if_exists='append',index=False)