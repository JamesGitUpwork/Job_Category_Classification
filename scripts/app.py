import yaml
from SqlConn import SqlConn
from GetJobs import GetJobs
from JobRunControl import JobRunControl
from ExtractText import ExtractText
from ClassifyText import ClassifyText
from CreateJobDescription import CreateJobDescription
from PredictJobCategory import PredictJobCategory
from sqlalchemy import text

import pandas as pd

def load_config(file_path):
    with open(file_path,'r') as f:
        config = yaml.safe_load(f)
    return config

def predict_job_category(engine,text_class_model,text_threshold=0.8,category_threshold=0.3):

    # Step 0: Get current job_id
    current_job_run_id = JobRunControl().getJobRunId(engine,text_threshold,category_threshold)

    # Step 1: Get Job Posts
    GetJobPosts_obj = GetJobs(current_job_run_id,'current')
    GetJobPosts_obj.fetchLatestJobs(engine)
    GetJobPosts_obj.insertLatestJobs(engine)
    job_posts_df = GetJobPosts_obj.getCurrentJobPosts()

    # Step 2: Extract text from description
    ExtractText_obj = ExtractText(current_job_run_id,'current')
    ExtractText_obj.extractText(engine)
    #ExtractText_obj.insertText(engine)
    extract_text_df = ExtractText_obj.getText(engine)

    # Step 3: Classify Text
    ClassifyText_obj = ClassifyText(current_job_run_id,'current')
    #ClassifyText_obj.classifyText(extract_text_df,text_class_model,text_threshold)
    #ClassifyText_obj.insertTextPrediction(engine)
    classified_text_df = ClassifyText_obj.getTextPrediction(engine)

    # Step 4: Create Job Desscription
    CreateJobDescription_obj = CreateJobDescription(current_job_run_id,'current')
    #CreateJobDescription_obj.createJobDescription(classified_text_df)
    #CreateJobDescription_obj.insertJobDescription(engine)
    job_description_df = CreateJobDescription_obj.getJobDescription(engine)

    # Step 5: Predict Job Category
    PredictionJobCategory_obj = PredictJobCategory(current_job_run_id,'current')
    #PredictionJobCategory_obj.classifyJobDescription(engine,job_description_df,category_threshold)
    #PredictionJobCategory_obj.insertJobCategoryPrediction(engine)
    job_prediction_df = PredictionJobCategory_obj.getJobCategoryDescription(engine)

    # Step 6: Update JobRunId

# Get name of text classification model
def getTextClassModel(engine,version=0):
    if version == 0:
        model_query = '''SELECT name 
        FROM fact_sch.text_classification_model_tb
        WHERE version = (SELECT MAX(version) FROM fact_sch.text_classification_model_tb)
        limit 1;
        '''
    else:
        model_query = '''SELECT name 
        FROM fact_sch.text_classification_model_tb
        WHERE version = ''' + str(version) + ''' limit 1;'''

    with engine.connect() as con:
        query = text(model_query)
        rs = con.execute(query)

        row = rs.fetchall()
    name = row[0][0]
    return name

def run():
    # Get local database variables
    config = load_config('./configuration/config.yaml')
    db_host = config['local_db']['host']
    db_port = config['local_db']['port']
    database = config['local_db']['database']
    username = config['local_db']['username']
    password = config['local_db']['password']

    # Create database connection instance
    sqlconn_obj = SqlConn(username,password,db_host,db_port,database)
    engine = sqlconn_obj.connect()

    # Models to use (This needs to be updated to get all model types)
    extract_text_model = getTextClassModel(engine)

    # Configure thresholds
    text_class_threshold = 0.8
    category_class_threshold = 0.3

    # Predict job category
    predict_job_category(engine,
                         extract_text_model,
                         text_threshold=text_class_threshold,
                         category_threshold=category_class_threshold)

if __name__ == "__main__":
    run()