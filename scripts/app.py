import yaml
from SqlConn import SqlConn
from GetJobs import GetJobs
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

def getJobPost(engine,schema):
    GetJobs_obj = GetJobs(schema)
    GetJobs_obj.fetchLatestJobs(engine)
    GetJobs_obj.insertLatestJobs(engine)

def predict_job_category(engine,job_run_id,get_job_post_query,text_class_model,
                         text_threshold=0.3,
                         category_threshold = 0.3
                         ):

    current_job_run_id = job_run_id

    # Step 1: Extract text from description
    ExtractText_obj = ExtractText()
    ExtractText_obj.extractText(engine,get_job_post_query)
    # ExtractText_obj.insertText(engine)
    extract_text_df = ExtractText_obj.getText(engine)

    # Step 2: Classify Text
    ClassifyText_obj = ClassifyText()
    ClassifyText_obj.classifyText(extract_text_df,text_class_model,engine,text_threshold)
    # ClassifyText_obj.insertTextPrediction(engine)
    classified_text_df = ClassifyText_obj.getTextPrediction()
    
    # Step 2: Create Job Desscription
    CreateJobDescription_obj = CreateJobDescription()
    CreateJobDescription_obj.createJobDescription(engine,classified_text_df)
    #CreateJobDescription_obj.insertJobDescription(engine)
    job_description_df = CreateJobDescription_obj.getJobDescription(engine)

    # Step 3: Predict Job Category
    PredictionJobCategory_obj = PredictJobCategory()
    PredictionJobCategory_obj.classifyJobDescription(engine,job_description_df,category_threshold)
    #PredictionJobCategory_obj.insertJobCategoryPrediction(engine)
    job_prediction_df = PredictionJobCategory_obj.getJobCategoryDescription()
    job_prediction_df.head()

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

def getJobRunId(engine):
    id_query = '''
    select max(job_run_id) from fact_sch.job_run_id_tb
    '''
    with engine.connect() as con:
        query = text(id_query)
        rs = con.execute(query)
        rows = rs.fetchall()
    
    id_list = pd.DataFrame(rows,columns=['job_run_id'])

    job_run_id = id_list['job_run_id'].max()
    
    print(type(job_run_id))
    print(job_run_id)

    if pd.isna(job_run_id):
        current_job_run_id = 1
    else:
        current_job_run_id = current_job_run_id + 1

    return current_job_run_id

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

    # Fetch Job Post
    # getJobPost(engine,schema)

    # Get current job_id
    current_job_run_id = getJobRunId(engine)

    # Get Job Post to categorize query
    temp = '''
    select distinct on (job_id) 
        {} as job_run_id, 
        job_id, title, description_md, created_at 
        from test_sch.latest_job_post_tb
    '''
    get_job_post_query = temp.format(current_job_run_id)

    # Models to use
    extract_text_model = getTextClassModel(engine)
    text_class_threshold = 0.3
    category_class_threshold = 0.3

    # Predict job category
    predict_job_category(engine,
                         current_job_run_id,
                         get_job_post_query,
                         extract_text_model,
                         text_threshold=text_class_threshold,
                         category_threshold=category_class_threshold)

if __name__ == "__main__":
    run()