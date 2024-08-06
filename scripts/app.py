import yaml
from SqlConn import SqlConn
from GetJobs import GetJobs
from JobRunControl import JobRunControl
from ExtractText import ExtractText
from ClassifyText import ClassifyText
from CreateJobDescription import CreateJobDescription
from PredictJobCategory import PredictJobCategory
from sqlalchemy import text
from SchemaDataManager import SchemaDataManager
import logging

def load_config(file_path):
    with open(file_path,'r') as f:
        config = yaml.safe_load(f)
    return config

def predict_job_category(engine,ci_host,text_class_model,text_threshold=0.8,category_threshold=0.3):
    
    # Step 0: Get current job_id
    current_job_run_id = JobRunControl().setJobRunId(engine,text_threshold,category_threshold)
    logging.info(f"Obtained job_run_id: {current_job_run_id}")
    print(f"Obtained job_run_id: {current_job_run_id}")

    # Step 1: Get Job Posts
    GetJobPosts_obj = GetJobs(current_job_run_id,ci_host,'current')
    GetJobPosts_obj.fetchLatestJobs()
    GetJobPosts_obj.wipePreviousJobPosts(engine)
    GetJobPosts_obj.insertLatestJobs(engine)
    logging.info("Successfully fetched and inserted latest job posts")
    print("Successfully fetched and inserted latest job posts")

    # Step 2: Extract text from description
    logging.info("Step 1: Fetching and inserting latest job posts")
    ExtractText_obj = ExtractText(current_job_run_id,'current')
    ExtractText_obj.extractText(engine)
    ExtractText_obj.insertText(engine)
    extract_text_df = ExtractText_obj.getText(engine)
    logging.info("Successfully extracted text from job descriptions")
    print("Successfully extracted text from job descriptions")
    
    # Step 3: Classify Text
    logging.info("Step 3: Classifying text")
    ClassifyText_obj = ClassifyText(current_job_run_id,'current')
    ClassifyText_obj.classifyText(engine,extract_text_df,text_class_model,text_threshold)
    ClassifyText_obj.insertTextPrediction(engine)
    logging.info("Successfully classified text")
    print("Successfully classified text")
    
    # Step 4: Create Job Description
    logging.info("Step 4: Creating job descriptions")
    CreateJobDescription_obj = CreateJobDescription(current_job_run_id,'current')
    CreateJobDescription_obj.createAndInsertJobDescription(engine)    
    job_description_df = CreateJobDescription_obj.getJobDescription(engine)
    logging.info("Successfully created job descriptions")
    print("Successfully created job descriptions")

    # Step 5: Predict Job Category
    logging.info("Step 5: Predicting job categories")
    PredictionJobCategory_obj = PredictJobCategory(current_job_run_id,'current')
    PredictionJobCategory_obj.classifyJobDescription(engine,job_description_df,category_threshold)
    PredictionJobCategory_obj.insertJobCategoryPrediction(engine)
    logging.info("Successfully predicted job categories")
    print("Successfully predicted job categories")

    # Step 6: Update JobRunId
    logging.info("Step 6: Updating job_run_id with success status")
    JobRunControl().updateSuccessJobRunId(engine,current_job_run_id)
    logging.info("Successfully updated job_run_id with success status")
    print("Successfully updated job_run_id with success status")
    
    # Step 7: Copy current job run data to data_sch
    logging.info("Step 7: Copy current job run data to data_sch")
    SchemaDataManager_obj = SchemaDataManager(current_job_run_id,'data')
    #SchemaDataManager_obj.store_current_job_run_data(engine)
    SchemaDataManager_obj.transfer_prediction_for_verification(engine)
    logging.info("Successfully transfer current job run data to data_sch")
    print("Successfully transfer current job run data to data_sch")

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
    ci_host = config['winthrop_ci']['host']

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
                         ci_host,
                         extract_text_model,
                         text_threshold=text_class_threshold,
                         category_threshold=category_class_threshold)

if __name__ == "__main__":
    run()