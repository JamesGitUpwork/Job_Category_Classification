import yaml
from SqlConn import SqlConn
from ExtractText import ExtractText
from ClassifyText import ClassifyText
from CreateJobDescription import CreatJobDescription
from PredictJobCategory import PredictionJobCategory
from sqlalchemy import text

def load_config(file_path):
    with open(file_path,'r') as f:
        config = yaml.safe_load(f)
    return config

def predict_job_category(engine,schema,get_job_post_query,text_class_model,text_treshold=0.3):

    # Step 1: Extract text from description
    ExtractText_obj = ExtractText(schema)
    ExtractText_obj.extractText(engine,get_job_post_query)
    # ExtractText_obj.insertText(engine) -- must uncomment
    extract_text_df = ExtractText_obj.getText()

    # Step 2: 
    ClassifyText_obj = ClassifyText(schema)
    ClassifyText_obj.classifyText(extract_text_df,text_class_model,engine,text_treshold)
    classify_prediction_df = ClassifyText_obj.getTextPrediction()
    print(classify_prediction_df.head())
    #CreatJobDescription_obj = CreatJobDescription()
    #PredictionJobCategory_obj = PredictionJobCategory()

# Get name of text classification model
def getTextClassModel(engine,schema,version=0):
    if version == 0:
        temp = '''SELECT name 
        FROM test_sch.text_classification_model_tb
        WHERE version = (SELECT MAX(version) FROM {}.text_classification_model_tb)
        limit 1;
        '''
    else:
        temp = '''SELECT name 
        FROM {}.text_classification_model_tb
        WHERE version = ''' + str(version) + ''' limit 1;'''

    model_query = temp.format(schema)
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
    schema = "test_sch"

    # Job Post to categorize query
    temp = '''
        select distinct job_id, description_md from {}.job_post_tb
        where date(created_at) = '2024-01-01'
    '''
    get_job_post_query = temp.format(schema)

    # Models to use
    extract_text_model = getTextClassModel(engine,schema)
    text_class_threshold = 0.3

    # Predict job category
    predict_job_category(engine,
                         schema,
                         get_job_post_query,
                         extract_text_model,
                         text_treshold=text_class_threshold)

if __name__ == "__main__":
    run()