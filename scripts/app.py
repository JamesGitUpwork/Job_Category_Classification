import yaml
from SqlConn import SqlConn
from ExtractText import ExtractText
from ClassifyText import ClassifyText
from CreateJobDescription import CreatJobDescription
from PredictJobCategory import PredictionJobCategory

def load_config(file_path):
    with open(file_path,'r') as f:
        config = yaml.safe_load(f)
    return config

def get_job_category(engine,schema,get_job_post_query):
    ExtractText_obj = ExtractText(schema)
    extractText_df = ExtractText_obj.getText(engine,get_job_post_query)
    print(extractText_df.head())
    # ExtractText_obj.insertText(engine) -- must uncomment

    #ClassifyText_obj = ClassifyText()
    #CreatJobDescription_obj = CreatJobDescription()
    #PredictionJobCategory_obj = PredictionJobCategory()


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

    # Predict job category
    get_job_category(engine,schema,get_job_post_query)

if __name__ == "__main__":
    run()