import yaml
from scripts.SqlConn import SqlConn
from scripts.ExtractText import ExtractText
from scripts.ClassifyText import ClassifyText
from scripts.CreateJobDescription import CreatJobDescription
from scripts.PredictJobCategory import PredictionJobCategory

def load_config(file_path):
    with open(file_path,'r') as f:
        config = yaml.safe_load(f)
    return config

def get_job_category(engine,query):
    ExtractText(engine,query)


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
        select distinct job_id, title, description_md from {}.job_post_tb
    '''
    get_job_post_query = temp.format(schema)

    # Predict job category
    get_job_category(engine,get_job_post_query)



if __name__ == "__main__":
    run()