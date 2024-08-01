from SqlConn import SqlConn
import yaml
import pandas as pd

def load_config(file_path):
    with open(file_path,'r') as f:
        config = yaml.safe_load(f)
    return config

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

# Load verified data into data_sch
verified_file_path = './verified_data/verified_data.csv'

'''
df = pd.read_csv(verified_file_path)
table_name = 'job_category_prediction_verification_tb'
schema_name = 'data_sch'

df.to_sql(table_name,engine,if_exists='append',index=False,schema=schema_name)
'''

# Transform verified data from data_sch and load into sot_sch
temp = '''
select distinct(ca)
'''


temp = '''
select
a.job_id,
a.category,
1 as prediction,
b.description as description
from data_sch.job_category_prediction_verification_tb as a
left join data_sch.job_description_tb as b
	on b.description_id = a.description_id
	and a.job_run_id = b.job_run_id
where a.prediction = a.correct_category_prediction
union all
select
a.job_id,
a.category,
0 as prediction,
b.description as description
from data_sch.job_category_prediction_verification_tb as a
left join data_sch.job_description_tb as b
	on b.description_id = a.description_id
	and a.job_run_id = b.job_run_id
where a.prediction != a.correct_category_prediction
'''