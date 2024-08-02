from SqlConn import SqlConn
import yaml
import pandas as pd
from sqlalchemy import text

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


df = pd.read_csv(verified_file_path)
table_name = 'job_category_prediction_verification_tb'
schema_name = 'data_sch'

df.to_sql(table_name,engine,if_exists='append',index=False,schema=schema_name)


# Transform verified data from data_sch and load into sot_sch
def convert_to_table_name(input_string):
    # Convert to lowercase
    input_string = input_string.lower()
    # Replace spaces with underscores
    table_name = input_string.replace(' ', '_')
    # Add '_tb' suffix
    table_name += '_tb'
    return table_name

temp = '''
select distinct(category) from data_sch.job_category_prediction_verification_tb
'''
with engine.connect() as conn:
    query = text(temp)
    rs = conn.execute(query)
    rows = rs.fetchall()

distinct_category = pd.DataFrame(rows,columns=['category'])

for index, row in distinct_category.iterrows():
    table_name = convert_to_table_name(row['category'])
    with engine.connect() as conn:
        with conn.begin() as trans:
            temp = '''
            insert into sot_sch.{table} (
            job_id, category, prediction, description
            ) select * from (
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
            and a.category = '{cat}'
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
            and a.category = '{cat}'
            )
            '''
            query = temp.format(cat=row['category'],table=table_name)
            conn.execute(text(query))
    
# Store Statistical Results Per Job Run