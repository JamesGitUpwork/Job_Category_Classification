from sqlalchemy import MetaData, Table, insert, text
import pandas as pd

from ErrorHandler import ErrorHandler

class SchemaDataManager(ErrorHandler):

    def __init__(self,log_type):
        super().__init__(log_type)

    def transfer_job_posts(self,engine):
        try:
            with engine.connect() as conn:
                # Begin a transaction
                with conn.begin() as trans:
                    # Construct the SQL query
                    query = text('''
                        INSERT INTO data_sch.job_post_tb
                        SELECT * FROM current_sch.current_job_post_tb;
                        
                    ''')

                    # Execute the query
                    conn.execute(query)

                    query = text('''
                        INSERT INTO data_sch.job_post_tb
                        SELECT * FROM current_sch.current_job_post_tb;
                        
                    ''')

                    # Execute the query
                    conn.execute(query)

        except Exception as e:
            message = "Failed job posts data transfer from current_sch to data_sch"
            