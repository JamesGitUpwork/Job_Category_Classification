from sqlalchemy import MetaData, Table, update, text
import pandas as pd

class JobRunControl:

    @staticmethod
    def setJobRunId(engine,text_threshold,class_threshold):
        id_query = '''
        select max(job_run_id) from fact_sch.job_run_id_tb
        '''
        with engine.connect() as con:
            query = text(id_query)
            rs = con.execute(query)
            rows = rs.fetchall()
        
        id_list = pd.DataFrame(rows,columns=['job_run_id'])

        job_run_id = id_list['job_run_id'].max()

        if pd.isna(job_run_id):
            current_job_run_id = 1
        else:
            current_job_run_id = job_run_id + 1

        JobRunControl.__insertJobRunId(engine,current_job_run_id,text_threshold,class_threshold,2)

        return current_job_run_id
    
    @staticmethod
    def __insertJobRunId(engine,job_run_id,text_threshold,class_threshold,status):
        data = {
            'job_run_id': [job_run_id],
            'text_classification_threshold': [text_threshold],
            'job_classification_threshold': [class_threshold],
            'status': [status]
        }

        df = pd.DataFrame(data)

        df.to_sql('job_run_id_tb',
                engine,
                schema='fact_sch',
                if_exists='append',
                index=False
                )

    @staticmethod
    def updateFailedJobRunId(engine,current_job_run_id):

        with engine.connect() as con:
            metadata = MetaData()
            my_table = Table('job_run_id_tb', metadata, autoload_with=engine, schema='fact_sch')
            stmt = update(my_table).where(my_table.c.job_run_id == int(current_job_run_id)).values(status=0)

            # Execute the update statement
            con.execute(stmt)
            con.commit()

    @staticmethod
    def updateSuccessJobRunId(engine,current_job_run_id):

        with engine.connect() as con:
            metadata = MetaData()
            my_table = Table('job_run_id_tb', metadata, autoload_with=engine, schema='fact_sch')
            stmt = update(my_table).where(my_table.c.job_run_id == int(current_job_run_id)).values(status=1)

            # Execute the update statement
            con.execute(stmt)
            con.commit()