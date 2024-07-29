import logging
from sqlalchemy import MetaData, Table, insert, text
from JobRunControl import JobRunControl
import sys

class ErrorHandler:

    logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)

    def __init__(self,log_type='current'):
        if log_type == 'current':
            self.log_table = 'current_error_logs_tb'
            self.schema = 'current_sch'
        elif log_type == 'data':
            self.log_table = 'data_error_logs_tb'
            self.schema = 'data_sch'
        elif log_type == 'training':
            self.log_table = 'training_error_logs_tb'
            self.schema = 'train_sch'
        else:
            pass

    def log_exception(self,engine,job_run_id,exception,message='An error occurred'):
        log_level = 'ERROR'
        self.insert_log(engine,log_level,message,exception,job_run_id)

    def insert_log(self,engine,log_level,message,exception,job_run_id):
        error_message = str(exception)
        with engine.connect() as con:
            metadata = MetaData()
            my_table = Table(self.log_table,metadata,autoload_with=engine,schema=self.schema)
            stmt = insert(my_table).values(
                log_level = log_level,
                job_run_id=int(job_run_id),
                message = f"{message}: {error_message}"
            )

            con.execute(stmt)
            con.commit()

    # Handle errors from GetJobs
    def get_jobs_handle_exception(self,engine,job_run_id,exception,message='An error occurred'):
        self.log_exception(engine,job_run_id,exception,message)
        JobRunControl.updateFailedJobRunId(engine,job_run_id)
        sys.exit("Program terminated after handling get jobs exception.")

    # Handle errors from ExtractText
    def extract_text_handle_exception(self,engine,job_run_id,exception,message='An error occurred'):
        self.log_exception(engine,job_run_id,exception,message)
        JobRunControl.updateFailedJobRunId(engine,job_run_id)
        sys.exit("Program terminated after handling extract text exception.")

    # Handle errors from ClassifyText
    def extract_classify_text_exception(self,engine,job_run_id,exception,message='An error occurred'):
        self.log_exception(engine,job_run_id,exception,message)
        JobRunControl.updateFailedJobRunId(engine,job_run_id)
        sys.exit("Program terminated after handling classify text exception.")

    # Handle errors from JobDescription
    def create_job_description_handle_exception(self,engine,job_run_id,exception,message='An error occurred'):
        self.log_exception(engine,job_run_id,exception,message)
        JobRunControl.updateFailedJobRunId(engine,job_run_id)
        sys.exit("Program terminated after handling create job description exception.")

    # Handle errors from PredictJobCategory
    def pred_job_category_handle_exception(self,engine,job_run_id,exception,message='An error occurred'):
        self.log_exception(engine,job_run_id,exception,message)
        JobRunControl.updateFailedJobRunId(engine,job_run_id)
        sys.exit("Program terminated after handling predict job category exception.")

    # Handle errors from SchemaDataManager
    def store_current_job_run_data_handle_exception(self,engine,job_run_id,exception,message='An error ocurred'):
        self.log_exception(engine,job_run_id,exception,message)
        JobRunControl.updateFailedJobRunId(engine,job_run_id)
        sys.exit("Program terminated after handling current job data run transfer exception.")

    # Handle erros from previous job run data wipe
    def wipe_previous_job_posts_handle_exception(self,engine,job_run_id,exception,message='An error occured'):
        self.log_exception(engine,job_run_id,exception,message)
        JobRunControl.updateFailedJobRunId(engine,job_run_id)
        sys.exit("Program terminated after handling current job data run transfer exception.")

