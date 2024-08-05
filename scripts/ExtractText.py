from sqlalchemy import text
import pandas as pd

from ErrorHandler import ErrorHandler

class ExtractText(ErrorHandler):

    def __init__(self,job_run_id,log_type):
        super().__init__(log_type)
        self.extract_text_df = None
        self.job_run_id = job_run_id

    def __extract_description(self,job_post,output_ls):
        job_run_id = job_post['job_run_id']
        job_id = job_post['job_id']
        title = job_post['title']
        text = job_post['description_md']
        text_ls = text.split('\n\n')
        text_ls = [s for s in text_ls if s.strip()]
        threshold_length = 50
        for i, line in enumerate(text_ls):
            if len(line) > threshold_length:
                output = [job_run_id,job_id,title,line]
                output_ls.append(output)
        
        return output_ls

    def extractText(self,engine):
        try:
            # Extract job_run_id, job id, title, and description
            query = '''
            select job_run_id, job_id, title, description_md
            from current_sch.current_job_post_tb
            '''

            with engine.connect() as con:
                text_query = text(query)
                rs = con.execute(text_query)

                rows = rs.fetchall()

            job_posts_info = pd.DataFrame(rows,columns=['job_run_id','job_id','title','description_md'])

            output_ls = []
            for index, job_post in job_posts_info.iterrows():
                output_ls = self.__extract_description(job_post,output_ls)

            columns = ['job_run_id','job_id','title','extract_text']
            self.extract_text_df = pd.DataFrame(output_ls,columns=columns)
        except Exception as e:
            message = "Extract Text Error"
            self.extract_text_handle_exception(engine,self.job_run_id,e,message)
            
    def getText(self,engine):
        query = '''
        select job_run_id, text_id, job_id, title, extract_text 
        from current_sch.current_extract_text_tb 
        ''' 
        with engine.connect() as con:
            text_query = text(query)
            rs = con.execute(text_query)
            rows = rs.fetchall()

        df = pd.DataFrame(rows,columns=['job_run_id','text_id','job_id','title','extract_text'])
        return df
    
    def __cleanseText(self,engine):
        # Cleanse text
        # Attempt to reduce unnesscary text
        pass

    def insertText(self,engine):
        self.extract_text_df.to_sql('current_extract_text_tb',
                                    engine,
                                    schema='current_sch',
                                    if_exists='append',
                                    index=False)
