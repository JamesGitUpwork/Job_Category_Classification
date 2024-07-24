from sqlalchemy import text
import pandas as pd

class ExtractText:

    def __init__(self):
        self.extract_text_df = None

    def __extract_description(self,job_post,output_ls):
        job_id = job_post['job_id']
        title = job_post['title']
        text = job_post['description_md']
        text_ls = text.split('\n\n')
        text_ls = [s for s in text_ls if s.strip()]
        threshold_length = 50
        for i, line in enumerate(text_ls):
            if len(line) > threshold_length:
                output = [job_id,title,line]
                output_ls.append(output)
        
        return output_ls

    def extractText(self,engine,query):
        # Extract job id, title, and description
        with engine.connect() as con:
            text_query = text(query)
            rs = con.execute(text_query)

            rows = rs.fetchall()

        job_posts_info = pd.DataFrame(rows,columns=['job_id','title','description_md'])

        output_ls = []
        flag = 0 # Having a flag maybe uncessary
        for index, job_post in job_posts_info.iterrows():
            if flag % 100 == 0:
                print(flag)
            flag = flag + 1
            output_ls = self.__extract_description(job_post,output_ls)

        columns = ['job_id','title','text']
        text_df = pd.DataFrame(output_ls,columns=columns)

        self.extract_text_df = text_df[['job_id','title','text']]
        self.extract_text_df.set_axis(['job_id','title','extract_text'],axis=1,inplace=True)
        
    def getText(self,engine):
        temp = '''
        select text_id, job_id, title, extract_text from current_sch.extract_text_tb 
        where job_id in (select distinct(job_id) from current_sch.current_job_post_tb)
        ''' 
        query = temp.format(schema=self.schema)
        with engine.connect() as con:
            text_query = text(query)
            rs = con.execute(text_query)
            rows = rs.fetchall()

        df = pd.DataFrame(rows,columns=['text_id','job_id','title','extract_text'])
        return df
    
    def insertText(self,engine):
        self.extract_text_df.to_sql('current_extract_text_tb',
                                    engine,
                                    schema='current_sch',
                                    if_exists='append',
                                    index=False)
