from sqlalchemy import text
import pandas as pd

class ExtractText:

    def __init__(self,schema):
        self.schema = schema
        self.extract_text_df = None

    def __extract_description(self,job_post,output_ls):
        job_id = job_post['job_id']
        text = job_post['description_md']
        text_ls = text.split('\n\n')
        text_ls = [s for s in text_ls if s.strip()]
        threshold_length = 50
        for i, line in enumerate(text_ls):
            if len(line) > threshold_length:
                output = [job_id,line]
                output_ls.append(output)
        
        return output_ls

    def extractText(self,engine,query):
        # Extract job id, title, and description
        with engine.connect() as con:
            text_query = text(query)
            rs = con.execute(text_query)

            rows = rs.fetchall()

        job_posts_info = pd.DataFrame(rows,columns=['job_id','description_md'])

        output_ls = []
        flag = 0 # Having a flag maybe uncessary
        for index, job_post in job_posts_info.iterrows():
            if flag % 100 == 0:
                print(flag)
            flag = flag + 1
            output_ls = self.__extract_description(job_post,output_ls)

        columns = ['job_id','text']
        text_df = pd.DataFrame(output_ls,columns=columns)

        self.extract_text_df = text_df[['job_id','text']]
        self.extract_text_df.set_axis(['job_id','extract_text'],axis=1,inplace=True)
        
    def getText(self,engine):
            temp = '''
            select * from {}.extract_text_tb where job_id 
            '''
            # Continue work here
            # Need to find a way to store and get the lastest job post to process
    
    def insertText(self,engine):
        self.extract_text_df.to_sql('extract_text_tb',
                                    engine,
                                    schema=self.schema,
                                    if_exists='append',
                                    index=False)
