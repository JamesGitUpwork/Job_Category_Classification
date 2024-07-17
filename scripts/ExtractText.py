from sqlalchemy import text
import pandas as pd

class ExtractText:

    def __init__(self):
        pass

    def __extract_description(self,job_post,output_ls):
        job_id = job_post['job_id']
        text = job_post['description_md']
        text_ls = text.split('\n\n')
        text_ls = [s for s in text_ls if s.strip()]
        threshold_length = 50
        for i, line in enumerate(text_ls):
            if len(line) > threshold_length:
                output = [job_id,job_post['title'],line]
                output_ls.append(output)
        
        return output_ls

    def extractText(self,engine,schema,query):
        # Extract job id, title, and description
        with engine.connect() as con:
            text_query = text(query)
            rs = con.execute(text_query)

            rows = rs.fetchall()

        job_posts_info = pd.DataFrame(rows,columns=['job_id','title','description_md'])

        output_ls = []
        flag = 0
        for index, job_post in job_posts_info.iterrows():
            if flag % 100 == 0:
                print(flag)
            flag = flag + 1
            output_ls = self.__extract_description(job_post,output_ls)

        columns = ['job_id','title','text']
        text_df = pd.DataFrame(output_ls,columns=columns)

        extract_text_df = text_df[['job_id','text']]
        extract_text_df.set_axis(['job_id','extract_text'],axis=1,inplace=True)
        extract_text_df.to_sql('extract_text_tb',engine,schema=schema,if_exists='append',index=False)
        print(extract_text_df.head())
        
        return extract_text_df['job_id'].nunique()