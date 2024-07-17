from sqlalchemy import create_engine

class SqlConn:

    def __init__(self,username,password,host,port,database):
        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.database = database

    def connect(self):
        engine = create_engine(f'postgresql+psycopg2://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}')
        
        return engine
    
    # Create local db connection
    def __create_local_db_engine(self):
        username = self.config.get('local_db',{}).get('username')
        password = self.config.get('local_db',{}).get('password')
        db_host = self.config.get('local_db',{}).get('db_host')
        port = self.config.get('local_db',{}).get('post')
        database = self.config.get('local_db',{}).get('winthrop_dbb')

        engine = create_engine(f'postgresql+psycopg2://{username}:{password}@{db_host}:{port}/{database}')       

        return engine
    
