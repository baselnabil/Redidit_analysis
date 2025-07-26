from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import pandas as pd 
import os 
load_dotenv()
import sys 
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

load_dir = '/opt/airflow/data'

def initialize_session():
    psql_username = os.getenv('POSTGRES_USERNAME')
    psql_password = os.getenv('POSTGRES_PASSWORD')
    psql_connection_str = f'postgresql://{psql_username}:{psql_password}@analyitical_eng-postgres-1:5432/postgres'
    
    try:
        psql_engine = create_engine(psql_connection_str)
        psql_session = sessionmaker(bind=psql_engine)
        session = psql_session()
        print('session initialized')
        return session
    except Exception as e:
        print(e)
        return None
    

def create_check_columns(session):
    pass

def load_csv(session):
    files = os.listdir(load_dir)
    print(files)
    for file in files:
        if 'done' not in file:
            full_path= os.path.join(load_dir,file)
            chunks = pd.read_csv(full_path,chunksize=10000)
            
            try:
                for chunk in chunks:
                    chunk.to_sql(name='reddit_posts',if_exists='append',con=session.bind, index=False)
                    os.rename(full_path,full_path + '.done')
                    print(f"{file} loaded and marked as done.")

            except Exception as e :
                print(e)
                return False

def main ():
    session= initialize_session()
    if session:
        
        load_csv(session)
    else:
        print('session error ')
if __name__== '__main__':
    main()