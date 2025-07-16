from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import pandas as pd 
import os 
load_dotenv()


def initialize_session():
    psql_username = os.getenv('POSTGRES_USERNAME')
    psql_password = os.getenv('POSTGRES_PASSWORD')
    print(psql_password,psql_username)
    psql_connection_str = f'postgresql://{psql_username}:{psql_password}@localhost:5432/postgres'
    
    try:
        psql_engine = create_engine(psql_connection_str)
        psql_session = sessionmaker(bind=psql_engine)
        session = psql_session()
        return session
    except Exception as e:
        print(e)
        return None
    
def load_csv(session):
    load_dir = '/home/basel/main/analyitical_eng/data'
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

def main ():
    session= initialize_session()
    if session:
        load_csv(session)
if __name__== '__main__':
    main()