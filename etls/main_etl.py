import praw
from dotenv import load_dotenv
import os
import pandas as pd
from datetime import datetime
import sys
load_dotenv()
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
def initialize_reddit_client():
    try:
        reddit = praw.Reddit(
        client_id=os.getenv("REDDIT_CLIENT_ID"),
        client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
        user_agent="User-Agent: Python:com.example.reddit-extractor:v1.0 (by /u/RootUser11)",
        username=os.getenv("REDDIT_USERNAME"),
        password=os.getenv("REDDIT_PASSWORD"),
    )
        print('reddit instance intiliazed')
        return reddit

    except Exception as e:
        print(f"Error initializing Reddit client: {e}")
        return None 
def extract_reddit_data(reddit):
    subreddit = reddit.subreddit("dataengineering")
    posts=[]
    mapped_keys = ('id','subreddit_id','author','selftext','title','over_18','created_utc','clicked','score')
    for post in subreddit.hot(limit=100000):
        posts_dict = vars(post)
        post_dict = {key : posts_dict.get(key) for key in mapped_keys}
        posts.append(post_dict)
    print('data extracted ')
    print(posts)

    return posts

def transform_data(posts):
    types_dict = {
        'id':'string',
        'subreddit_id':'string',
        'author':'string',
        'selftext':'string',
        'title':'string',
        'over_18':bool,
        'clicked':bool,
        'score':int,
    }
    posts_df = pd.DataFrame(posts)
    posts_df = posts_df.astype(types_dict)
    posts_df['created_utc'] = pd.to_datetime(posts_df['created_utc'], unit='s', errors='coerce')
    print('data transformed')
    print(posts_df)

    return posts_df

def load_data(df):
    timestamp = datetime.now().timestamp()
    full_path = os.path.join('/opt/airflow/data/',f'reddit_data_{int(timestamp)}.csv')
    df.to_csv(full_path, index=False)
    print(f'Data loaded to {full_path}')
def main():
    reddit = initialize_reddit_client()
    posts = extract_reddit_data(reddit)
    df =transform_data(posts)
    load_data(df)

if __name__ == "__main__":
    main()