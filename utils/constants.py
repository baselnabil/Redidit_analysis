import configparser
import os

parser = configparser.ConfigParser()
parser.read(os.path.join(os.path.dirname(__file__), '../config/.conf'))

parser.get('Reddit','REDDIT_CLIENT_ID')