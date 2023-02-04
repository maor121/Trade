import logging
from logging.config import dictConfig

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s in %(module)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S')

# Init Flask app
# Flask logging config
dictConfig({
    'version': 1,
    'formatters': {'default': {
        'format': '[%(asctime)s] %(levelname)s in %(module)s: %(message)s',
    }},
    'handlers': {'wsgi': {
        'class': 'logging.StreamHandler',
        'stream': 'ext://sys.stdout',
        'formatter': 'default'
    }},
    'root': {
        'level': 'INFO',
        'handlers': ['wsgi']
    }
})


RECREATE_DB = False


class BaseConfig(object):
    SQLALCHEMY_DATABASE_URI = "sqlite:///./app.db"
    STREETS_CSV_PATH = "./data/Streets_Jan2023.csv"
    MAX_DAYS_BACK = 100
    MAX_POSTS = 400
    
    # Network
    SERVER_PORT = 9000


SELECTED_CONFIG = BaseConfig
