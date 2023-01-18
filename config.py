RECREATE_DB = False


class BaseConfig(object):
    SQLALCHEMY_DATABASE_URI = "sqlite:///./app.db"
    STREETS_CSV_PATH = "./data/Streets_Jan2023.csv"
    MAX_DAYS_BACK = 100
    MAX_POSTS = 400
    
    # Network
    SERVER_PORT = 9000

SELECTED_CONFIG = BaseConfig
