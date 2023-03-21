import configparser
from sqlalchemy import create_engine

config = configparser.ConfigParser()
config.read("config.ini")

host = config["DEFAULT"]["host"]
port = config["DEFAULT"]["port"]
username = config["database"]["username"]
password = config["database"]["password"]
database_name = config["database"]["database_name"]

engine = create_engine(
    f"postgresql://{username}:{password}@{host}:{port}/{database_name}"
)