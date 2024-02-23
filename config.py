import os
from dotenv import load_dotenv


load_dotenv()

MYSQL_USER = os.environ.get("MYSQL_USER")
MYSQL_PASSWORD = os.environ.get("MYSQL_PASSWORD")
MYSQL_DATABASE = os.environ.get("MYSQL_DATABASE")
MYSQL_ROOT_PASSWORD = os.environ.get("MYSQL_ROOT_PASSWORD")


ELASTIC_USERNAME = os.environ.get("ELASTIC_USERNAME")
ELASTIC_PASSWORD = os.environ.get("ELASTIC_PASSWORD")
