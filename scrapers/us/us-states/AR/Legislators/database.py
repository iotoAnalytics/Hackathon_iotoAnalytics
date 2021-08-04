from psycopg2 import pool
from psycopg2.extras import RealDictCursor
import configparser
import boto3
import sys, os

# Initialize config parser and get variables from config file
configParser = configparser.RawConfigParser()
configParser.read('config.cfg')

#Initialize database and scraper utils
db_host = str(configParser.get('databaseConfig', 'db_host'))
db_port = str(configParser.get('databaseConfig', 'db_port'))
db_user = str(configParser.get('databaseConfig', 'db_user'))
db_region = str(configParser.get('databaseConfig', 'db_region'))
db_name = str(configParser.get('databaseConfig', 'db_name'))

client = boto3.client('rds', db_region)

class Database:
    __connection_pool = None

    @staticmethod
    def initialise():
        db_token = client.generate_db_auth_token(db_host, db_port, db_user, Region=db_region)
        
        Database.__connection_pool = pool.SimpleConnectionPool(1,
                                                               5,
                                                               database=db_name, host=db_host, user=db_user, password=db_token)

    @classmethod
    def get_connection(cls):
        return cls.__connection_pool.getconn()

    @classmethod
    def return_connection(cls, connection):
        Database.__connection_pool.putconn(connection)

    @classmethod
    def close_all_connections(cls):
        Database.__connection_pool.closeall()


class CursorFromConnectionFromPool:
    def __init__(self):
        self.connect = None
        self.cursor = None

    def __enter__(self):
        self.connection = Database.get_connection()
        self.cursor = self.connection.cursor(cursor_factory=RealDictCursor)
        return self.cursor

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_val is not None:
            self.connection.rollback()
        else:
            self.cursor.close()
            self.connection.commit()
        Database.return_connection(self.connection)
