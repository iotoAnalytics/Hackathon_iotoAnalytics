# from psycopg2 import pool
from psycopg2.extras import RealDictCursor
import psycopg2
import configparser
import boto3
import sys, os


db_host = 'openparl.cia2zobysfwo.us-west-2.rds.amazonaws.com'
db_port = 5432
db_user = 'rds'
db_region = 'us-west-2'
db_name = 'openparl'

client = boto3.client('rds', db_region)

class Database:
    
    def __init__(self):
        db_token = client.generate_db_auth_token(db_host, db_port, db_user, Region=db_region)
        
        self.conn = psycopg2.connect(database=db_name, host=db_host, user=db_user, password=db_token)
        self.cur = self.conn.cursor(cursor_factory=RealDictCursor)

    def close_all_connections(self):
        if self.conn:
            self.conn.commit()
            if self.cur:
                self.cur.close()
            self.conn.close()
