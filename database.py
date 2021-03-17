"""
Used for connecting scrapers to relational database using psycopg2.
Author: Justin Tendeck
"""

from psycopg2.extras import RealDictCursor
from psycopg2 import pool
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
    """
    Used for establishing connection to database an managing connections.
    
    Author: Jose Salvatierra
    Source: https://www.udemy.com/course/the-complete-python-postgresql-developer-course
    """
    __connection_pool = None

    @staticmethod
    def initialise():
        """
        Generates database connection token then connects to database via
        connection pooling. Must be run before attempting to connect to
        the database.
        """
        db_token = client.generate_db_auth_token(db_host, db_port, db_user, Region=db_region)
        
        Database.__connection_pool = pool.SimpleConnectionPool(1,
                                                               10,
                                                               database=db_name, host=db_host, user=db_user, password=db_token)

    @classmethod
    def get_connection(cls):
        """
        Returns a connection from the database connection pool.
        
        Returns:
            connection: a psycopg2 connection to the database.
        """
        return cls.__connection_pool.getconn()

    @classmethod
    def return_connection(cls, connection):
        """
        Returns a database connection back to the database connection pool.

        Args:
            connection: Psycopg2 database connection to be returned.
        """
        Database.__connection_pool.putconn(connection)

    @classmethod
    def close_all_connections(cls):
        """
        Closes all connections in the connection pool.
        """
        Database.__connection_pool.closeall()


class CursorFromConnectionFromPool:
    """
    Used for obtaining a database cursor from the Database connection
    pool. Before using you must import the Database class and
    run Database.initialise()
    
    Author: Jose Salvatierra
    Source: https://www.udemy.com/course/the-complete-python-postgresql-developer-course
    """

    def __init__(self):
        """
        Initialize object with empty connection and cursor.
        """
        self.connect = None
        self.cursor = None

    def __enter__(self):
        """
        Calls the get_connection() method from the Database class and stores
        a connection and cursor.

        Returns:
            cursor: Pscyopg2 database connection cursor
        """
        self.connection = Database.get_connection()
        self.cursor = self.connection.cursor(cursor_factory=RealDictCursor)
        return self.cursor

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Commits database session then closes database connection, cursors. If an
        exception occurs, the database session will rollback instead.

        Args:
            exc_type: Exception type
            exc_val: Exception value
            exc_tb: Exception traceback
        """
        if exc_val is not None:
            self.connection.rollback()
        else:
            self.cursor.close()
            self.connection.commit()
        Database.return_connection(self.connection)
