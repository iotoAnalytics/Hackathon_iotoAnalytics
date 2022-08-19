"""
Used for connecting scrapers to relational database using psycopg2.
Author: Justin Tendeck
"""

from typing import AnyStr
from psycopg2.extras import RealDictCursor
# from psycopg2 import pool
import psycopg2
import boto3
from psycopg2 import sql
from datetime import datetime
import json
import utils
import pandas as pd


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
    _connection = None

    @staticmethod
    def initialise():
        """
        Generates database connection token then connects to database via
        connection pooling. Must be run before attempting to connect to
        the database.
        """
        db_token = client.generate_db_auth_token(db_host, db_port, db_user, Region=db_region)

        Database._connection = psycopg2.connect(database=db_name, host=db_host, user=db_user, password=db_token)

    @classmethod
    def get_connection(cls):
        """
        Returns a connection from the database connection pool.

        Returns:
            connection: a psycopg2 connection to the database.
        """
        db_token = client.generate_db_auth_token(db_host, db_port, db_user, Region=db_region)

        Database._connection = psycopg2.connect(database=db_name, host=db_host, user=db_user, password=db_token)
        return cls._connection

    # @classmethod
    # def return_connection(cls, connection):
    #     """
    #     Returns a database connection back to the database connection pool.

    #     Args:
    #         connection: Psycopg2 database connection to be returned.
    #     """
    #     Database._connection.putconn(connection)

    @classmethod
    def close_connection(cls):
        """
        Closes all connections in the connection pool.
        """
        Database._connection.close()

    
    # def __del__(self):
    #     if Database._connection:
    #         Database.close_connection()


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
        Database.close_connection()


class Persistence:
    """Class for writing collected data to database"""
    # table that indexes columns in canadian datasets that have null values

    # writes pm_video data to database
    # @staticmethod
    # def write_pm_vid_data(data, table):
    #     with CursorFromConnectionFromPool() as cur:
    #         try:
    #             create_table_query = sql.SQL("""

    #                 CREATE TABLE IF NOT EXISTS {table} (
    #                     title text UNIQUE,
    #                     video_text text
    #                 );

    #                 ALTER TABLE {table} OWNER TO rds_ad;
    #             """).format(table=sql.Identifier(table))

    #             cur.execute(create_table_query)
    #             cur.connection.commit()
    #         except Exception as e:
    #             print(f'An exception occured executting a query:\n{e}')
    #             cur.connection.rollback()

    #         insert_legislator_query = sql.SQL("""
    #                 INSERT INTO {table}
    #                 VALUES (%s, %s)
    #                 ON CONFLICT (title) DO UPDATE SET
    #                     title = excluded.title,
    #                     video_text = excluded.video_text;
    #                 """).format(table=sql.Identifier(table))

    #         # This is used to convert dictionaries to rows. Need to test it out!
    #         for item in data:
    #             if isinstance(item, dict):
    #                 item = utils.DotDict(item)
    #             try:
    #                 tup = (
    #                     item.title,
    #                     item.video_text
    #                 )

    #                 cur.execute(insert_legislator_query, tup)
    #             except Exception as e:
    #                 print(f'Exception occured inserting the following data:\n{tup}')
    #                 print(e)
    #                 cur.connection.rollback()

    # @staticmethod
    # def write_stats_data_test(data, table):
    #     with CursorFromConnectionFromPool() as cur:
    #         try:
    #             create_table_query = sql.SQL("""

    #                 CREATE TABLE IF NOT EXISTS {table} (
    #                     state_name text UNIQUE,
    #                     legislator_count int, 
    #                     ave_bills_sponsored decimal(5,2),
    #                     ave_bills_sponsored_percent decimal(5,2),
    #                     ave_age decimal(5,2),
    #                     ave_years_active decimal(5,2),
    #                     topics_count json
    #                 );

    #                 ALTER TABLE {table} OWNER TO rds_ad;
    #             """).format(table=sql.Identifier(table))

    #             cur.execute(create_table_query)
    #             cur.connection.commit()
    #         except Exception as e:
    #             print(f'An exception occured executting a query:\n{e}')
    #             cur.connection.rollback()

    #         insert_legislator_query = sql.SQL("""
    #                 INSERT INTO {table}
    #                 VALUES (%s, %s, %s, %s, %s, %s, %s)
    #                 ON CONFLICT (state_name) DO UPDATE SET
    #                     legislator_count = excluded.legislator_count,
    #                     ave_bills_sponsored = excluded.ave_bills_sponsored,
    #                     ave_bills_sponsored_percent = excluded.ave_bills_sponsored_percent,
    #                     ave_age = excluded.ave_age,
    #                     ave_years_active = excluded.ave_years_active,
    #                     topics_count = excluded.topics_count;
    #                 """).format(table=sql.Identifier(table))

    #         # This is used to convert dictionaries to rows. Need to test it out!
    #         for item in data:
    #             if isinstance(item, dict):
    #                 item = utils.DotDict(item)
    #             try:
    #                 tup = (
    #                     item.state_name,
    #                     item.legislator_count,
    #                     item.ave_bills_sponsored, 
    #                     item.ave_bills_sponsored_percent,
    #                     item.ave_age,
    #                     item.ave_years_active,
    #                     # item.topics_count
    #                     json.dumps(item.topics_count, default=utils.json_serial)
    #                 )

    #                 cur.execute(insert_legislator_query, tup)
    #             except Exception as e:
    #                 print(f'Exception occured inserting the following data:\n{tup}')
    #                 print(e)
    #                 cur.connection.rollback()


    @staticmethod
    def write_aq_meeting_data(data, table):
        if not isinstance(data, list):
            raise TypeError(
                'Data being written to database must be a list of Rows or dictionaries!')
        
        with CursorFromConnectionFromPool() as cur:
            try:
                create_table_query = sql.SQL("""
                    CREATE TABLE IF NOT EXISTS {table} (
                        meeting_date text,
                        num_matches int,
                        meeting_minutes text,
                        keyword text
                    );

                    ALTER TABLE {table} OWNER TO rds_ad;
                    """).format(table=sql.Identifier(table))

                cur.execute(create_table_query)
                cur.connection.commit()
            
            except Exception as e:
                print(f'An exception occurred executing a query:\n{e}')
                cur.connection.rollback()
            
            insert_query = sql.SQL("""
                    INSERT INTO {table}
                    VALUES (
                        %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING;
                    """).format(table=sql.Identifier(table))
            for item in data:
                if isinstance(item, dict):
                    item = utils.DotDict(item)
                else:
                    print("error")
                tup = (
                    item.meeting_date,
                    item.num_matches,
                    item.meeting_minutes,
                    item.keyword
                )

                cur.execute(insert_query, tup)
 

