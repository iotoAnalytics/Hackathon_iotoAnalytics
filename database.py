"""
Used for connecting scrapers to relational database using psycopg2.
Author: Justin Tendeck
"""

from typing import AnyStr
from psycopg2.extras import RealDictCursor
# from psycopg2 import pool
import psycopg2
import configparser
import boto3
import sys
import os
from psycopg2 import sql
from datetime import date, datetime
import json
import utils
import sys
import numpy as np
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
    def write_ca_fed_vote_data(data, table):
        if not isinstance(data, list):
            raise TypeError(
                'Data being written to database must be a list of Rows or dictionaries!')

        with CursorFromConnectionFromPool() as cur:
            try:
                create_table_query = sql.SQL("""
                    
                    CREATE TABLE IF NOT EXISTS {table} (
                        goverlytics_id text PRIMARY KEY,
                        Name text,
                        Session text,
                        Vote Number text,
                        Vote Respect text,
                        Vote Number text
                    );

                    ALTER TABLE {table} OWNER TO rds_ad;
                    """).format(table=sql.Identifier(table))

                cur.execute(create_table_query)
                cur.connection.commit()

            except Exception as e:
                print(
                    f'An exception occurred creating {table}:\n{e}')
                cur.connection.rollback()

            insert_legislator_query = sql.SQL("""
                INSERT INTO {table}
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (source_url) DO UPDATE SET
                    date_collected = excluded.date_collected,
                    bill_title = excluded.bill_title,
                    bill_name = excluded.bill_name,
                    bill_type = excluded.bill_type,
                    sponsors = excluded.sponsors,
                    sponsors_id = excluded.sponsors_id,
                    principal_sponsor_id = excluded.principal_sponsor_id,
                    principal_sponsor = excluded.principal_sponsor,
                    current_status = excluded.current_status,
                    actions = excluded.actions,
                    date_introduced = excluded.date_introduced,
                    chamber_origin = excluded.chamber_origin,
                    session = excluded.session,
                    state = excluded.state,
                    state_id = excluded.state_id,
                    source_topic = excluded.source_topic,
                    votes = excluded.votes,
                    goverlytics_id = excluded.goverlytics_id,
                    source_id = excluded.source_id,
                    committees = excluded.committees,
                    cosponsors = excluded.cosponsors,
                    cosponsors_id = excluded.cosponsors_id,
                    topic = excluded.topic,
                    bill_text = excluded.bill_text,
                    bill_description = excluded.bill_description,
                    bill_summary = excluded.bill_summary,
                    country_id = excluded.country_id,
                    country = excluded.country;
                """).format(table=sql.Identifier(table))

            date_collected = datetime.now()

            for row in data:

                if isinstance(row, dict):
                    row = utils.DotDict(row)

                if pd.notna(row.principal_sponsor_id):
                    row.principal_sponsor_id = int(row.principal_sponsor_id)
                else:
                    row.principal_sponsor_id = None

                tup = (row.goverlytics_id, row.source_id, date_collected, row.bill_name,
                       row.session, row.date_introduced, row.source_url, row.chamber_origin,
                       json.dumps(row.committees, default=utils.json_serial),
                       row.state_id, row.state, row.bill_type, row.bill_title, row.current_status,
                       row.principal_sponsor_id, row.principal_sponsor, row.sponsors, row.sponsors_id,
                       row.cosponsors, row.cosponsors_id, row.bill_text, row.bill_description, row.bill_summary,
                       json.dumps(row.actions, default=utils.json_serial),
                       json.dumps(row.votes, default=utils.json_serial),
                       row.source_topic, row.topic, row.country_id, row.country)

                try:
                    cur.execute(insert_legislator_query, tup)

                except Exception as e:
                    print(
                        f'An exception occurred inserting {row.goverlytics_id}:\n{e}')
                    cur.connection.rollback()
    



    @staticmethod
    def write_us_fed_legislation(data, table):
        if not isinstance(data, list):
            raise TypeError(
                'Data being written to database must be a list of Rows or dictionaries!')

        with CursorFromConnectionFromPool() as cur:
            try:
                create_table_query = sql.SQL("""
                    
                    CREATE TABLE IF NOT EXISTS {table} (
                        goverlytics_id text PRIMARY KEY,
                        source_id text,
                        date_collected timestamp,
                        bill_name text,
                        session TEXT,
                        date_introduced date,
                        source_url text UNIQUE,
                        chamber_origin text,
                        committees jsonb,
                        state_id int,
                        state char(2),
                        bill_type text,
                        bill_title text,
                        current_status text,
                        principal_sponsor_id int,
                        principal_sponsor text,
                        sponsors text[],
                        sponsors_id int[],
                        cosponsors text[],
                        cosponsors_id int[],
                        bill_text text,
                        bill_description text,
                        bill_summary text,
                        actions jsonb,
                        votes jsonb,
                        source_topic text,
                        topic text,
                        country_id int,
                        country text
                    );

                    ALTER TABLE {table} OWNER TO rds_ad;
                    """).format(table=sql.Identifier(table))

                cur.execute(create_table_query)
                cur.connection.commit()

            except Exception as e:
                print(
                    f'An exception occurred creating {table}:\n{e}')
                cur.connection.rollback()

            insert_legislator_query = sql.SQL("""
                INSERT INTO {table}
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (source_url) DO UPDATE SET
                    date_collected = excluded.date_collected,
                    bill_title = excluded.bill_title,
                    bill_name = excluded.bill_name,
                    bill_type = excluded.bill_type,
                    sponsors = excluded.sponsors,
                    sponsors_id = excluded.sponsors_id,
                    principal_sponsor_id = excluded.principal_sponsor_id,
                    principal_sponsor = excluded.principal_sponsor,
                    current_status = excluded.current_status,
                    actions = excluded.actions,
                    date_introduced = excluded.date_introduced,
                    chamber_origin = excluded.chamber_origin,
                    session = excluded.session,
                    state = excluded.state,
                    state_id = excluded.state_id,
                    source_topic = excluded.source_topic,
                    votes = excluded.votes,
                    goverlytics_id = excluded.goverlytics_id,
                    source_id = excluded.source_id,
                    committees = excluded.committees,
                    cosponsors = excluded.cosponsors,
                    cosponsors_id = excluded.cosponsors_id,
                    topic = excluded.topic,
                    bill_text = excluded.bill_text,
                    bill_description = excluded.bill_description,
                    bill_summary = excluded.bill_summary,
                    country_id = excluded.country_id,
                    country = excluded.country;
                """).format(table=sql.Identifier(table))

            date_collected = datetime.now()

            for row in data:

                if isinstance(row, dict):
                    row = utils.DotDict(row)

                if pd.notna(row.principal_sponsor_id):
                    row.principal_sponsor_id = int(row.principal_sponsor_id)
                else:
                    row.principal_sponsor_id = None

                tup = (row.goverlytics_id, row.source_id, date_collected, row.bill_name,
                       row.session, row.date_introduced, row.source_url, row.chamber_origin,
                       json.dumps(row.committees, default=utils.json_serial),
                       row.state_id, row.state, row.bill_type, row.bill_title, row.current_status,
                       row.principal_sponsor_id, row.principal_sponsor, row.sponsors, row.sponsors_id,
                       row.cosponsors, row.cosponsors_id, row.bill_text, row.bill_description, row.bill_summary,
                       json.dumps(row.actions, default=utils.json_serial),
                       json.dumps(row.votes, default=utils.json_serial),
                       row.source_topic, row.topic, row.country_id, row.country)

                try:
                    cur.execute(insert_legislator_query, tup)

                except Exception as e:
                    print(
                        f'An exception occurred inserting {row.goverlytics_id}:\n{e}')
                    cur.connection.rollback()

    @staticmethod
    def write_us_fed_legislators(data, table):
        if not isinstance(data, list):
            raise TypeError(
                'Data being written to database must be a list of Rows or dictionaries!')

        with CursorFromConnectionFromPool() as cur:
            try:
                create_table_query = sql.SQL("""
            
                        CREATE TABLE IF NOT EXISTS {table} (
                            goverlytics_id bigint PRIMARY KEY,
                            source_id text,
                            most_recent_term_id text,
                            date_collected timestamp,
                            source_url TEXT UNIQUE,
                            name_full text,
                            name_last text,
                            name_first text,
                            name_middle text,
                            name_suffix text,
                            country_id bigint,
                            country text,
                            state_id int,
                            state char(2),
                            party_id int,
                            party text,
                            role text,
                            district text,
                            years_active int[],
                            committees jsonb,
                            areas_served text[],
                            phone_numbers jsonb,
                            addresses jsonb,
                            email text,
                            birthday date,
                            seniority int,
                            occupation text[],
                            education jsonb,
                            military_experience text,
                            gender text,
                            wiki_url text UNIQUE
                        );

                        ALTER TABLE {table} OWNER TO rds_ad;
                        """).format(table=sql.Identifier(table))

                cur.execute(create_table_query)
                cur.connection.commit()

            except Exception as e:
                print(f'An exception occurred executing a query:\n{e}')
                cur.connection.rollback()

            insert_legislator_query = sql.SQL("""
            
                    WITH leg_id AS (SELECT NEXTVAL('legislator_id') leg_id)
                    INSERT INTO {table}
                    VALUES (
                        (SELECT leg_id FROM leg_id), %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (source_url) DO UPDATE SET
                        date_collected = excluded.date_collected,
                        name_full = excluded.name_full,
                        name_last = excluded.name_last,
                        name_first = excluded.name_first,
                        name_middle = excluded.name_middle,
                        name_suffix = excluded.name_suffix,
                        district = excluded.district,
                        role = excluded.role,
                        committees = excluded.committees,
                        areas_served = excluded.areas_served,
                        phone_numbers = excluded.phone_numbers,
                        addresses = excluded.addresses,
                        state = excluded.state,
                        state_id = excluded.state_id,
                        party = excluded.party,
                        party_id = excluded.party_id,
                        email = excluded.email,
                        birthday = excluded.birthday,
                        military_experience = excluded.military_experience,
                        occupation = excluded.occupation,
                        education = excluded.education,
                        source_id = excluded.source_id,
                        most_recent_term_id = excluded.most_recent_term_id,
                        years_active = excluded.years_active,
                        seniority = excluded.seniority
                        gender = excluded.gender,
                        wiki_url = excluded.wiki_url;
                    """).format(table=sql.Identifier(table))

            date_collected = datetime.now()

            # This is used to convert dictionaries to rows. Need to test it out!
            for item in data:
                if isinstance(item, dict):
                    item = utils.DotDict(item)

                tup = (
                    item.source_id,
                    item.most_recent_term_id,
                    date_collected,
                    item.source_url,
                    item.name_full,
                    item.name_last,
                    item.name_first,
                    item.name_middle,
                    item.name_suffix,
                    item.country_id,
                    item.country,
                    item.state_id,
                    item.state,
                    item.party_id,
                    item.party,
                    item.role,
                    item.district,
                    item.years_active,
                    json.dumps(item.committees, default=utils.json_serial),
                    item.areas_served,
                    json.dumps(item.phone_numbers, default=utils.json_serial),
                    json.dumps(item.addresses, default=utils.json_serial),
                    item.email,
                    item.birthday,
                    item.seniority,
                    item.occupation,
                    json.dumps(item.education, default=utils.json_serial),
                    item.military_experience,
                    item.gender,
                    item.wiki_url
                )

                cur.execute(insert_legislator_query, tup)

    @staticmethod
    def write_ca_fed_legislators(data, table):
        if not isinstance(data, list):
            raise TypeError(
                'Data being written to database must be a list of Rows or dictionaries!')

        with CursorFromConnectionFromPool() as cur:
            try:
                create_table_query = sql.SQL("""
                        
                        CREATE TABLE IF NOT EXISTS {table} (
                            goverlytics_id bigint PRIMARY KEY,
                            source_id text,
                            most_recent_term_id text,
                            date_collected timestamp,
                            source_url TEXT UNIQUE,
                            name_full text,
                            name_last text,
                            name_first text,
                            name_middle text,
                            name_suffix text,
                            country_id bigint,
                            country text,
                            province_territory_id int,
                            province_territory char(2),
                            party_id int,
                            party text,
                            role text,
                            riding text,
                            years_active int[],
                            committees jsonb,
                            phone_numbers jsonb,
                            addresses jsonb,
                            email text,
                            birthday date,
                            seniority int,
                            occupation text[],
                            education jsonb,
                            military_experience text,
                            region text,
                            offices_roles_as_mp text[],
                            parl_assoc_interparl_groups jsonb,
                            gender text,
                            wiki_url text UNIQUE
                        );

                        ALTER TABLE {table} OWNER TO rds_ad;
                        """).format(table=sql.Identifier(table))

                cur.execute(create_table_query)
                cur.connection.commit()
            except Exception as e:
                print(f'An exception occurred executing a query:\n{e}')
                cur.connection.rollback()

            insert_legislator_query = sql.SQL("""
                    WITH leg_id AS (SELECT NEXTVAL('legislator_id') leg_id)
                    INSERT INTO {table}
                    VALUES (
                        (SELECT leg_id FROM leg_id), %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (source_url) DO UPDATE SET
                        date_collected = excluded.date_collected,
                        name_full = excluded.name_full,
                        name_last = excluded.name_last,
                        name_first = excluded.name_first,
                        name_middle = excluded.name_middle,
                        name_suffix = excluded.name_suffix,
                        riding = excluded.riding,
                        province_territory = excluded.province_territory,
                        province_territory_id = excluded.province_territory_id,
                        party = excluded.party,
                        party_id = excluded.party_id,
                        role = excluded.role,
                        committees = excluded.committees,
                        phone_numbers = excluded.phone_numbers,
                        addresses = excluded.addresses,
                        email = excluded.email,
                        birthday = excluded.birthday,
                        military_experience = excluded.military_experience,
                        occupation = excluded.occupation,
                        education = excluded.education,
                        source_id = excluded.source_id,
                        most_recent_term_id = excluded.most_recent_term_id,
                        years_active = excluded.years_active,
                        offices_roles_as_mp = excluded.offices_roles_as_mp,
                        parl_assoc_interparl_groups = excluded.parl_assoc_interparl_groups,
                        region = excluded.region,
                        gender = excluded.gender,
                        wiki_url = excluded.wiki_url,
                        seniority = excluded.seniority;
                    """).format(table=sql.Identifier(table))

            date_collected = datetime.now()

            # This is used to convert dictionaries to rows. Need to test it out!
            for item in data:
                if isinstance(item, dict):
                    item = utils.DotDict(item)

                tup = (
                    item.source_id,
                    item.most_recent_term_id,
                    date_collected,
                    item.source_url,
                    item.name_full,
                    item.name_last,
                    item.name_first,
                    item.name_middle,
                    item.name_suffix,
                    item.country_id,
                    item.country,
                    item.province_territory_id,
                    item.province_territory,
                    item.party_id,
                    item.party,
                    item.role,
                    item.riding,
                    item.years_active,
                    json.dumps(item.committees, default=utils.json_serial),
                    json.dumps(item.phone_numbers, default=utils.json_serial),
                    json.dumps(item.addresses, default=utils.json_serial),
                    item.email,
                    item.birthday,
                    item.seniority,
                    item.occupation,
                    json.dumps(item.education, default=utils.json_serial),
                    item.military_experience,
                    item.region,
                    item.offices_roles_as_mp,
                    json.dumps(item.parl_assoc_interparl_groups,
                               default=utils.json_serial),
                    item.gender,
                    item.wiki_url
                )

                cur.execute(insert_legislator_query, tup)

    @staticmethod
    def write_ca_prov_terr_legislators(data, table):
        if not isinstance(data, list):
            raise TypeError(
                'Data being written to database must be a list of Rows or dictionaries!')

        with CursorFromConnectionFromPool() as cur:
            try:
                create_table_query = sql.SQL("""
                        
                        CREATE TABLE IF NOT EXISTS {table} (
                            goverlytics_id bigint PRIMARY KEY,
                            source_id text,
                            most_recent_term_id text,
                            date_collected timestamp,
                            source_url TEXT UNIQUE,
                            name_full text,
                            name_last text,
                            name_first text,
                            name_middle text,
                            name_suffix text,
                            country_id bigint,
                            country text,
                            province_territory_id int,
                            province_territory char(2),
                            party_id int,
                            party text,
                            role text,
                            riding text,
                            years_active int[],
                            committees jsonb,
                            phone_numbers jsonb,
                            addresses jsonb,
                            email text,
                            birthday date,
                            seniority int,
                            occupation text[],
                            education jsonb,
                            military_experience text,
                            region text,
                            gender text,
                            wiki_url text UNIQUE
                        );

                        ALTER TABLE {table} OWNER TO rds_ad;
                        """).format(table=sql.Identifier(table))

                cur.execute(create_table_query)
                cur.connection.commit()
            except Exception as e:
                print(f'An exception occurred executing a query:\n{e}')
                cur.connection.rollback()

            insert_legislator_query = sql.SQL("""
                    
                    WITH leg_id AS (SELECT NEXTVAL('legislator_id') leg_id)
                    INSERT INTO {table}
                    VALUES (
                        (SELECT leg_id FROM leg_id), %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (source_url) DO UPDATE SET
                        date_collected = excluded.date_collected,
                        name_full = excluded.name_full,
                        name_last = excluded.name_last,
                        name_first = excluded.name_first,
                        name_middle = excluded.name_middle,
                        name_suffix = excluded.name_suffix,
                        riding = excluded.riding,
                        province_territory = excluded.province_territory,
                        province_territory_id = excluded.province_territory_id,
                        party = excluded.party,
                        party_id = excluded.party_id,
                        role = excluded.role,
                        committees = excluded.committees,
                        phone_numbers = excluded.phone_numbers,
                        addresses = excluded.addresses,
                        email = excluded.email,
                        birthday = excluded.birthday,
                        military_experience = excluded.military_experience,
                        occupation = excluded.occupation,
                        education = excluded.education,
                        source_id = excluded.source_id,
                        most_recent_term_id = excluded.most_recent_term_id,
                        years_active = excluded.years_active,
                        seniority = excluded.seniority,
                        gender = excluded.gender,
                        wiki_url = excluded.wiki_url;
                    """).format(table=sql.Identifier(table))

            date_collected = datetime.now()

            # This is used to convert dictionaries to rows. Need to test it out!
            for item in data:
                if isinstance(item, dict):
                    item = utils.DotDict(item)

                try:
                    tup = (
                        item.source_id,
                        item.most_recent_term_id,
                        date_collected,
                        item.source_url,
                        item.name_full,
                        item.name_last,
                        item.name_first,
                        item.name_middle,
                        item.name_suffix,
                        item.country_id,
                        item.country,
                        item.province_territory_id,
                        item.province_territory,
                        item.party_id,
                        item.party,
                        item.role,
                        item.riding,
                        item.years_active,
                        json.dumps(item.committees, default=utils.json_serial),
                        json.dumps(item.phone_numbers, default=utils.json_serial),
                        json.dumps(item.addresses, default=utils.json_serial),
                        item.email,
                        item.birthday,
                        item.seniority,
                        item.occupation,
                        json.dumps(item.education, default=utils.json_serial),
                        item.military_experience,
                        item.region,
                        item.gender,
                        item.wiki_url)

                    cur.execute(insert_legislator_query, tup)
                except Exception as e:
                    print(f'Exception occurred inserting the following data:\n{tup} \n{e}')
                    cur.connection.rollback()


    @staticmethod
    def write_ca_fed_legislation(data, table):
        if not isinstance(data, list):
            raise TypeError(
                'Data being written to database must be a list of Rows or dictionaries!')

        with CursorFromConnectionFromPool() as cur:
            try:
                create_table_query = sql.SQL("""
                    CREATE TABLE IF NOT EXISTS {table} (
                        goverlytics_id text PRIMARY KEY,
                        source_id text,
                        date_collected timestamp,
                        bill_name text,
                        session TEXT,
                        date_introduced date,
                        source_url text,
                        chamber_origin text,
                        committees jsonb,
                        province_territory_id int,
                        province_territory char(2),
                        region text,
                        bill_type text,
                        bill_title text,
                        current_status text,
                        principal_sponsor_id int,
                        principal_sponsor text,
                        sponsors text[],
                        sponsors_id int[],
                        cosponsors text[],
                        cosponsors_id int[],
                        bill_text text,
                        bill_description text,
                        bill_summary text,
                        actions jsonb,
                        votes jsonb,
                        source_topic text,
                        topic text,
                        country_id int,
                        country text,
                        sponsor_affiliation text,
                        sponsor_gender char(1),
                        pm_name_full text,
                        pm_party text,
                        pm_party_id int,
                        statute_year int,
                        statute_chapter int,
                        publications text[],
                        last_major_event jsonb
                    );

                    ALTER TABLE {table} OWNER TO rds_ad;
                    """).format(table=sql.Identifier(table))

                cur.execute(create_table_query)
                cur.connection.commit()

            except Exception as e:
                print(
                    f'An exception occurred creating {table}:\n{e}')
                cur.connection.rollback()

            insert_legislator_query = sql.SQL("""
                INSERT INTO {table}
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (goverlytics_id) DO UPDATE SET
                    date_collected = excluded.date_collected,
                    bill_title = excluded.bill_title,
                    bill_name = excluded.bill_name,
                    bill_type = excluded.bill_type,
                    sponsors = excluded.sponsors,
                    sponsors_id = excluded.sponsors_id,
                    principal_sponsor_id = excluded.principal_sponsor_id,
                    principal_sponsor = excluded.principal_sponsor,
                    current_status = excluded.current_status,
                    actions = excluded.actions,
                    date_introduced = excluded.date_introduced,
                    chamber_origin = excluded.chamber_origin,
                    session = excluded.session,
                    province_territory = excluded.province_territory,
                    province_territory_id = excluded.province_territory_id,
                    region = excluded.region,
                    source_topic = excluded.source_topic,
                    votes = excluded.votes,
                    goverlytics_id = excluded.goverlytics_id,
                    source_id = excluded.source_id,
                    committees = excluded.committees,
                    cosponsors = excluded.cosponsors,
                    cosponsors_id = excluded.cosponsors_id,
                    topic = excluded.topic,
                    bill_text = excluded.bill_text,
                    bill_description = excluded.bill_description,
                    pm_party = excluded.pm_party,
                    pm_party_id = excluded.pm_party_id,
                    bill_summary = excluded.bill_summary,
                    country_id = excluded.country_id,
                    country = excluded.country,
                    statute_year = excluded.statute_year,
                    statute_chapter = excluded.statute_chapter,
                    publications = excluded.publications,
                    last_major_event = excluded.last_major_event;
                """).format(table=sql.Identifier(table))

            date_collected = datetime.now()

            for row in data:

                if isinstance(row, dict):
                    row = utils.DotDict(row)

                if pd.notna(row.principal_sponsor_id):
                    row.principal_sponsor_id = int(row.principal_sponsor_id)
                else:
                    row.principal_sponsor_id = None

                if pd.notna(row.province_territory_id):
                    row.province_territory_id = int(row.province_territory_id)
                else:
                    row.province_territory_id = None 

                tup = (row.goverlytics_id, row.source_id, date_collected, row.bill_name,
                       row.session, row.date_introduced, row.source_url, row.chamber_origin,
                       json.dumps(row.committees, default=utils.json_serial),
                       row.province_territory_id, row.province_territory, row.region, row.bill_type, row.bill_title,
                       row.current_status,
                       row.principal_sponsor_id, row.principal_sponsor, row.sponsors, row.sponsors_id,
                       row.cosponsors, row.cosponsors_id, row.bill_text, row.bill_description, row.bill_summary,
                       json.dumps(row.actions, default=utils.json_serial),
                       json.dumps(row.votes, default=utils.json_serial),
                       row.source_topic, row.topic, row.country_id, row.country,
                       row.sponsor_affiliation, row.sponsor_gender, row.pm_name_full,
                       row.pm_party, row.pm_party_id, row.statute_year, row.statute_chapter,
                       row.publications,
                       json.dumps(row.last_major_event, default=utils.json_serial))

                try:
                    cur.execute(insert_legislator_query, tup)

                except Exception as e:
                    print(
                        f'An exception occurred inserting {row.goverlytics_id}:\n{e}')
                    cur.connection.rollback()

    @staticmethod
    def write_ca_prov_terr_legislation(data, table):
        if not isinstance(data, list):
            raise TypeError(
                'Data being written to database must be a list of Rows or dictionaries!')

        with CursorFromConnectionFromPool() as cur:
            try:
                create_table_query = sql.SQL("""
                  
                    CREATE TABLE IF NOT EXISTS {table} (
                        goverlytics_id text PRIMARY KEY,
                        source_id text,
                        date_collected timestamp,
                        bill_name text,
                        session TEXT,
                        date_introduced date,
                        source_url text UNIQUE,
                        chamber_origin text,
                        committees jsonb,
                        province_territory_id int,
                        province_territory char(2),
                        bill_type text,
                        bill_title text,
                        current_status text,
                        principal_sponsor_id int,
                        principal_sponsor text,
                        sponsors text[],
                        sponsors_id int[],
                        cosponsors text[],
                        cosponsors_id int[],
                        bill_text text,
                        bill_description text,
                        bill_summary text,
                        actions jsonb,
                        votes jsonb,
                        source_topic text,
                        topic text,
                        country_id int,
                        country text
                    );

                    ALTER TABLE {table} OWNER TO rds_ad;
                    """).format(table=sql.Identifier(table))

                cur.execute(create_table_query)
                cur.connection.commit()

            except Exception as e:
                print(
                    f'An exception occurred creating {table}:\n{e}')
                cur.connection.rollback()

            insert_legislator_query = sql.SQL("""
                INSERT INTO {table}
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (source_url) DO UPDATE SET
                    date_collected = excluded.date_collected,
                    bill_title = excluded.bill_title,
                    bill_name = excluded.bill_name,
                    bill_type = excluded.bill_type,
                    sponsors = excluded.sponsors,
                    sponsors_id = excluded.sponsors_id,
                    principal_sponsor_id = excluded.principal_sponsor_id,
                    principal_sponsor = excluded.principal_sponsor,
                    current_status = excluded.current_status,
                    actions = excluded.actions,
                    date_introduced = excluded.date_introduced,
                    chamber_origin = excluded.chamber_origin,
                    session = excluded.session,
                    province_territory = excluded.province_territory,
                    province_territory_id = excluded.province_territory_id,
                    source_topic = excluded.source_topic,
                    votes = excluded.votes,
                    goverlytics_id = excluded.goverlytics_id,
                    source_id = excluded.source_id,
                    committees = excluded.committees,
                    cosponsors = excluded.cosponsors,
                    cosponsors_id = excluded.cosponsors_id,
                    topic = excluded.topic,
                    bill_text = excluded.bill_text,
                    bill_description = excluded.bill_description,
                    bill_summary = excluded.bill_summary,
                    country_id = excluded.country_id,
                    country = excluded.country;
                """).format(table=sql.Identifier(table))

            date_collected = datetime.now()

            for row in data:
                if isinstance(row, dict):
                    row = utils.DotDict(row)

                if pd.notna(row.principal_sponsor_id):
                    row.principal_sponsor_id = int(row.principal_sponsor_id)
                else:
                    row.principal_sponsor_id = None

                tup = (row.goverlytics_id, row.source_id, date_collected, row.bill_name,
                       row.session, row.date_introduced, row.source_url, row.chamber_origin,
                       json.dumps(row.committees, default=utils.json_serial),
                       row.province_territory_id, row.province_territory, row.bill_type, row.bill_title,
                       row.current_status,
                       row.principal_sponsor_id, row.principal_sponsor, row.sponsors, row.sponsors_id,
                       row.cosponsors, row.cosponsors_id, row.bill_text, row.bill_description, row.bill_summary,
                       json.dumps(row.actions, default=utils.json_serial),
                       json.dumps(row.votes, default=utils.json_serial),
                       row.source_topic, row.topic, row.country_id, row.country)

                try:
                    cur.execute(insert_legislator_query, tup)

                except Exception as e:
                    print(
                        f'An exception occurred inserting {row.goverlytics_id}:\n{e}')
                    cur.connection.rollback()

    @staticmethod
    def write_previous_election_data(data, table):
        if not isinstance(data, list):
            raise TypeError(
                'Data being written to database must be a list of Rows or dictionaries!')

        with CursorFromConnectionFromPool() as cur:
            insert_previous_election_query = sql.SQL("""
                INSERT INTO {table}
                VALUES (
                    DEFAULT, %s, %s, %s, %s, %s)
                ON CONFLICT (election_name) DO UPDATE SET
                    election_date = excluded.election_date,
                    official_votes_record_url = excluded.official_votes_record_url,
                    description = excluded.description,
                    is_by_election = excluded.is_by_election;
                """).format(table=sql.Identifier(table))

            for row in data:

                if isinstance(row, dict):
                    row = utils.DotDict(row)

                tup = (row.election_name, row.election_date, row.official_votes_record_url, row.description, row.is_by_election)

                try:
                    cur.execute(insert_previous_election_query, tup)

                except Exception as e:
                    print(
                        f'An exception occurred inserting {row.election_name}:\n{e}')
                    cur.connection.rollback()

    @staticmethod
    def write_electoral_districts_data(data, table):
        if not isinstance(data, list):
            raise TypeError(
                'Data being written to database must be a list of Rows or dictionaries!')

        with CursorFromConnectionFromPool() as cur:
            insert_previous_election_query = sql.SQL("""
                INSERT INTO {table}
                VALUES (
                    DEFAULT, %s, %s, %s, %s, %s, %s, %s, %s)
                """).format(table=sql.Identifier(table))

            for row in data:

                if isinstance(row, dict):
                    row = utils.DotDict(row)

                tup = (row.province_territory_id, 
                       row.population, 
                       row.census_year, 
                       row.prev_district_names, 
                       row.district_name,
                       row.region,
                       row.is_active,
                       row.start_date)

                try:
                    cur.execute(insert_previous_election_query, tup)

                except Exception as e:
                    print(
                        f'An exception occurred inserting {row.district_name}:\n{e}')
                    cur.connection.rollback()

    @staticmethod
    def write_electors(data, table):
        if not isinstance(data, list):
            raise TypeError(
                'Data being written to database must be a list of Rows or dictionaries!')

        with CursorFromConnectionFromPool() as cur:
            insert_electors_query = sql.SQL("""
                INSERT INTO {table}
                VALUES (
                    DEFAULT, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING;
                """).format(table=sql.Identifier(table))

            for row in data:

                if isinstance(row, dict):
                    row = utils.DotDict(row)

                tup = (row.province_territory_id,
                       row.election_id,
                       row.population,
                       row.electors)

                try:
                    cur.execute(insert_electors_query, tup)

                except Exception as e:
                    print(
                        f'An exception occurred inserting {row.election_id}:\n{e}')
                    cur.connection.rollback()

    @staticmethod
    def write_election_votes(data, table):
        if not isinstance(data, list):
            raise TypeError(
                'Data being written to database must be a list of Rows or dictionaries!')

        with CursorFromConnectionFromPool() as cur:
            insert_electors_query = sql.SQL("""
                    INSERT INTO {table}
                    VALUES (
                        DEFAULT, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING;
                    """).format(table=sql.Identifier(table))

            for row in data:

                if isinstance(row, dict):
                    row = utils.DotDict(row)

                tup = (row.province_territory_id,
                       row.election_id,
                       row.ordinary_stationary,
                       row.ordinary_mobile,
                       row.advanced_polling,
                       row.special_voting_rules,
                       row.invalid_votes,
                       row.voter_turnout,
                       row.total)

                try:
                    cur.execute(insert_electors_query, tup)
                except Exception as e:
                    print(
                        f'An exception occurred inserting {row.election_id}:\n{e}')
                    cur.connection.rollback()

    @staticmethod
    def write_candidate_data(data, table):
        if not isinstance(data, list):
            raise TypeError(
                'Data being written to database must be a list of Rows or dictionaries!')

        with CursorFromConnectionFromPool() as cur:
            for row in data:

                if isinstance(row, dict):
                    row = utils.DotDict(row)

                if row.goverlytics_id == -10:
                    insert_previous_election_query = sql.SQL("""
                        WITH leg_id AS (SELECT NEXTVAL('legislator_id') leg_id)
                        INSERT INTO {table}
                        VALUES (
                            (SELECT leg_id FROM leg_id), %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (goverlytics_id) DO UPDATE SET
                            current_party_id = excluded.current_party_id,
                            current_electoral_district_id = excluded.current_electoral_district_id,
                            name_full = excluded.name_full,
                            name_last = excluded.name_last,
                            name_first = excluded.name_first,
                            name_middle = excluded.name_middle,
                            name_suffix = excluded.name_suffix,
                            gender = excluded.gender,
                            candidate_image = excluded.candidate_image;
                        """).format(table=sql.Identifier(table))

                    tup = (row.current_party_id, 
                        row.current_electoral_district_id, 
                        row.name_full, 
                        row.name_last, 
                        row.name_first,
                        row.name_middle,
                        row.name_suffix,
                        row.gender,
                        row.candidate_image)
                else:
                    insert_previous_election_query = sql.SQL("""
                        INSERT INTO {table}
                        VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (goverlytics_id) DO UPDATE SET
                            current_party_id = excluded.current_party_id,
                            current_electoral_district_id = excluded.current_electoral_district_id,
                            name_full = excluded.name_full,
                            name_last = excluded.name_last,
                            name_first = excluded.name_first,
                            name_middle = excluded.name_middle,
                            name_suffix = excluded.name_suffix,
                            gender = excluded.gender,
                            candidate_image = excluded.candidate_image;
                        """).format(table=sql.Identifier(table))

                    tup = (row.goverlytics_id,
                        row.current_party_id, 
                        row.current_electoral_district_id, 
                        row.name_full, 
                        row.name_last, 
                        row.name_first,
                        row.name_middle,
                        row.name_suffix,
                        row.gender,
                        row.candidate_image)

                try:
                    cur.execute(insert_previous_election_query, tup)

                except Exception as e:
                    print(
                        f'An exception occurred inserting {row.name_full}:\n{e}')
                    cur.connection.rollback()

    @staticmethod
    def write_candidate_election_details_data(data, table):
        if not isinstance(data, list):
            raise TypeError(
                'Data being written to database must be a list of Rows or dictionaries!')

        with CursorFromConnectionFromPool() as cur:
            insert_previous_election_query = sql.SQL("""
                INSERT INTO {table}
                VALUES (
                    DEFAULT, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING;
                """).format(table=sql.Identifier(table))

            for row in data:
                if isinstance(row, dict):
                    row = utils.DotDict(row)

                tup = (row.candidate_id,
                       row.electoral_district_id, 
                       row.party_id, 
                       row.election_id, 
                       row.is_incumbent)

                try:
                    cur.execute(insert_previous_election_query, tup)

                except Exception as e:
                    print(
                        f'An exception occurred inserting {row.candidate_id}:\n{e}')
                    cur.connection.rollback()


    @staticmethod
    def write_financial_contributions(data, table):

        with CursorFromConnectionFromPool() as cur:
            insert_financial_contributions_query = sql.SQL("""
                               INSERT INTO {table}
                               VALUES (
                                   DEFAULT, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                               ON CONFLICT DO NOTHING;
                               """).format(table=sql.Identifier(table))

            for row in data:
                if isinstance(row, dict):
                    row = utils.DotDict(row)

                tup = (row.recipient_id,
                       row.recipient_party_id,
                        row.contributor_prov_terr_id,
                        row.contributor_name,
                        row.contributor_city,
                        row.contributor_postal_code,
                        row.recipient_name,
                        row.date_received,
                        row.fiscal_year_or_event_date,
                        row.part_no_of_return,
                        row.contribution_type,
                        row.monetary_amount,
                        row.non_monetary_amount)

                try:
                    cur.execute(insert_financial_contributions_query, tup)
                except Exception as e:
                    print(
                        f'An exception occurred inserting {row.recipient_id}:\n{e}')
                    cur.connection.rollback()

    @staticmethod
    def write_candidate_election_finances(data, table):

        with CursorFromConnectionFromPool() as cur:
            insert_candidate_election_finances_query = sql.SQL("""
                               INSERT INTO {table}
                               VALUES (
                                   DEFAULT, %s, %s)
                               ON CONFLICT DO NOTHING;
                               """).format(table=sql.Identifier(table))

            for row in data:
                if isinstance(row, dict):
                    row = utils.DotDict(row)

                tup = (row.candidate_election_id,
                       row.date_of_return)

                try:
                    cur.execute(insert_candidate_election_finances_query, tup)
                except Exception as e:
                    print(
                        f'An exception occurred inserting {row.candidate_election_id}:\n{e}')
                    cur.connection.rollback()

    @staticmethod
    def write_candidate_election_finances(data, table):

        with CursorFromConnectionFromPool() as cur:
            insert_candidate_election_finances_query = sql.SQL("""
                               INSERT INTO {table}
                               VALUES (
                                   DEFAULT, %s, %s)
                               ON CONFLICT DO NOTHING;
                               """).format(table=sql.Identifier(table))

            for row in data:
                if isinstance(row, dict):
                    row = utils.DotDict(row)

                tup = (row.candidate_election_id,
                       row.date_of_return)

                try:
                    cur.execute(insert_candidate_election_finances_query, tup)
                except Exception as e:
                    print(
                        f'An exception occurred inserting {row.candidate_election_id}:\n{e}')
                    cur.connection.rollback()

    @staticmethod
    def write_inflows(data, table):

        with CursorFromConnectionFromPool() as cur:
            insert_inflows_query = sql.SQL("""
                               INSERT INTO {table}
                               VALUES (
                                   DEFAULT, %s, %s, %s, %s, %s, %s, %s, %s,
                                   %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                               ON CONFLICT DO NOTHING;
                               """).format(table=sql.Identifier(table))

            for row in data:
                if isinstance(row, dict):
                    row = utils.DotDict(row)

                tup = (row.candidate_election_finances_id,
                       row.monetary,
                       row.non_monetary,
                       row.contribution_detail,
                       row.contribution_totals,
                       row.loans,
                       row.loans_received,
                       row.loans_detail,
                       row.monetary_returned,
                       row.non_monetary_returned,
                       row.returned_detail,
                       row.monetary_transfer_received,
                       row.non_monetary_transfer_received,
                       row.transfer_totals,
                       row.transfer_detail,
                       row.other_cash_inflow,
                       row.other_inflow_detail,
                       row.total_inflow)

                try:
                    cur.execute(insert_inflows_query, tup)
                except Exception as e:
                    print(
                        f'An exception occurred inserting {row.candidate_election_finances_id}:\n{e}')

    @staticmethod
    def write_candidate_election_votes(data, table):

        with CursorFromConnectionFromPool() as cur:
            insert_candidate_election_votes_query = sql.SQL("""
                                   INSERT INTO {table}
                                   VALUES (
                                       DEFAULT, %s, %s, %s, %s, %s)
                                   ON CONFLICT DO NOTHING;
                                   """).format(table=sql.Identifier(table))

            for row in data:
                if isinstance(row, dict):
                    row = utils.DotDict(row)

                tup = (row.candidate_election_id,
                       row.votes_obtained,
                       row.votes_percentage,
                       row.majority,
                       row.majority_percentage
                       )

                try:
                    cur.execute(insert_candidate_election_votes_query, tup)
                except Exception as e:
                    print(
                        f'An exception occurred inserting {row.candidate_election_id}:\n{e}')
                    cur.connection.rollback()

    @staticmethod
    def write_ca_vote_data(data, table):
        with CursorFromConnectionFromPool() as cur:
            try:
                create_table_query = sql.SQL("""
                  
                    CREATE TABLE IF NOT EXISTS {table} (
                        name text,
                        goverlytics_id bigint UNIQUE,
                        source_id text,
                        voting_data jsonb
                    );

                    ALTER TABLE {table} OWNER TO rds_ad;
                """).format(table=sql.Identifier(table))

                cur.execute(create_table_query)
                cur.connection.commit()

            except Exception as e:
                print(
                    f'An exception occurred creating {table}:\n{e}')
                cur.connection.rollback()

        insert_legislator_query = sql.SQL("""
            INSERT INTO {table}
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (goverlytics_id) DO UPDATE SET
                name = excluded.name,
                goverlytics_id = excluded.goverlytics_id,
                source_id = excluded.source_id,
                voting_data = excluded.voting_data
            """).format(table=sql.Identifier(table))

        for row in data:
            if isinstance(row, dict):
                row = utils.DotDict(row)

            tup = (row.name, row.goverlytics_id, row.source_id, json.dumps(row.voting_data, default=utils.json_serial))

            try:
                cur.execute(insert_legislator_query, tup)

            except Exception as e:
                print(
                    f'An exception occurred inserting {row.goverlytics_id}:\n{e}')
                cur.connection.rollback()

    @staticmethod
    def write_outflows(data, table):

        with CursorFromConnectionFromPool() as cur:
            insert_outflows_query = sql.SQL("""
                               INSERT INTO {table}
                               VALUES (
                                   DEFAULT, %s, %s, %s, %s, %s, %s, %s, %s,
                                   %s, %s, %s, %s, %s, %s)
                               ON CONFLICT DO NOTHING;
                               """).format(table=sql.Identifier(table))

            for row in data:
                if isinstance(row, dict):
                    row = utils.DotDict(row)

                tup = (row.candidate_election_finances_id,
                       row.expenses_limit,
                       row.total_expenses_subject_to_limit,
                       row.total_expenses_subject_to_limit_detail,
                       row.personal_expenses,
                       row.personal_expenses_detail,
                       row.other_expenses,
                       row.other_detail,
                       row.campaign_expenses,
                       row.contributed_transferred_property_or_service,
                       row.non_monetary_transfers_sent_to_political_entities,
                       row.unpaid_claims,
                       row.unpaid_claims_detail,
                       row.total_outflows)

                try:
                    cur.execute(insert_outflows_query, tup)
                except Exception as e:
                    print(
                        f'An exception occurred inserting {row.candidate_election_finances_id}:\n{e}')

    @staticmethod
    def write_bank_reconciliation(data, table):

        with CursorFromConnectionFromPool() as cur:
            insert_bank_reconciliation_query = sql.SQL("""
                               INSERT INTO {table}
                               VALUES (
                                   DEFAULT, %s, %s, %s)
                               ON CONFLICT DO NOTHING;
                               """).format(table=sql.Identifier(table))

            for row in data:
                if isinstance(row, dict):
                    row = utils.DotDict(row)

                tup = (row.candidate_election_finances_id,
                       row.inflow,
                       row.outflow,
                       row.surplus)

                try:
                    cur.execute(insert_bank_reconciliation_query, tup)
                except Exception as e:
                    print(
                        f'An exception occurred inserting {row.candidate_election_finances_id}:\n{e}')

    @staticmethod
    def write_bank_account(data, table):

        with CursorFromConnectionFromPool() as cur:
            insert_bank_account_query = sql.SQL("""
                               INSERT INTO {table}
                               VALUES (
                                   DEFAULT, %s, %s, %s, %s, %s, %s, %s)
                               ON CONFLICT DO NOTHING;
                               """).format(table=sql.Identifier(table))

            for row in data:
                if isinstance(row, dict):
                    row = utils.DotDict(row)

                tup = (row.candidate_election_finances_id,
                       row.total_credits,
                       row.total_debits,
                       row.total_balance,
                       row.outstanding_cheques,
                       row.deposits_in_transit,
                       row.account_balance)

                try:
                    cur.execute(insert_bank_account_query, tup)
                except Exception as e:
                    print(
                        f'An exception occurred inserting {row.candidate_election_finances_id}:\n{e}')
