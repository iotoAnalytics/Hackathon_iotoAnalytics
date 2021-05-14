"""
Used for connecting scrapers to relational database using psycopg2.
Author: Justin Tendeck
"""

from psycopg2.extras import RealDictCursor
from psycopg2 import pool
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
    _connection_pool = None

    @staticmethod
    def initialise():
        """
        Generates database connection token then connects to database via
        connection pooling. Must be run before attempting to connect to
        the database.
        """
        db_token = client.generate_db_auth_token(
            db_host, db_port, db_user, Region=db_region)

        Database._connection_pool = pool.SimpleConnectionPool(1,
                                                               10,
                                                               database=db_name, host=db_host, user=db_user, password=db_token)

    @classmethod
    def get_connection(cls):
        """
        Returns a connection from the database connection pool.

        Returns:
            connection: a psycopg2 connection to the database.
        """
        return cls._connection_pool.getconn()

    @classmethod
    def return_connection(cls, connection):
        """
        Returns a database connection back to the database connection pool.

        Args:
            connection: Psycopg2 database connection to be returned.
        """
        Database._connection_pool.putconn(connection)

    @classmethod
    def close_all_connections(cls):
        """
        Closes all connections in the connection pool.
        """
        Database._connection_pool.closeall()

    def __del__(self):
        if Database._connection_pool:
            Database._connection_pool.closeall()


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


class Persistence:
    """Class for writing collected data to database"""
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

                for v in row:
                    print(v, type(v))

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
                    import traceback
                    print(traceback.format_exc())
                    print(
                        f'An exception occurred inserting {row.goverlytics_id}:\n{e}')

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
                            military_experience text
                        );

                        ALTER TABLE {table} OWNER TO rds_ad;
                        """).format(table=sql.Identifier(table))

                cur.execute(create_table_query)
                cur.connection.commit()

            except Exception as e:
                print(f'An exception occurred executing a query:\n{e}')

            insert_legislator_query = sql.SQL("""
            
                    WITH leg_id AS (SELECT NEXTVAL('legislator_id') leg_id)
                    INSERT INTO {table}
                    VALUES (
                        (SELECT leg_id FROM leg_id), %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
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
                    item.military_experience
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
                            parl_assoc_interparl_groups jsonb
                        );

                        ALTER TABLE {table} OWNER TO rds_ad;
                        """).format(table=sql.Identifier(table))

                cur.execute(create_table_query)
                cur.connection.commit()
            except Exception as e:
                print(f'An exception occurred executing a query:\n{e}')

            insert_legislator_query = sql.SQL("""
                    WITH leg_id AS (SELECT NEXTVAL('legislator_id') leg_id)
                    INSERT INTO {table}
                    VALUES (
                        (SELECT leg_id FROM leg_id), %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                               default=utils.json_serial)
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
                            region text
                        );

                        ALTER TABLE {table} OWNER TO rds_ad;
                        """).format(table=sql.Identifier(table))

                cur.execute(create_table_query)
                cur.connection.commit()
            except Exception as e:
                print(f'An exception occurred executing a query:\n{e}')

            insert_legislator_query = sql.SQL("""
                    
                    WITH leg_id AS (SELECT NEXTVAL('legislator_id') leg_id)
                    INSERT INTO {table}
                    VALUES (
                        (SELECT leg_id FROM leg_id), %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
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
                    item.region
                )

                cur.execute(insert_legislator_query, tup)

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
