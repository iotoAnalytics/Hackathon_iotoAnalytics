"""
A collection of classes and function used to fasciliate the collection of legislation
data. Classes are designed to be extended to help collect data for other countries
and jurisdictions.
Author: Justin Tendeck
"""

import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
import json
import sys
from database import Database, CursorFromConnectionFromPool
import pandas as pd
from pandas.core.computation.ops import UndefinedVariableError
from rows import *
import numpy
import atexit
import copy
import utils
from datetime import date, datetime


class LegislationScraperUtils:
    """
    Base class containing common methods and attributes that can be used by all
    legislation scrapers.
    """

    def __init__(self, country: str, database_table_name: str, legislator_table_name: str, row_type: LegislationRow):
        """
        Stores arguments as instance variables. Instantiates a database object and establishes
        database connection pool. Pulls country, legislator, and division (ie: states, provinces,
        etc.) tables from database and creates dataframes, which are used by other methods in
        this class.

        Args:
            country: Country this scraper is used for
            database_table_name: Database table where the data will be stored
            legislation_table_name: Name of table containing legislator data that will
                be used in various methods in this class
            row_type: The type of row that this scraper will generate.
        """

        self.database_table_name = database_table_name
        self.legislator_table_name = legislator_table_name

        Database.initialise()
        atexit.register(Database.close_all_connections)

        with CursorFromConnectionFromPool() as cur:
            try:
                query = f'SELECT * FROM {country}_divisions'
                cur.execute(query)
                division_results = cur.fetchall()

                query = f'SELECT * FROM {legislator_table_name}'
                cur.execute(query)
                legislator_results = cur.fetchall()

                query = f'SELECT * FROM countries'
                cur.execute(query)
                country_results = cur.fetchall()

                query = f'SELECT * FROM {country}_parties'
                cur.execute(query)
                party_results = cur.fetchall()
            except Exception as e:
                sys.exit(f'An exception occurred retrieving either US parties or legislator table from database. \
                \nHas the legislator data been collected for this state yet?\n{e}')

        self.divisions = pd.DataFrame(division_results)
        self.legislators = pd.DataFrame(legislator_results)
        self.countries = pd.DataFrame(country_results)
        self.parties = pd.DataFrame(party_results)

        self.country = self.countries.loc[self.countries['abbreviation'] == country]['country'].values[0]
        self.country_id = int(self.countries.loc[self.countries['abbreviation'] == country]['id'].values[0])

        self.row_type = row_type

    def get_attribute(self, table_name, column_to_search, value_to_search, attribute_to_return='id'):
        accepted_tables = ['country', 'legislator', 'division', 'party']
        if table_name not in accepted_tables:
            raise Exception(f'Error: table must be one of the following: {accepted_tables}')

        if table_name == 'country':
            df = self.countries
        if table_name == 'legislator':
            df = self.legislators
        if table_name == 'division':
            df = self.divisions
        if table_name == 'party':
            df = self.parties
        
        val = df.loc[df[column_to_search] == value_to_search][attribute_to_return]
        if val.any():
            try:
                return int(val.values[0])
            except Exception:
                return val.values[0]
        else:
            raise Exception(f'Could not locate value using following search parameters: table_name={table_name}, column_to_search={column_to_search}, value_to_search={value_to_search}, attribute_to_return={attribute_to_return}')

    def _convert_to_int(self, value):
        """
        Used to try and convert values into int. Functions like df.loc might return
        a numpy.int64 which is incompatible with the database, so this function must
        be used.
        """
        try:
            value = int(value)
        except ValueError:
            pass
        return value

    def _convert_value_to_column_type(self, column, value):
        str_columns = ['source_id']

        if column in str_columns:
            return str(value)
        else:
            return self._convert_to_int(value)

    def initialize_row(self):
        '''
        Factory method for creating a legislation row. This gets sent back to the scrape() function
        which then gets filled in with values collected from the website.
        '''
        row = copy.deepcopy(self.row_type)
        row.country = self.country
        row.country_id = self.country_id
        return row

    def search_for_legislators(self, **kwargs) -> pd.DataFrame:
        """
        Returns a dataframe containing search results based on kwargs.
        """

        query_lst = []
        for k, v in kwargs.items():
            q = ''

            # Certain fields may be converted to int while they need to stay as strings
            v = self._convert_value_to_column_type(k, v)

            if isinstance(v, int):
                q = f'{k}=={v}'
            elif isinstance(v, str):
                q = f'{k}=="{v}"'
            else:
                print(f'Unable to use {k}: {v} as search parameter. Must search by either a text or int column.')
                continue
            query_lst.append(q)

        query = ' & '.join(query_lst)
        try:
            df = self.legislators.query(query)
        except UndefinedVariableError as e:
            print(f'Column not found: {e}')
            return None
        except Exception as e:
            print(f'An error occurred finding legislator: {e}')
            return None

        if len(df) > 1:
            print(f'WARNING: More than one legislator found using {kwargs} search parameter.')
        if len(df) == 0:
            print(f'WARNING: No legislators found while searching {kwargs}!')
            return None

        return df

    def get_legislator_id(self, **kwargs) -> int:
        """
        Method for getting the Goverlytics ID based on search parameters.
        """
        df = self.search_for_legislators(**kwargs)
        if df is not None:
            return self._convert_to_int(df.iloc[0]['goverlytics_id'])
        else:
            return None

    def legislators_search_startswith(self, column_val_to_return, column_to_search, startswith, **kwargs):
        """
        Utilizes panda's .startswith method for finding information about legislators. Useful for finding
        things like the Goverlytics ID when given only the first initial and last name of a legislator.
        """
        val = None

        if not kwargs:
            print('Must include kwargs when using legislators_search_startswith!')
            return val

        df = self.search_for_legislators(**kwargs)

        startswith = self._convert_value_to_column_type(column_to_search, startswith)

        if df is not None:
            try:
                val = df.loc[df[column_to_search].str.startswith(startswith)][column_val_to_return].values[0]
            except IndexError:
                print(
                    f"Unable to find '{column_val_to_return}' using these search parameters: {column_to_search} : {startswith}")
            except KeyError:
                print(f"'{column_to_search}' is not a valid column name in the legislator data frame!")
            except AttributeError:
                print('Can only search columns of type str/text when using legislators_search_startswith!')
            except Exception as e:
                print('An exception occurred: {e}')
        if isinstance(val, numpy.int64):
            val = int(val)
        return val


class USFedLegislationScraperUtils(LegislationScraperUtils):

    def __init__(self, database_table_name='us_fed_legislation', legislator_table_name='us_fed_legislators'):
        super().__init__('us', database_table_name, legislator_table_name, USLegislationRow())

    def insert_legislation_data_into_db(self, data) -> None:
        """
        Takes care of inserting legislation data into database.
        """
        if not isinstance(data, list):
            raise TypeError('Data being written to database must be a list of USStateLegislationRows!')

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
                    """).format(table=sql.Identifier(self.database_table_name))

                cur.execute(create_table_query)
                cur.connection.commit()

            except Exception as e:
                print(f'An exception occurred creating {self.database_table_name}:\n{e}')

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
                    cosponsors = excluded.sponsors,
                    cosponsors_id = excluded.cosponsors_id,
                    topic = excluded.topic,
                    bill_text = excluded.bill_text,
                    bill_description = excluded.bill_description,
                    bill_summary = excluded.bill_summary,
                    country_id = excluded.country_id,
                    country = excluded.country;
                """).format(table=sql.Identifier(self.database_table_name))

            date_collected = datetime.now()

            for row in data:

                if isinstance(row, dict):
                    row = utils.DotDict(row)

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
                    print(f'An exception occurred inserting {row.goverlytics_id}:\n{e}')


class USStateLegislationScraperUtils(USFedLegislationScraperUtils):
    def __init__(self, state_abbreviation, database_table_name='us_state_legislation',
                 legislator_table_name='us_state_legislators'):
        super().__init__(database_table_name, legislator_table_name)
        self.state = state_abbreviation
        self.state_id = int(self.divisions.loc[self.divisions['abbreviation'] == state_abbreviation]['id'].values[0])

    def initialize_row(self):
        row = super().initialize_row()
        row.state = self.state
        row.state_id = self.state_id
        return row


class CAFedLegislationScraperUtils(LegislationScraperUtils):
    def __init__(self, database_table_name='ca_fed_legislation', legislator_table_name='ca_fed_legislators', row_type=CAFedLegislationRow()):
        super().__init__('ca', database_table_name, legislator_table_name, row_type)

    def insert_legislation_data_into_db(self, data) -> None:
        """
        Takes care of inserting legislation data into database.
        """
        if not isinstance(data, list):
            raise TypeError('Data being written to database must be a list of USStateLegislationRows!')

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
                    """).format(table=sql.Identifier(self.database_table_name))

                cur.execute(create_table_query)
                cur.connection.commit()

            except Exception as e:
                print(f'An exception occurred creating {self.database_table_name}:\n{e}')

            insert_legislator_query = sql.SQL("""
                INSERT INTO {table}
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
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
                    source_topic = excluded.source_topic,
                    votes = excluded.votes,
                    goverlytics_id = excluded.goverlytics_id,
                    source_id = excluded.source_id,
                    committees = excluded.committees,
                    cosponsors = excluded.sponsors,
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
                """).format(table=sql.Identifier(self.database_table_name))

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
                    row.source_topic, row.topic, row.country_id, row.country,
                    row.sponsor_affiliation, row.sponsor_gender, row.pm_name_full,
                    row.pm_party, row.pm_party_id, row.statute_year, row.statute_chapter,
                    row.publications,
                    json.dumps(row.last_major_event, default=utils.json_serial))

                try:
                    cur.execute(insert_legislator_query, tup)

                except Exception as e:
                    print(f'An exception occurred inserting {row.goverlytics_id}:\n{e}')


class CAProvinceTerrLegislationScraperUtils(CAFedLegislationScraperUtils):
    def __init__(self, prov_terr_abbreviation, database_table_name='ca_provterr_legislation',
                 legislator_table_name='ca_provterr_legislators'):
        super().__init__(database_table_name, legislator_table_name, CALegislationRow())
        self.province_territory = prov_terr_abbreviation
        self.province_territory_id = int(
            self.divisions.loc[self.divisions['abbreviation'] == prov_terr_abbreviation]['id'].values[0])

    def initialize_row(self):
        row = super().initialize_row()
        row.province_territory = self.province_territory
        row.province_territory_id = self.province_territory_id
        return row

    def insert_legislation_data_into_db(self, data) -> None:
        """
        Takes care of inserting legislation data into database.
        """
        if not isinstance(data, list):
            raise TypeError('Data being written to database must be a list of USStateLegislationRows!')

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
                    """).format(table=sql.Identifier(self.database_table_name))

                cur.execute(create_table_query)
                cur.connection.commit()

            except Exception as e:
                print(f'An exception occurred creating {self.database_table_name}:\n{e}')

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
                    cosponsors = excluded.sponsors,
                    cosponsors_id = excluded.cosponsors_id,
                    topic = excluded.topic,
                    bill_text = excluded.bill_text,
                    bill_description = excluded.bill_description,
                    bill_summary = excluded.bill_summary,
                    country_id = excluded.country_id,
                    country = excluded.country;
                """).format(table=sql.Identifier(self.database_table_name))

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
                    print(f'An exception occurred inserting {row.goverlytics_id}:\n{e}')
