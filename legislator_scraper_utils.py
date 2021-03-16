import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
from datetime import date, datetime
import json
import sys
import pandas as pd
from database import Database, CursorFromConnectionFromPool
from dataclasses import dataclass, field
from typing import List
from rows import *
import copy
import atexit
import utils

"""
Contains utilities and data structures meant to help resolve common issues
that occur with data collection. These can be used with your legislator
date collectors.
"""


class LegislatorScraperUtils():

    def __init__(self, country, database_table_name, row_type):

        Database.initialise()
        # self.db = Database()
        # atexit.register(self.db.close_all_connections)

        with CursorFromConnectionFromPool() as cur:
            try:
                query = 'SELECT * FROM countries'
                cur.execute(query)
                countries_results = cur.fetchall()

                query = f'SELECT * FROM {country}_parties'
                cur.execute(query)
                parties_results = cur.fetchall()

                query = f'SELECT * FROM {country}_divisions'
                cur.execute(query)
                division_results = cur.fetchall()
            except Exception as e:
                sys.exit(f'An exception occurred retrieving tables from database:\n{e}')

        self.countries = pd.DataFrame(countries_results)
        self.parties = pd.DataFrame(parties_results)
        self.divisions = pd.DataFrame(division_results)

        self.country = self.countries.loc[self.countries['abbreviation'] == country]['country'].values[0]
        self.country_id = int(self.countries.loc[self.countries['abbreviation'] == country]['id'].values[0])
        self.database_table_name = database_table_name
        self.row_type = row_type



    def initialize_row(self):
        row = copy.deepcopy(self.row_type)
        row.country_id = self.country_id
        row.country = self.country
        return row

    def get_attribute_id(self, table_name, column_to_search, value):
        accepted_tables = ['country', 'party', 'division']
        if table_name not in accepted_tables:
            raise Exception(f'Error: table must be one of the following: {accepted_tables}')

        if table_name == 'country':
            df = self.countries
        if table_name == 'party':
            df = self.parties
        if table_name == 'division':
            df = self.divisions

        try:
            return int(df.loc[df[column_to_search] == value]['id'].values[0])
        except Exception as e:
            raise Exception(f'Error retrieving ID from table {table_name}: {e}')

    def get_party_id(self, party_name):
        """
        Used for getting the party ID number.
        """
        return self.get_attribute_id('party', 'party', party_name)


class USFedLegislatorScraperUtils(LegislatorScraperUtils):
    """
    Utilities to help with collecting and storing legislator data.
    """

    def __init__(self, database_table_name='us_fed_legislators'):
        """
        The state_abbreviation, database_table_name, and country come from
        the config.cfg file and must be updated to work properly with your legislation
        data collector.
        """
        super().__init__('us', database_table_name, USLegislatorRow())

    def get_state_id(self, state_abbreviation):
        return self.get_attribute_id('division', 'abbreviation', state_abbreviation)

    def insert_legislator_data_into_db(self, data):
        """
        """
        if not isinstance(data, list):
            raise TypeError('Data being written to database must be a list of USStateLegislationRows or dictionaries!')

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
                            phone_number jsonb,
                            addresses jsonb,
                            email text,
                            birthday date,
                            seniority int,
                            occupation text[],
                            education jsonb,
                            military_experience text
                        );

                        ALTER TABLE {table} OWNER TO rds_ad;
                        """).format(table=sql.Identifier(self.database_table_name))

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
                        phone_number = excluded.phone_number,
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
                    """).format(table=sql.Identifier(self.database_table_name))

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
                    json.dumps(item.phone_number, default=utils.json_serial),
                    json.dumps(item.addresses, default=utils.json_serial),
                    item.email,
                    item.birthday,
                    item.seniority,
                    item.occupation,
                    json.dumps(item.education, default=utils.json_serial),
                    item.military_experience
                )

                cur.execute(insert_legislator_query, tup)


class USStateLegislatorScraperUtils(USFedLegislatorScraperUtils):
    def __init__(self, state_abbreviation, database_table_name='us_state_legislators'):
        super().__init__(database_table_name)
        self.state = state_abbreviation
        self.state_id = self.get_state_id(state_abbreviation)

    def initialize_row(self):
        row = super().initialize_row()
        row.state = self.state
        row.state_id = self.state_id
        return row

class CadFedLegislatorScraperUtils(LegislatorScraperUtils):
    """
    Utilities to help with collecting and storing legislator data.
    """

    def __init__(self, database_table_name='cad_fed_legislators'):
        """
        The state_abbreviation, database_table_name, and country come from
        the config.cfg file and must be updated to work properly with your legislation
        data collector.
        """
        super().__init__('cad', database_table_name, CadLegislatorRow())

    def get_prov_terr_id(self, prov_terr_id):
        return self.get_attribute_id('division', 'abbreviation', prov_terr_id)
    
    def insert_legislator_data_into_db(self, data):
        """
        """
        if not isinstance(data, list):
            raise TypeError('Data being written to database must be a list of USStateLegislationRows or dictionaries!')

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
                            phone_number jsonb,
                            addresses jsonb,
                            email text,
                            birthday date,
                            seniority int,
                            occupation text[],
                            education jsonb,
                            military_experience text
                        );

                        ALTER TABLE {table} OWNER TO rds_ad;
                        """).format(table=sql.Identifier(self.database_table_name))

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
                        %s, %s, %s, %s, %s, %s, %s)
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
                        phone_number = excluded.phone_number,
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
                    """).format(table=sql.Identifier(self.database_table_name))

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
                    json.dumps(item.phone_number, default=utils.json_serial),
                    json.dumps(item.addresses, default=utils.json_serial),
                    item.email,
                    item.birthday,
                    item.seniority,
                    item.occupation,
                    json.dumps(item.education, default=utils.json_serial),
                    item.military_experience
                )
                print(tup)

                cur.execute(insert_legislator_query, tup)


class CadProvTerrLegislatorScraperUtils(CadFedLegislatorScraperUtils):
    def __init__(self, prov_terr_abbreviation, database_table_name='cad_provterr_legislators'):
        super().__init__(database_table_name)
        self.province_territory = prov_terr_abbreviation
        self.province_territory_id = self.get_prov_terr_id(prov_terr_abbreviation)

    def initialize_row(self):
        row = super().initialize_row()
        row.province_territory = self.province_territory
        row.province_territory_id = self.province_territory_id
        return row
