import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
from datetime import date, datetime
import json
import sys
import pandas as pd
from database import CursorFromConnectionFromPool, Database
from dataclasses import dataclass, field
from typing import List

"""
Contains utilities and data structures meant to help resolve common issues
that occur with data collection. These can be used with your legislator
date collectors.
"""

@dataclass
class LegislatorRow:
    """
    Data structure for housing data about each piece of legislator.
    """
    state_member_id: str = ''
    most_recent_term_id: str = ''
    state_url: str = ''
    name_full: str = ''
    name_last: str = ''
    name_first: str = ''
    name_middle: str = ''
    name_suffix: str = ''
    country_id: int = None
    country: str = ''
    state_id: int = None
    state: str = ''
    party_id: int = None
    party: str = ''
    role: str = ''
    district: str = ''
    years_active: List[int] = field(default_factory=list)
    committees: List[dict] = field(default_factory=list)
    areas_served: List[str] = field(default_factory=list)
    phone_number: List[dict] = field(default_factory=list)
    addresses: List[dict] = field(default_factory=list)
    email: str = ''
    birthday: datetime = None
    seniority: int = 0
    occupation: List[str] = field(default_factory=list)
    education: List[dict] = field(default_factory=list)
    military_experience: str = ''
    

class LegislatorScraperUtils:
    """
    Utilities to help with collecting and storing legislator data.
    """
    def __init__(self, state_abbreviation, database_table_name, country):
        """
        The state_abbreviation, database_table_name, and country come from
        the config.cfg file and must be updated to work properly with your legislation
        data collector.
        """
        self.state_abbreviation = state_abbreviation
        self.database_table_name = database_table_name
        self.country = country

        Database.initialise()

        with CursorFromConnectionFromPool() as curs:
            try:
                query = 'SELECT * FROM us_parties'
                curs.execute(query)
                parties_results = curs.fetchall()

                query = 'SELECT * FROM countries'
                curs.execute(query)
                countries_results = curs.fetchall()

                query = 'SELECT state_no, state_name, abbreviation FROM us_state_info'
                curs.execute(query)
                state_results = curs.fetchall()
            except Exception as e:
                sys.exit(f'An exception occurred retrieving us_parties, countries, us_state_info from database:\n{e}')

        self.parties = pd.DataFrame(parties_results)
        self.countries = pd.DataFrame(countries_results)
        self.states = pd.DataFrame(state_results)

    
    def __json_serial(self, obj):
        """
        Serializes date/datetime object. This is used to convert date and datetime objects to
        a format that can be digested by the database.
        """
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        raise TypeError("Type %s not serializable" % type(obj))

    
    def initialize_row(self):
        '''
        Create a row and fill with empty values. This gets sent back to the scrape() function
        which then gets filled in with values collected from the website.
        '''

        row = LegislatorRow()
        
        try:
            row.country = self.country
            row.country_id = int(self.countries.loc[self.countries['country'] == self.country]['id'].values[0])
            row.state = self.state_abbreviation
            row.state_id = int(self.states.loc[self.states['abbreviation'] == self.state_abbreviation]['state_no'].values[0])
        except IndexError:
            sys.exit('An error occurred inserting state_id and/or country_id. Did you update the config file?')
        except Exception as e:
            sys.exit(f'An error occurred involving the state_id and/or country_id: {e}')

        return row

    
    def get_party_id(self, party_name):
        """
        Used for getting the party ID number.
        """
        try:
            party_id = int(self.parties.loc[self.parties['party'] == party_name]['id'].values[0])
        except IndexError:
            sys.exit('An error occurred getting party_id.\nParty not found. Has the party been collected, and is it in the correct format?')
        except Exception as e:
            sys.exit(f'An error occurred involving the party_id: {e}')

        return party_id

    
    def insert_legislator_data_into_db(self, data):
        """
        Takes care of inserting legislator data into database.
        """

        if not isinstance(data, list):
            raise TypeError('Data being written to database must be a list of LegislationRows!')

        with CursorFromConnectionFromPool() as curs:
            try:
                create_table_query = sql.SQL("""
                    CREATE TABLE IF NOT EXISTS {table} (
                        goverlytics_id bigint PRIMARY KEY,
                        state_member_id text,
                        most_recent_term_id text,
                        date_collected timestamp,
                        state_url TEXT UNIQUE,
                        url text,
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

                curs.execute(create_table_query)

            except Exception as e:
                print(f'An exception occurred creating {self.database_table_name}:\n{e}')

            insert_legislator_query = sql.SQL("""
                WITH leg_id AS (SELECT NEXTVAL('legislator_id') leg_id)
                INSERT INTO {table}
                VALUES (
                    (SELECT leg_id FROM leg_id), %s, %s, %s, %s,
                    CONCAT('us/{state}/legislators/', (SELECT leg_id FROM leg_id)),
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (state_url) DO UPDATE SET
                    date_collected = excluded.date_collected,
                    name_full = excluded.name_full,
                    name_last = excluded.name_last,
                    name_first = excluded.name_first,
                    name_middle = excluded.name_middle,
                    name_suffix = excluded.name_suffix,
                    country_id = excluded.country_id,
                    country = excluded.country,
                    state_id = excluded.state_id,
                    state = excluded.state,
                    party_id = excluded.party_id,
                    party = excluded.party,
                    district = excluded.district,
                    role = excluded.role,
                    committees = excluded.committees,
                    areas_served = excluded.areas_served,
                    phone_number = excluded.phone_number,
                    addresses = excluded.addresses,
                    email = excluded.email,
                    birthday = excluded.birthday,
                    military_experience = excluded.military_experience,
                    occupation = excluded.occupation,
                    education = excluded.education,
                    state_member_id = excluded.state_member_id,
                    most_recent_term_id = excluded.most_recent_term_id,
                    years_active = excluded.years_active,
                    seniority = excluded.seniority;
                """).format(table=sql.Identifier(self.database_table_name), state=sql.SQL(self.state_abbreviation))

            date_collected = datetime.now()

            for row in data:
                try:

                    tup = (row.state_member_id, row.most_recent_term_id, date_collected, row.state_url,
                    row.name_full, row.name_last, row.name_first, row.name_middle, row.name_suffix,
                    row.country_id, row.country, row.state_id, row.state, row.party_id, row.party,
                    row.role, row.district, row.years_active,
                    json.dumps(row.committees, default=LegislatorScraperUtils.__json_serial),
                    row.areas_served,
                    json.dumps(row.phone_number, default=LegislatorScraperUtils.__json_serial),
                    json.dumps(row.addresses, default=LegislatorScraperUtils.__json_serial),
                    row.email, row.birthday, row.seniority,
                    row.occupation,
                    json.dumps(row.education, default=LegislatorScraperUtils.__json_serial),
                    row.military_experience)

                    curs.execute(insert_legislator_query, tup)

                except Exception as e:
                    print(f'An exception occurred inserting {row.state_url}: {e}')