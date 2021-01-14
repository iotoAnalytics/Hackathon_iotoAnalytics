import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
from datetime import date, datetime
import json
import sys
from database_tables import DatabaseTables
from database import CursorFromConnectionFromPool


columns = [
    'state_member_id',
    'most_recent_term_id',
    'state_url',
    'url',
    'name_full',
    'name_last',
    'name_first',
    'name_middle',
    'name_suffix',
    'country_id',
    'country',
    'state_id',
    'state',
    'party_id',
    'party',
    'role',
    'district',
    'years_active',
    'committees',
    'areas_served',
    'phone_number',
    'addresses',
    'email',
    'birthday',
    'seniority',
    'occupation',
    'education',
    'military_experience'
]


class ScraperUtils:
    def __init__(self, state_abbreviation, database_table_name, country):
        self.state_abbreviation = state_abbreviation
        self.database_table_name = database_table_name
        self.country = country
        self.db_tables = DatabaseTables()

    
    def __json_serial(self, obj):
        """ Serializes date/datetime object. """
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        raise TypeError("Type %s not serializable" % type(obj))

    
    def initialize_row(self):
        '''
        Create a row and fill with empty values. This gets sent back to the scrape() function
        which then gets filled in with values collected from the website.
        '''

        row = {column_name: '' for column_name in columns}
        
        try:
            row['country'] = self.country
            row['country_id'] = int(self.db_tables.countries.loc[self.db_tables.countries['country'] == self.country]['id'].values[0])
            row['state'] = self.state_abbreviation
            row['state_id'] = int(self.db_tables.states.loc[self.db_tables.states['abbreviation'] == self.state_abbreviation]['state_no'].values[0])
        except IndexError:
            sys.exit('An error occurred inserting state_id and/or country_id. Did you update the config file?')
        except Exception as e:
            sys.exit(f'An error occurred involving the state_id and/or country_id: {e}')
        
        row['years_active'] = []
        row['committees'] = {}
        row['areas_served'] = []
        row['phone_number'] = {}
        row['seniority'] = 0
        row['occupation'] = []
        row['education'] = {}
        row['addresses'] = []
        row['birthday'] = None

        return row

    
    def get_party_id(self, party_name):
        try:
            party_id = int(self.db_tables.parties.loc[self.db_tables.parties['party'] == party_name]['id'].values[0])
        except IndexError:
            sys.exit('An error occurred getting party_id.\nParty not found. Has the party been collected, and is it in the correct format?')
        except Exception as e:
            sys.exit(f'An error occurred involving the party_id: {e}')

        return party_id

    
    def insert_legislator_data_into_db(self, data):

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
                    tup = (row['state_member_id'], row['most_recent_term_id'], date_collected, row['state_url'],
                    row['name_full'], row['name_last'], row['name_first'], row['name_middle'], row['name_suffix'],
                    row['country_id'], row['country'], row['state_id'], row['state'], row['party_id'], row['party'],
                    row['role'], row['district'], row['years_active'],
                    json.dumps(row['committees'], default=ScraperUtils.__json_serial),
                    row['areas_served'],
                    json.dumps(row['phone_number'], default=ScraperUtils.__json_serial),
                    json.dumps(row['addresses'], default=ScraperUtils.__json_serial),
                    row['email'], row['birthday'], row['seniority'],
                    row['occupation'],
                    json.dumps(row['education'], default=ScraperUtils.__json_serial),
                    row['military_experience'])

                    # print(f'Inserting <Row {row["state_url"]}>')
                    curs.execute(insert_legislator_query, tup)

                except Exception as e:
                    print(f'An exception occurred inserting <Row {row["state_url"]}>: {e}')