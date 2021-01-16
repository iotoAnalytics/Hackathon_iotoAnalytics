import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
from datetime import date, datetime
import json
import sys
from database_tables import DatabaseTables
from database import CursorFromConnectionFromPool
import pandas as pd
from pandas.core.computation.ops import UndefinedVariableError


columns = [
    'goverlytics_id',
    'bill_state_id',
    'bill_name',
    'session',
    'date_introduced',
    'state_url',
    'url',
    'chamber_origin',
    'committees',
    'state_id',
    'state',
    'bill_type',
    'bill_title',
    'current_status',
    'principal_sponsor_id',
    'principal_sponsor',
    'sponsors',
    'sponsors_id',
    'cosponsors',
    'cosponsors_id',
    'bill_text',
    'bill_description',
    'bill_summary',
    'actions',
    'votes',
    'topic'
]


class LegislationScraperUtils:
    def __init__(self, state_abbreviation, database_table_name, legislator_table_name):
        self.state_abbreviation = state_abbreviation
        self.database_table_name = database_table_name
        self.legislator_table_name = legislator_table_name
        
        with CursorFromConnectionFromPool() as curs:
            try:
                query = 'SELECT state_no, state_name, abbreviation FROM us_state_info'
                curs.execute(query)
                state_results = curs.fetchall()

                query = f'SELECT * FROM {legislator_table_name}'
                curs.execute(query)
                legislator_results = curs.fetchall()
            except Exception as e:
                sys.exit(f'An exception occurred retrieving either US parties or state legislator table from database. \
                \nHas the legislator data been collected for this state yet?\n{e}')

        self.states = pd.DataFrame(state_results)
        self.legislators = pd.DataFrame(legislator_results)

    
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
            row['state'] = self.state_abbreviation
            row['state_id'] = int(self.states.loc[self.states['abbreviation'] == self.state_abbreviation]['state_no'].values[0])
        except IndexError:
            sys.exit('An error occurred inserting state_id. Has the config file been updated?')
        except Exception as e:
            sys.exit(f'An error occurred involving the state_id and/or country_id: {e}')
        
        row['date_introduced'] = None
        row['committees'] = {}
        row['principal_sponsor_id'] = 0
        row['sponsors'] = []
        row['sponsors_id'] = []
        row['cosponsors'] = []
        row['cosponsors_id'] = []
        row['actions'] = {}
        row['votes'] = {}

        return row
    
    def insert_legislator_data_into_db(self, data : list):

        with CursorFromConnectionFromPool() as curs:
            try:
                create_table_query = sql.SQL("""
                    CREATE TABLE IF NOT EXISTS {table} (
                        goverlytics_id text PRIMARY KEY,
                        bill_state_id text,
                        date_collected timestamp,
                        bill_name text,
                        session TEXT,
                        date_introduced date,
                        state_url text UNIQUE,
                        url text,
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
                        site_topic text,
                        topic text
                    );
                    """).format(table=sql.Identifier(self.database_table_name))

                curs.execute(create_table_query)

            except Exception as e:
                print(f'An exception occurred creating {self.database_table_name}:\n{e}')

            insert_legislator_query = sql.SQL("""
                INSERT INTO {table}
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (state_url) DO UPDATE SET
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
                    site_topic = excluded.site_topic,
                    votes = excluded.votes,
                    goverlytics_id = excluded.goverlytics_id,
                    url = excluded.url,
                    bill_state_id = excluded.bill_state_id,
                    committees = excluded.committees,
                    cosponsors = excluded.sponsors,
                    cosponsors_id = excluded.cosponsors_id,
                    topic = excluded.topic,
                    bill_text = excluded.bill_text,
                    bill_description = excluded.bill_description,
                    bill_summary = excluded.bill_summary;
                """).format(table=sql.Identifier(self.database_table_name), state=sql.SQL(self.state_abbreviation))

            date_collected = datetime.now()

            for row in data:
                try:
                    tup = (row['goverlytics_id'], row['bill_state_id'], date_collected, row['bill_name'],
                    row['session'], row['date_introduced'], row['state_url'], row['url'], row['chamber_origin'],
                    json.dumps(row['committees'], default=LegislationScraperUtils.__json_serial),
                    row['state_id'], row['state'], row['bill_type'], row['bill_title'], row['current_status'],
                    row['principal_sponsor_id'], row['principal_sponsor'], row['sponsors'], row['sponsors_id'],
                    row['cosponsors'], row['cosponsors_id'], row['bill_text'], row['bill_description'], row['bill_summary'],
                    json.dumps(row['actions'], default=LegislationScraperUtils.__json_serial),
                    json.dumps(row['votes'], default=LegislationScraperUtils.__json_serial),
                    row['site_topic'], row['topic'])

                    # print(f'Inserting <Row {row["state_url"]}>')
                    curs.execute(insert_legislator_query, tup)

                except Exception as e:
                    print(f'An exception occurred inserting <Row {row["state_url"]}>: {e}')

    def search_for_legislators(self, **kwargs):
        query_lst = []
        for k, v in kwargs.items():
            q = ''
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
            print(f'WARNING: More than one legislator found using {kwargs} search parameter! \
            Must use a more unique identifier!')
            return None
        if len(df) == 0:
            print(f'WARNING: No legislators found while searching {kwargs}!')
            return None

        return df

    def get_legislator_id(self, **kwargs):
        df = self.search_for_legislators(**kwargs)
        if df is not None:
            return df.iloc[0]['goverlytics_id']
        else:
            return None

    def get_legislator_id_list(self, search_list):
        legislator_ids = []
        for item in search_list:
            if not isinstance(item, dict):
                print(f'Unable to find legislator ID for: {item}. Search parameters must be a dictionary!')
            else:
                legislator_ids.append(self.get_legislator_id(**item))
        return legislator_ids

    def get_first_name_using_initial(self, first_name_initial, **kwargs):
        df = self.search_for_legislators(**kwargs)
        if df is not None:
            return df.loc[df["name_first"].str.startswith(first_name_initial)]["name_first"].values[0]
        return None
