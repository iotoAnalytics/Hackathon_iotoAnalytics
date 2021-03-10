import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
from datetime import date, datetime
import json
import sys
from database import CursorFromConnectionFromPool
import pandas as pd
from pandas.core.computation.ops import UndefinedVariableError
from typing import List
from dataclasses import dataclass, field
import numpy

"""
Contains utilities and data structures meant to help resolve common issues
that occur with data collection. These can be used with your legislation
date collectors.
"""

@dataclass
class LegislationRow:
    """
    Data structure for housing data about each piece of legislation.
    """
    goverlytics_id: int = None
    bill_state_id: str = ''
    bill_name: str = ''
    session: str = ''
    date_introduced: datetime = None
    state_url: str = ''
    url: str = ''
    chamber_origin: str = ''
    committees: List[dict] = field(default_factory=list)
    state_id: int = 0
    state: str = ''
    bill_type: str =  ''
    bill_title: str = ''
    current_status: str = ''
    principal_sponsor_id: int = None
    principal_sponsor: str = ''
    sponsors: List[str] = field(default_factory=list)
    sponsors_id: List[int] = field(default_factory=list)
    cosponsors: List[str] = field(default_factory=list)
    cosponsors_id: List[int] = field(default_factory=list)
    bill_text: str = ''
    bill_description: str =''
    bill_summary:str = ''
    actions: List[dict] = field(default_factory=list)
    votes: List[dict] = field(default_factory=list)
    site_topic: str = ''
    topic: str = ''


class USLegislationScraperUtils:
    """
    Utilities to help with collecting and storing legislation data.
    """
    def __init__(self, state_abbreviation, database_table_name, legislator_table_name):
        """
        The state_abbreviation, database_table_name, and legislator_table_name come from
        the config.cfg file and must be updated to work properly with your legislation
        data collector.
        """
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
                \nHas the legislator data been collected for this state yet? Has the config.cfg file been updated?\n{e}')

        self.states = pd.DataFrame(state_results)
        self.legislators = pd.DataFrame(legislator_results)

    
    def __json_serial(self, obj):
        """
        Serializes date/datetime object. This is used to convert date and datetime objects to
        a format that can be digested by the database.
        """
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        raise TypeError("Type %s not serializable" % type(obj))

    def __convert_to_int(self, value):
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
    
    def initialize_row(self) -> LegislationRow:
        '''
        Factory method for creating a legislation row. This gets sent back to the scrape() function
        which then gets filled in with values collected from the website.
        '''
        row = LegislationRow()
        
        try:
            row.state = self.state_abbreviation
            row.state_id = int(self.states.loc[self.states['abbreviation'] == self.state_abbreviation]['state_no'].values[0])
        except IndexError:
            sys.exit('An error occurred inserting state_id. Has the config file been updated?')
        except Exception as e:
            sys.exit(f'An error occurred involving the state_id and/or country_id: {e}')
        return row
    
    
    def insert_legislation_data_into_db(self, data : List[LegislationRow]) -> None:
        """
        Takes care of inserting legislation data into database.
        """
        if not isinstance(data, list):
            raise TypeError('Data being written to database must be a list of LegislationRows!')
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
                    ALTER TABLE {table} OWNER TO rds_ad;
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
                    tup = (row.goverlytics_id, row.bill_state_id, date_collected, row.bill_name,
                    row.session, row.date_introduced, row.state_url, row.url, row.chamber_origin,
                    json.dumps(row.committees, default=USLegislationScraperUtils.__json_serial),
                    row.state_id, row.state, row.bill_type, row.bill_title, row.current_status,
                    row.principal_sponsor_id, row.principal_sponsor, row.sponsors, row.sponsors_id,
                    row.cosponsors, row.cosponsors_id, row.bill_text, row.bill_description, row.bill_summary,
                    json.dumps(row.actions, default=USLegislationScraperUtils.__json_serial),
                    json.dumps(row.votes, default=USLegislationScraperUtils.__json_serial),
                    row.site_topic, row.topic)
                
                    curs.execute(insert_legislator_query, tup)
                except Exception as e:
                    print(f'An exception occurred inserting {row.goverlytics_id}:\n{e}')
    
    def search_for_legislators(self, **kwargs) -> pd.DataFrame:
        """
        Returns a dataframe containing search results based on kwargs.
        """


        query_lst = []
        for k, v in kwargs.items():
            q = ''
            v = self.__convert_to_int(v)
            if isinstance(v, int):
                q = f'{k}=={v}'
            elif isinstance(v, str):
                q = f'{k}=="{v}"'
            else:
                print(f'Unable to use {k}: {v} as search parameter. Must search by either a text or int column.')
                continue
            query_lst.append(q)
        query = ' & '.join(query_lst)
        if 'state_member_id' in kwargs:
            temp = '\"' + query.split('=')[-1] + '\"'
            query = query.split('=')[0:-1][0] + '==' + temp
        print(self.legislators)
        print(query)
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

    def get_legislator_id(self, **kwargs) -> int:
        """
        Method for getting the Goverlytics ID based on search parameters.
        """
        df = self.search_for_legislators(**kwargs)
        if df is not None:
            return self.__convert_to_int(df.iloc[0]['goverlytics_id'])
        else:
            return None
    def legislators_search_startswith(self, column_val_to_return, column_to_search, startswith, **kwargs):
        """
        Utilizes panda's .startswith method for finding information about legislators. Useful for finding
        things like the Goverlytics ID when given only the first initial and last name of a legislator.
        """
        df = self.legislators
        if kwargs:
            df = self.search_for_legislators(**kwargs)
        val = None
        startswith = self.__convert_to_int(startswith)
        if df is not None:
            try:
                val = df.loc[df[column_to_search].str.startswith(startswith)][column_val_to_return].values[0]
            except IndexError:
                print(f"Unable to find '{column_val_to_return}' using these search parameters: {column_to_search} : {startswith}")
            except KeyError:
                print(f"'{column_to_search}' is not a valid column name in the legislator data frame!")
        if isinstance(val, numpy.int64):
            val = int(val)
        return val
    
    
  
  

