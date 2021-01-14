import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
import sys
import configparser
from database import CursorFromConnectionFromPool

class DatabaseTables:
    def __init__(self):
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
