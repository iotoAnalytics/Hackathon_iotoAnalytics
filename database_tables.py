import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
import sys
import configparser

class DatabaseTables:
    def __init__(self):

        # TODO modify this so its similar to the GetConnectionFromPool class in the old database.py file
        configParser = configparser.RawConfigParser()
        configParser.read('config.cfg')
        
        self.db_user = str(configParser.get('databaseConfig', 'db_user'))
        self.db_pass = str(configParser.get('databaseConfig', 'db_pass'))
        self.db_host = str(configParser.get('databaseConfig', 'db_host'))
        self.db_port = str(configParser.get('databaseConfig', 'db_port'))
        self.db_name = str(configParser.get('databaseConfig', 'db_name'))

        conn = psycopg2.connect(
            dbname=self.db_name,
            user=self.db_user,
            password=self.db_pass,
            host=self.db_host,
            port=self.db_port
        )

        with conn:
            with conn.cursor(cursor_factory=RealDictCursor) as curs:
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

        conn.close()

        self.parties = pd.DataFrame(parties_results)
        self.countries = pd.DataFrame(countries_results)
        self.states = pd.DataFrame(state_results)