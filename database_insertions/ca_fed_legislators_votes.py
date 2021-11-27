'''
Takes info from ca_fed_legislation and ca_fed_legislation_topic_prob tables, stores 
vote data into separate table for custom webapp endpoint to use.
'''
import sys
import os
from pathlib import Path

from psycopg2.extras import RealDictCursor
from psycopg2 import sql
import pandas as pd
from tqdm import tqdm

p = Path(os.path.abspath(__file__)).parents[1]

sys.path.insert(0, str(p))

import utils
import database as db

with db.CursorFromConnectionFromPool() as cur:
    vote_data = []
    try:
        table_1, table_2 = 'ca_fed_legislation', 'ca_fed_legislation_topic_probs'
        joined_table = f'{table_1} LEFT JOIN {table_2} ON {table_1}.goverlytics_id = {table_2}.goverlytics_id'
        query_string = sql.SQL(""" 
            SELECT ca_fed_legislation.goverlytics_id, 
                   ca_fed_legislation.bill_name, 
                   ca_fed_legislation.current_status, 
                   ca_fed_legislation.session,
                   ca_fed_legislation.votes, ca_fed_legislation_topic_probs.topic 
            FROM {table} WHERE ca_fed_legislation.votes != '[]'
            """).format(
                table=sql.SQL(joined_table)
            )
        cur.execute(query_string)
        data = cur.fetchall()

        for item in data:
            '''Loop through each vote element; vote element is NOT the list of legislator votes, it includes fields like total, nay, yay, etc.'''
            for i in range(len(item['votes'])):
                temp_vote_obj = item['votes'][i]
                '''loop through all the votes within the vote element'''
                for vote in temp_vote_obj['votes']:
                    vote_data += [{
                            'name': vote['legislator'],
                            'vote_text': vote['vote_text'],
                            'legislator_goverlytics_id': vote['goverlytics_id'],
                            'vote_session_date': temp_vote_obj['date'],
                            'session': item['session'],
                            'vote_description': temp_vote_obj['description'],
                            'legislation_goverlytics_id': item['goverlytics_id'],   
                            'bill_name': item['bill_name'],
                            'current_status': item['current_status'],
                            'passed': temp_vote_obj['passed'],
                            'topic': item['topic']
                        }]

        print('\ndone mutating data\n')


    except Exception as e:
        print(f'ERROR: \n {e} \n')
        cur.connection.rollback()

    make_table_name = 'ca_fed_legislators_votes_webapp'
    create_table_query = sql.SQL("""
        CREATE TABLE IF NOT EXISTS {table} (
            legislator_goverlytics_id bigint,
            name text,
            vote_text text,
            vote_session_date text,
            vote_description text,
            session text,
            legislation_goverlytics_id text,
            bill_name text,
            current_status text,
            passed int,
            topic text,
        UNIQUE (legislator_goverlytics_id, legislation_goverlytics_id, vote_session_date, vote_description)
        );
        ALTER TABLE {table} OWNER TO rds_ad;
    """).format(table=sql.Identifier(make_table_name))
    cur.execute(create_table_query)
    cur.connection.commit()
    insert_data_query = sql.SQL("""
        INSERT INTO {table}
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (legislator_goverlytics_id, legislation_goverlytics_id, vote_session_date, vote_description) DO UPDATE SET 
            name = excluded.name,
            vote_text = excluded.vote_text,
            bill_name = excluded.bill_name,
            session = excluded.session,
            current_status = excluded.current_status,
            passed = excluded.passed,
            topic = excluded.topic
    """).format(table=sql.Identifier(make_table_name))
    
    for row in tqdm(vote_data):
        if isinstance(row, dict):
            row = utils.DotDict(row)
        if row.legislator_goverlytics_id:
            tup = (row.legislator_goverlytics_id, row.name, row.vote_text, row.vote_session_date,
                    row.vote_description, row.session, row.legislation_goverlytics_id, row.bill_name, row.current_status,
                    row.passed, row.topic)
            try:
                cur.execute(insert_data_query, tup)
            except Exception as e:
                print(f'Exception occured inserting {row.legislator_goverlytics_id}:\n{e}')
                cur.connection.rollback()


print('\nDone!')