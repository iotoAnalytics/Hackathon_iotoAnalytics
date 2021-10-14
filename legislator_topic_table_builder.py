import pandas as pd
from database import Database
from database import CursorFromConnectionFromPool
from psycopg2 import sql
from datetime import datetime, date
import numpy as np
import utils
from scraper_utils import LegislatorSponsorTopicRow
from scraper_utils import LegislatorScraperUtils

query = 'SELECT * FROM ca_fed_legislation'

with CursorFromConnectionFromPool() as cursor:
    cursor.execute(query)
    results = cursor.fetchall()
    
ca_fed_legislation_df = pd.DataFrame(data=results)

query = 'SELECT * FROM ca_fed_legislators'

with CursorFromConnectionFromPool() as cursor:
    cursor.execute(query)
    results = cursor.fetchall()
    
ca_fed_legislator_df = pd.DataFrame(data=results)

ca_topics = ca_fed_legislation_df.groupby(['principal_sponsor', 'topic']).size().to_frame('bill_counts')
ca_topics = ca_topics.reset_index()
ca_topics['name_full'] = ca_topics['principal_sponsor']
ca_topics = ca_topics.drop('principal_sponsor', 1)
ca_topics = ca_topics.pivot(index='name_full', columns="topic", values="bill_counts").fillna(0)
ca_topics = ca_topics.astype(int)
ca_legislator_build_df = ca_fed_legislator_df[['country','goverlytics_id', 'name_full','name_first', 'name_last', 'name_middle', 'name_suffix', 'party']]
ca_legislator_build_df = ca_legislator_build_df.set_index('name_full')
ca_legislator_bills_sponsored_by_topic = ca_legislator_build_df.join(ca_topics).fillna(0)
ca_legislator_bills_sponsored_by_topic = ca_legislator_bills_sponsored_by_topic.reset_index()
df = ca_legislator_bills_sponsored_by_topic.rename(columns={"civil rights": "civil_rights", "domestic commerce": "domestic_commerce","education": "edu_cation" ,"foreign trade": "foreign_trade",
    "government operations": "government_operations", "international affairs": "international_affairs", "law and crime": "law_and_crime", "social welfare": "social_welfare"})
df.drop('', 1)

scraper_utils = LegislatorScraperUtils('ca', 'ca_legislator_bills_sponsored_by_topic', LegislatorSponsorTopicRow)

ca_fed_lst = []
for i in range(0,len(df)):
    row = scraper_utils.initialize_row()
    row.goverlytics_id = int(df['goverlytics_id'].iloc[i])
    row.name_full = str(df['name_full'].iloc[i])
    row.name_last = str(df['name_last'].iloc[i])
    row.name_first = str(df['name_first'].iloc[i])
    row.name_middle = str(df['name_middle'].iloc[i])
    row.name_suffix = str(df['name_suffix'].iloc[i])
    row.party = str(df['party'].iloc[i])
    row.country = str(df['country'].iloc[i])
    row.agriculture = int(df['agriculture'].iloc[i])
    row.civil_rights = int(df['civil_rights'].iloc[i])
    row.defense = int(df['defense'].iloc[i])
    row.domestic_commerce = int(df['domestic_commerce'].iloc[i])
    row.edu_cation = int(df['edu_cation'].iloc[i])
    row.energy = int(df['energy'].iloc[i])
    row.environment = int(df['environment'].iloc[i])
    row.foreign_trade = int(df['foreign_trade'].iloc[i])
    row.government_operations = int(df['government_operations'].iloc[i])
    row.health = int(df['health'].iloc[i])
    row.immigration = int(df['immigration'].iloc[i])
    row.international_affairs = int(df['international_affairs'].iloc[i])
    row.labor = int(df['labor'].iloc[i])
    row.law_and_crime = int(df['law_and_crime'].iloc[i])
    row.macroeconomics = int(df['macroeconomics'].iloc[i])
    row.social_welfare = int(df['social_welfare'].iloc[i])
    row.technology = int(df['technology'].iloc[i])
    row.transportation = int(df['transportation'].iloc[i])

    ca_fed_lst.append(row)

    scraper_utils.write_data(ca_fed_lst, 'ca_fed_legislator_sponsor_topics')