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
import requests
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser
from bs4 import BeautifulSoup
import time
import random
import boto3
import tempfile
import torch
from transformers import BertTokenizer
from torch.utils.data import TensorDataset
import numpy as np
from transformers import BertForSequenceClassification
from torch.utils.data import DataLoader, SequentialSampler

# region Base Scraper Utils


class LegislationScraperUtils:
    """
    Base class containing common methods and attributes used by all
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

        self.country = self.countries.loc[self.countries['abbreviation']
                                          == country]['country'].values[0]
        self.country_id = int(
            self.countries.loc[self.countries['abbreviation'] == country]['id'].values[0])

        self.row_type = row_type
        self.request_headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko)Chrome/79.0.3945.88 Safari/537.36; IOTO International Inc./enquiries@ioto.ca'
        }

    def request(self, url, headers=None):
        """Send request using default scraper util header. Header can be overridden by passing in your own."""
        header = self.request_headers
        if headers:
            header = headers
        return requests.get(url, headers=header)

    def add_topics(self, df):
        """
          Pulls a model from an S3 bucket as a temporary file
          Uses the model to classify the bill text
          Returns the dataframe with the topics filled in
          If you want a different model just upload it to the S3
          and change the code in this function to implement that model instead
          """
        print('Loading model...')
        s3 = boto3.client('s3')

        with tempfile.NamedTemporaryFile(mode='w+b', delete=False) as f:
            s3.download_fileobj('bill-topic-classifier-sample', 'bert_data_dem4.pt', f)
            mlmodel = f.name
            print(mlmodel)

        df = pd.DataFrame(df)

        possible_labels = ['government operations', 'health', 'education', 'macroeconomics', 'informal',
                           'international affairs', 'civil rights', 'social welfare', 'public lands',
                           'defense', 'domestic commerce', 'law and crime', 'culture', 'transportation',
                           'environment', 'labor', 'housing', 'technology', 'immigration', 'energy',
                           'agriculture', 'foreign trade']

        label_dict = {'government operations': 0, 'health': 1, 'education': 2, 'macroeconomics': 3, 'informal': 4,
                      'international affairs': 5, 'civil rights': 6, 'social welfare': 7, 'public lands': 8,
                      'defense': 9,
                      'domestic commerce': 10, 'law and crime': 11, 'culture': 12, 'transportation': 13,
                      'environment': 14,
                      'labor': 15, 'housing': 16, 'technology': 17, 'immigration': 18, 'energy': 19, 'agriculture': 20,
                      'foreign trade': 21}
        for index, possible_label in enumerate(possible_labels):
            label_dict[possible_label] = index

        # default initial value
        df = df.assign(topic="informal")
        l = df.topic.replace(label_dict)

        df = df.assign(label=l)

        # print(df)
        eval_texts = df.bill_text.values

        tokenizer = BertTokenizer.from_pretrained('bert-base-uncased', do_lower_case=True)

        encoded_data_val = tokenizer.batch_encode_plus(
            eval_texts,
            add_special_tokens=True,
            return_attention_mask=True,
            padding=True,
            truncation=True,
            max_length=256,
            return_tensors='pt'
        )

        input_ids_val = encoded_data_val['input_ids']
        attention_masks_val = encoded_data_val['attention_mask']
        labels_val = torch.tensor(df.label.values)

        dataset_val = TensorDataset(input_ids_val, attention_masks_val, labels_val)

        dataloader_validation = DataLoader(dataset_val,
                                           sampler=SequentialSampler(dataset_val),
                                           batch_size=1,
                                           )

        device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
        model = BertForSequenceClassification.from_pretrained("bert-base-uncased",
                                                              num_labels=len(label_dict),
                                                              output_attentions=False,
                                                              output_hidden_states=False)
        model.to(device)

        model.load_state_dict(torch.load(mlmodel, map_location=torch.device('cpu')))

        model.eval()

        loss_val_total = 0
        predictions, true_vals = [], []

        for batch in dataloader_validation:
            batch = tuple(b.to(device) for b in batch)

            inputs = {'input_ids': batch[0],
                      'attention_mask': batch[1],
                      'labels': batch[2],
                      }

            with torch.no_grad():
                outputs = model(**inputs)

            # loss = outputs[0]
            logits = outputs[1]
            # loss_val_total += loss.item()

            logits = logits.detach().cpu().numpy()
            # label_ids = inputs['labels'].cpu().numpy()
            predictions.append(logits)
            # true_vals.append(label_ids)

        # loss_val_avg = loss_val_total / len(dataloader_val)

        predictions = np.concatenate(predictions, axis=0)
        # predictions = evaluate(dataloader_validation, mlmodel)
        i = 0
        labels = []
        result_df = []
        processed_text = []
        # print(predictions)
        for pred in predictions:
            print('text:')
            txt = eval_texts[i]
            # txt = df.loc[['text'], [i]]
            print(txt)
            print("predicted label:")
            pred_label = (possible_labels[np.argmax(pred)])
            print(pred_label)

            i = i + 1
            entry = {'bill_text': txt,
                     'topic': pred_label}
            result_df.append(entry)
            processed_text.append(pred_label)

        # accuracy_per_class(predictions, true_vals)
        result_df = pd.DataFrame(result_df)
        return result_df

    def get_crawl_delay(self, url, user_agent=None):
        """Return crawl delay for a given URL based on robots.txt file. If a robots.txt file cannot be found or parsed, a default value will be returned."""
        user_agent = user_agent if user_agent else self.request_headers.get(
            'User-Agent')
        o = urlparse(url)
        scheme = 'https' if o.scheme == '' else o.scheme
        robots_url = f'{scheme}://{o.netloc}/robots.txt'
        rp = RobotFileParser()
        rp.set_url(robots_url)
        rp.read()
        crawl_delay = rp.crawl_delay(user_agent if user_agent else '*')
        if crawl_delay:
            return crawl_delay
        return 2

    def crawl_delay(self, min_seconds):
        """Add delay. Should be called after making request to website so as to not overburden web server."""
        time.sleep(random.uniform(1, 1.1) * min_seconds)

    def get_attribute(self, table_name, column_to_search, value_to_search, attribute_to_return='id'):
        """
        Essentially a dataframe search function. Specify the table you would like to search,
        the column you want to search, the value you want to use as a search key, and the 
        attribute to return. Tables come from the database and depend on the scraper type.
        For example, if you have a US state scraper for North Carolina, searching 'legislator'
        will search the US state legislator table for North Carolina. Note that this function
        only returns the first value found.
        Args:
            table_name: The table to search. Must be either country, legislator, division,
                or party, passed in as a string.
            column_to_search: The column you want to search. If you wanted to find an attribute
                by last name, for example, you would specify name_last
            value_to_search: The search key. For example, you would enter 'Smith' if you wanted
                to search for legislators with that last name.
            attribute_to_return: The attribute you wanted to return from the search. You could
                enter goverlytics_id if you wanted return the goverlytics_id when searching
                for the last name Smith.
        Returns:
            Single value based on search parameters.
        """
        accepted_tables = ['country', 'legislator', 'division', 'party']
        if table_name not in accepted_tables:
            raise Exception(
                f'Error: table must be one of the following: {accepted_tables}')

        if table_name == 'country':
            df = self.countries
        if table_name == 'legislator':
            df = self.legislators
        if table_name == 'division':
            df = self.divisions
        if table_name == 'party':
            df = self.parties

        val = df.loc[df[column_to_search] ==
                     value_to_search][attribute_to_return]
        if val.any():
            return self._convert_value_to_column_type(column_to_search, val.values[0])
        else:
            raise Exception(
                f'Could not locate value using following search parameters: table_name={table_name}, column_to_search={column_to_search}, value_to_search={value_to_search}, attribute_to_return={attribute_to_return}')

    def _convert_to_int(self, value):
        """
        Used to convert values into int. Functions like df.loc might return
        a numpy.int64 which is incompatible with the database, so this function must
        be used.
        Args:
            value: The value to convert to an int.
        Return:
            Value converted to an int.
        """
        try:
            value = int(value)
        except ValueError:
            pass
        return value

    def _convert_value_to_column_type(self, column, value):
        """
        Used to convert columns to the appropriate datatype. Some columns such as source_id
        must remain as a string, however they may inadvertently be converted to
        something like an int.
        Args:
            column: The column that the value is being placed into
            value: The value to convert
        """
        str_columns = {'source_id'}

        if column in str_columns:
            return str(value)
        else:
            return self._convert_to_int(value)

    def initialize_row(self):
        '''
        Used to create a row with the default values in place.
        Returns:
            row filled with default values.
        '''
        row = copy.deepcopy(self.row_type)
        row.country = self.country
        row.country_id = self.country_id
        return row

    def search_for_legislators(self, **kwargs) -> pd.DataFrame:
        """
        Returns a dataframe containing search results based on kwargs.
        Args:
            kwags: key-value pairs containing parameters to use for searching
        Returns:
            Dataframe containing search results
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
                print(
                    f'Unable to use {k}: {v} as search parameter. Must search by either a text or int column.')
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
            print(
                f'WARNING: More than one legislator found using {kwargs} search parameter.')
        if len(df) == 0:
            print(f'WARNING: No legislators found while searching {kwargs}!')
            return None

        return df

    def get_legislator_id(self, **kwargs) -> int:
        """
        Method for getting the Goverlytics ID based on search parameters. Note that
        this returns the first value found.
        Args:
            kwags: key-value pairs containing parameters to use for searching
        Returns:
            goverlytics_id based on search parameters.
        """
        df = self.search_for_legislators(**kwargs)
        if df is not None:
            return self._convert_to_int(df.iloc[0]['goverlytics_id'])
        else:
            return None

    def legislators_search_startswith(self, column_val_to_return, column_to_search, startswith, **kwargs):
        """
        Utilizes panda's .startswith method for finding information about legislators.
        Useful for finding things like the Goverlytics ID when given only the first
        initial and last name of a legislator.
        Args:
            column_val_to_return: The value to return. For example, the goverlytics_id.
            column_to_search: The column you would like to search using startswith. For example, name_first.
                Must be a valid column in the legislator database table.
            startswith: The value to search using the startswith function. For example, if
                searching "A. Smith", you would pass in "A".
            kwargs: Additional parameters used for searching. Must be included.
        Returns:
            Val found based on search parameters
        """
        val = None

        if not kwargs:
            print('Must include kwargs when using legislators_search_startswith!')
            return val

        df = self.search_for_legislators(**kwargs)

        startswith = self._convert_value_to_column_type(
            column_to_search, startswith)

        if df is not None:
            try:
                val = df.loc[df[column_to_search].str.startswith(
                    startswith)][column_val_to_return].values[0]
            except IndexError:
                print(
                    f"Unable to find '{column_val_to_return}' using these search parameters: {column_to_search} : {startswith}")
            except KeyError:
                print(
                    f"'{column_to_search}' is not a valid column name in the legislator data frame!")
            except AttributeError:
                print(
                    'Can only search columns of type str/text when using legislators_search_startswith!')
            except Exception as e:
                print('An exception occurred: {e}')
        if isinstance(val, numpy.int64):
            val = int(val)
        return val
# endregion

# region US Scraper Utils


class USFedLegislationScraperUtils(LegislationScraperUtils):
    """
    Scraper used for collecting US Federal legislation data.
    """

    def __init__(self, database_table_name='us_fed_legislation', legislator_table_name='us_fed_legislators'):
        super().__init__('us', database_table_name,
                         legislator_table_name, USLegislationRow())

    def insert_legislation_data_into_db(self, data, database_table=None) -> None:
        """
        Takes care of inserting legislation data into database. Must be a list of Row objects or dictionaries.
        """
        if not isinstance(data, list):
            raise TypeError(
                'Data being written to database must be a list of Rows or dictionaries!')

        table = self.database_table_name
        if database_table:
            table = database_table

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
                    """).format(table=sql.Identifier(table))

                cur.execute(create_table_query)
                cur.connection.commit()

            except Exception as e:
                print(
                    f'An exception occurred creating {table}:\n{e}')

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
                """).format(table=sql.Identifier(table))

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
                    print(
                        f'An exception occurred inserting {row.goverlytics_id}:\n{e}')


class USStateLegislationScraperUtils(USFedLegislationScraperUtils):
    """
    Utilities for collecting US state legislation.
    """

    def __init__(self, state_abbreviation, database_table_name='us_state_legislation',
                 legislator_table_name='us_state_legislators'):
        super().__init__(database_table_name, legislator_table_name)
        self.state = state_abbreviation
        self.state_id = int(
            self.divisions.loc[self.divisions['abbreviation'] == state_abbreviation]['id'].values[0])

    def initialize_row(self):
        """Creates a Row object with default values in place."""
        row = super().initialize_row()
        row.state = self.state
        row.state_id = self.state_id
        return row
# endregion

# region Canadian Scraper Utils


class CAFedLegislationScraperUtils(LegislationScraperUtils):
    def __init__(self, database_table_name='ca_fed_legislation', legislator_table_name='ca_fed_legislators', row_type=CAFedLegislationRow()):
        super().__init__('ca', database_table_name, legislator_table_name, row_type)

    def get_region(self, prov_terr_abbrev):
        """Returns the province/territory region based on a given province/territory abbreviation."""
        return self.get_attribute('division', 'abbreviation', prov_terr_abbrev, 'region')

    def get_prov_terr_abbrev(self, prov_terr):
        """Returns province/territory abbreviation based on the full name of a given province or territory"""
        return self.get_attribute('division', 'division', prov_terr, 'abbreviation')

    def insert_legislation_data_into_db(self, data, database_table=None) -> None:
        """
        Takes care of inserting legislation data into database. Data must be either a list or Row objects or dictionaries.
        """
        if not isinstance(data, list):
            raise TypeError(
                'Data being written to database must be a list of Rows or dictionaries!')

        table = self.database_table_name
        if database_table:
            table = database_table

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
                        region text,
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
                    """).format(table=table)

                cur.execute(create_table_query)
                cur.connection.commit()

            except Exception as e:
                print(
                    f'An exception occurred creating {table}:\n{e}')

            insert_legislator_query = sql.SQL("""
                INSERT INTO {table}
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
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
                    region = excluded.region,
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
                """).format(table=sql.Identifier(table))

            date_collected = datetime.now()

            for row in data:

                if isinstance(row, dict):
                    row = utils.DotDict(row)

                tup = (row.goverlytics_id, row.source_id, date_collected, row.bill_name,
                       row.session, row.date_introduced, row.source_url, row.chamber_origin,
                       json.dumps(row.committees, default=utils.json_serial),
                       row.province_territory_id, row.province_territory, row.region, row.bill_type, row.bill_title,
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
                    print(
                        f'An exception occurred inserting {row.goverlytics_id}:\n{e}')


class CAProvinceTerrLegislationScraperUtils(CAFedLegislationScraperUtils):
    """Scraper for helping collect Canadian provincial or territorial legislation."""

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

    def insert_legislation_data_into_db(self, data, database_table=None) -> None:
        """
        Takes care of inserting legislation data into database. Must be a list of Row objects or dictionaries.
        """
        if not isinstance(data, list):
            raise TypeError(
                'Data being written to database must be a list of Rows or dictionaries!')

        table = self.database_table_name
        if database_table:
            table = database_table

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
                    """).format(table=sql.Identifier(table))

                cur.execute(create_table_query)
                cur.connection.commit()

            except Exception as e:
                print(
                    f'An exception occurred creating {table}:\n{e}')

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
                """).format(table=sql.Identifier(table))

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
                    print(
                        f'An exception occurred inserting {row.goverlytics_id}:\n{e}')
# endregion
