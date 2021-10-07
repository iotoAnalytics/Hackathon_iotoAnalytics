import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
# import datetime
from datetime import date, datetime
import json
import numpy as np
import torch
import boto3
import tempfile
from transformers import BertTokenizer
from torch.utils.data import TensorDataset
from transformers import BertForSequenceClassification
from torch.utils.data import DataLoader, SequentialSampler
import functools

import sys
import pandas as pd
from genderComputer.genderComputer import GenderComputer # Must download repo on iotoAnalytics/genderComputer
from database import Database, CursorFromConnectionFromPool, Persistence
from dataclasses import dataclass, field
from typing import List
from rows import *
import copy
# import atexit
import utils
from urllib.request import urlopen as uReq
import re
import requests
import unidecode
from bs4 import BeautifulSoup as soup
from nameparser import HumanName
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser
from bs4 import BeautifulSoup
import time
import random
from collections import namedtuple
import exceptions
from pandas.core.computation.ops import UndefinedVariableError
import numpy
import pdfplumber
import io

"""
Contains utilities and data structures meant to help resolve common issues
that occur with data collection. These can be used with your legislator
date collectors.
"""


# region Base Scraper Utils
##########################################
# BASE CLASSES
##########################################

class ScraperUtils:
    """Bases class containing methods universal to all ScraperUtils"""

    class Robots:
        """Inner class for keeping track of robots.txt for a given URL"""

        def __init__(self, url, user_agent):
            self.url = url
            self.user_agent = user_agent

            self.robots_url = url + '/robots.txt'
            self.rp = RobotFileParser(self.robots_url)
            self.rp.read()

            self.crawl_delay = self.get_crawl_delay()
            self.request_rate = self.get_request_rate()

        def get_crawl_delay(self, user_agent=None):
            """Return crawl delay for a given URL based on robots.txt file. If a robots.txt file cannot be found or parsed, a default value will be returned."""
            ua = user_agent if user_agent else self.user_agent
            return self.rp.crawl_delay(ua)


        def get_request_rate(self, user_agent=None):
            """Return crawl delay for a given URL based on robots.txt file. If a robots.txt file cannot be found or parsed, a default value will be returned."""
            ua = user_agent if user_agent else self.user_agent
            return self.rp.request_rate(ua)


        def can_fetch(self, url, user_agent=None):
            """Determine whether data from a given URL can be collected."""
            ua = user_agent if user_agent else self.user_agent
            return self.rp.can_fetch(ua, url)

    def __init__(self, country, database_table_name, row_type):

        # Database.initialise()
        # atexit.register(Database.close_all_connections)

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
                sys.exit(
                    f'An exception occurred retrieving tables from database:\n{e}')

        self.countries = pd.DataFrame(countries_results)
        self.parties = pd.DataFrame(parties_results)
        self.divisions = pd.DataFrame(division_results)

        self.country = self.countries.loc[self.countries['abbreviation']
                                          == country]['country'].values[0]
        self.country_id = int(
            self.countries.loc[self.countries['abbreviation'] == country]['id'].values[0])
        self.database_table_name = database_table_name
        self.row_type = row_type
        self._request_headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko)Chrome/79.0.3945.88 Safari/537.36; IOTO International Inc./enquiries@ioto.ca'
        }
        self._robots = {}

    def get_base_url(self, url):
        """Parse the base URL for a given url."""
        o = urlparse(url)
        scheme = 'https' if o.scheme == '' else o.scheme
        return f'{scheme}://{o.netloc}'

    def add_robot(self, url, user_agent=None):
        """Adds the robots.txt information for a given URL."""
        ua = user_agent if user_agent else self._request_headers.get(
            'User-Agent', '*')
        bu = self.get_base_url(url)
        self._robots[bu] = self.Robots(bu, ua)

    def _auto_add_robot(self, base_url, auto_add_enabled):
        """Called for methods that have an auto_add_robot parameter."""
        if not self._robots.get(base_url) and auto_add_enabled:
            self.add_robot(base_url)

    def request(self, url, **kwargs):
        """More polite version of the requests.get function.
        Valid kwargs:
            headers : dict
                Headers to use for the request. Uses ScraperUtils _request_headers by default
            auto_add_robot : bool
                Automatically configure robots.txt for the given url. True by default.
        Raises:
            RobotsBlockedException
                Raised if website blocks web scrapers."""
        headers = kwargs.get('headers', self._request_headers)
        auto_add_robot = kwargs.get('auto_add_robot', True)

        bu = self.get_base_url(url)

        self._auto_add_robot(bu, auto_add_robot)

        r = self._robots.get(bu)
        if not r:
            raise exceptions.NoRobotsConfiguredException(
                self.request, url, list(self._robots.keys()))
        ua = headers.get('User-Agent', '*')

        can_fetch = r.can_fetch(ua, url)
        if can_fetch:
            return requests.get(url, headers=headers)
        raise exceptions.RobotsBlockedException(url, ua)

    def get_crawl_delay(self, url, **kwargs):
        """
        Return crawl delay for a given URL based on robots.txt file. If a robots.txt file cannot be found or the Crawl-delay or Request-rate cannot be parsed, a default value will be returned.
        kwargs:
            auto_add_robot : bool
                Automatically configure robots.txt for the given url. True by default.
            user_agent : str
                User-Agent for the given request. Uses default User-Agent if none is provided.
        """
        DEFAULT_CRAWL_RATE = 2
        bu = self.get_base_url(url)

        auto_add_robot = kwargs.get('auto_add_robot', True)
        user_agent = kwargs.get('user_agent')

        self._auto_add_robot(bu, auto_add_robot)

        r = self._robots.get(bu)
        if not r:
            raise exceptions.NoRobotsConfiguredException(
                self.get_crawl_delay, url, list(self._robots.keys()))

        crawl_delay = r.crawl_delay
        request_rate = r.request_rate
        if user_agent:
            crawl_delay = r.get_crawl_delay(user_agent)
            request_rate = r.get_request_rate(user_agent)

        if crawl_delay and request_rate:
            return min(crawl_delay, request_rate.seconds / request_rate.requests)
        elif crawl_delay:
            return crawl_delay
        elif request_rate:
            return request_rate
        return DEFAULT_CRAWL_RATE

    def crawl_delay(self, minimum_seconds):
        """Add delay. Should be called after making request to website so as to not overburden web server."""
        time.sleep(random.uniform(1, 1.1) * minimum_seconds)

    def initialize_row(self):
        """Instantiates a Row object filled with default values."""
        row = copy.deepcopy(self.row_type)
        row.country_id = self.country_id
        row.country = self.country
        return row

    class Timer:
        """A timing decorator to test the speed of your functions. Call @scraper_utils.Timer() above your function to
        time your function."""
        def __call__(self, func):
            @functools.wraps(func)
            def wrapper_timer(*args, **kwargs):
                start = time.perf_counter()
                value = func(*args, **kwargs)
                end = time.perf_counter()
                run_time = end - start
                print(f'Finished {func.__name__} in {run_time:.4f} secs')
                return value

            return wrapper_timer
    

class LegislatorScraperUtils(ScraperUtils):
    """Base scraper class. Contains methods common to all legislator scrapers."""

    def __init__(self, country, database_table_name, row_type):
        super().__init__(country, database_table_name, row_type)

    def get_attribute(self, table_name, column_to_search, value, attribute='id'):
        """Returns a given attribute from the specified table. This is a helper used by other functions but it’s available if you require attributes from one of the valid tables. Valid table names include country, party, and division."""
        accepted_tables = ['country', 'party', 'division']
        if table_name not in accepted_tables:
            raise Exception(
                f'Error: table must be one of the following: {accepted_tables}')

        if table_name == 'country':
            df = self.countries
        if table_name == 'party':
            df = self.parties
        if table_name == 'division':
            df = self.divisions

        val = df.loc[df[column_to_search] == value][attribute].values[0]
        try:
            return int(val)
        except Exception:
            return val

    def scrape_wiki_bio(self, wiki_link):
        """
        Used for getting missing legislator fields from their wikipedia bios.
        Useful for getting things like a legislator's birthday or education.
        Takes in a link to the personal wikipedia page of the legislator
        """
        # get available birthday
        wiki_url = wiki_link
        try:
            uClient = uReq(wiki_link)
            page_html = uClient.read()
            uClient.close()
            # # html parsing
            page_soup = soup(page_html, "html.parser")

            # # #grabs each product
            reps = page_soup.find("div", {"class": "mw-parser-output"})
            repBirth = reps.find("span", {"class": "bday"}).text
            # convert to proper data format
            b = datetime.strptime(repBirth, "%Y-%m-%d").date()

            birthday = b

        except:
            # couldn't find birthday in side box
            birthday = None
        # get birthday another way
        try:
            uClient = uReq(wiki_link)
            page_html = uClient.read()
            uClient.close()
            # # html parsing
            page_soup = soup(page_html, "html.parser")

            reps = page_soup.find("div", {"class": "mw-parser-output"})

            left_column_tags = reps.findAll()
            lefttag = left_column_tags[0]
            for lefttag in left_column_tags:
                if lefttag.text == "Born":
                    index = left_column_tags.index(lefttag) + 1
                    born = left_column_tags[index].text

                    if born != "Born":
                        b = datetime.strptime(born, "%Y-%m-%d").date()

                        birthday = b
                        # print(b)

        except Exception as ex:
            # birthday not available
            pass

        # get years_active, based off of "assumed office" year
        years_active = []
        year_started = ""
        try:
            uClient = uReq(wiki_link)
            page_html = uClient.read()
            uClient.close()
            # # html parsing
            page_soup = soup(page_html, "html.parser")

            table = page_soup.find("table", {"class": "infobox vcard"})

            tds = table.findAll("td", {"colspan": "2"})
            td = tds[0]

            for td in tds:
                asof = (td.find("span", {"class": "nowrap"}))
                if asof != None:
                    if (asof.b.text) == "Assumed office":

                        asofbr = td.find("br")

                        year_started = str(asofbr.nextSibling)

                        year_started = year_started.split('[')[0]
                        if "," in year_started:
                            year_started = year_started.split(',')[1]
                        year_started = (year_started.replace(" ", ""))
                        year_started = re.sub('[^0-9]', '', year_started)
                        if year_started.startswith("12"):
                            year_started = year_started.substring(1)
                    else:
                        pass

        except Exception as ex:

            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.args)

        if year_started != "":
            # create a list of years from that year to current year
            # use current year + 1 since it doesn't include endpoint
            # will need to be updated each year
            years_active = list(range(int(year_started), 2022))

        else:
            years_active = []

        # get education
        education = []
        # possible education levels that might show up, feel free to add more
        lvls = ["MA", "BA", "JD", "BSc", "MIA", "PhD",
                "DDS", "MS", "BS", "MBA", "MS", "MD"]

        try:
            uClient = uReq(wiki_link)
            page_html = uClient.read()
            uClient.close()
            # # html parsing
            page_soup = soup(page_html, "html.parser")
            # # #grabs each product
            reps = page_soup.find("div", {"class": "mw-parser-output"})

            left_column_tags = reps.findAll()
            lefttag = left_column_tags[0]
            for lefttag in left_column_tags:
                if lefttag.text == "Alma mater" or lefttag.text == "Education":
                    index = left_column_tags.index(lefttag) + 1
                    next = left_column_tags[index]
                    alines = next.findAll()
                    for aline in alines:
                        if "University" in aline.text or "College" in aline.text or "School" in aline.text:
                            school = aline.text
                            # this is most likely a school
                            level = ""
                            try:
                                lineIndex = alines.index(aline) + 1
                                nextLine = alines[lineIndex].text
                                if re.sub('[^a-zA-Z]+', "", nextLine) in lvls:
                                    level = nextLine
                            except:
                                pass

                        edinfo = {'level': level,
                                  'field': "", 'school': school}

                        if edinfo not in education:
                            education.append(edinfo)

        except Exception as ex:

            template = "An exception of type {0} occurred. Arguments:\n{1!r}"

            message = template.format(type(ex).__name__, ex.args)

        # get full name
        # this is necessary as it will be use to merge the resulting dataframe with the rest of your data
        try:
            uClient = uReq(wiki_link)
            page_html = uClient.read()
            uClient.close()
            # # html parsing
            page_soup = soup(page_html, "html.parser")

            # # #grabs each product
            head = page_soup.find("h1", {"id": "firstHeading"})
            name = head.text
            name = name.split("(")[0].strip()

        except:
            name = ""
        name = unidecode.unidecode(name)

        hN = HumanName(name)

        # get occupation
        occupation = []

        try:
            uClient = uReq(wiki_link)
            page_html = uClient.read()
            uClient.close()
            # # html parsing
            page_soup = soup(page_html, "html.parser")

            reps = page_soup.find("div", {"class": "mw-parser-output"})

            left_column_tags = reps.findAll()
            lefttag = left_column_tags[0]
            for lefttag in left_column_tags:
                if lefttag.text == "Occupation":
                    index = left_column_tags.index(lefttag) + 1
                    occ = left_column_tags[index].text
                    if occ != "Occupation":
                        occupation.append(occ)

        except:
            # no occupation available
            pass

        most_recent_term_id = ""

        try:
            most_recent_term_id = (years_active[len(years_active) - 1])

        except:
            pass

        info = {'name_first': hN.first, 'name_last': hN.last, 'birthday': birthday,
                'education': education, 'occupation': occupation, 'years_active': years_active,
                'most_recent_term_id': str(most_recent_term_id), 'wiki_url': wiki_url}

        """
            returns dictionary with the following fields if available
            choose the ones that you weren't able to find from the gov website 
            
            merge the resulting data with the data you scraped from the gov website, on name_first and name_last
            
            Note: not all legislators will have a wikipedia page.  
            This may cause some fields to be 'NaN' after the merge.
            Replace all NaN fields with "None" or something similar or you may get a type error when uploading to db
      
        """
        return info

    def get_legislator_gender(self, name_first: str, name_last: str, biography=None) -> str or None:
        """
        Used for getting the gender of legislator. 
        This should be used when gender is not specified on the legislator website. 
        
        PARAMS
        -----------
        name_first the first name of legislator
        name_last the last name of the legislator
        biography a block of text that contains legislator information

        RETURNS
        ----------
        'M' if the gender is guessed to be male\n
        'F' if the gender is guessed to be female\n
        None if the gender cannot be guessed
        """

        if biography:
            try:
                return self._guess_gender_from_text(biography)
            except:
                pass
        return self._guess_gender_using_genderComputer(name_first, name_last)


    def _guess_gender_from_text(self, biography):
        count_masculine = 0
        count_femanine = 0

        count_masculine += len(re.findall('Mr. ', biography))
        count_femanine += len(re.findall('Mrs. ', biography)) + len(re.findall('Ms. ', biography))
        
        count_masculine += len(re.findall(r'\she\W', biography)) or len(re.findall(r'\sHe\W', biography))
        count_femanine += len(re.findall(r'\sshe\W', biography)) or len(re.findall(r'\sShe\W', biography))

        if count_masculine > count_femanine:
            return 'M'
        if count_femanine > count_masculine:
            return 'F'
        raise ValueError('Cannot determine gender from biography')

    def _guess_gender_using_genderComputer(self, name_first, name_last):
        gc = GenderComputer()
        name = name_first + ' ' + name_last

        gender_mapping = {
            'female': 'F',
            'mostly female': 'F',
            'mostly male': 'M',
            'male': 'M',
        }

        legislator_gender = gc.resolveGender(name, self.country)
        return gender_mapping.get(legislator_gender)

class LegislationScraperUtils(ScraperUtils):
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
        super().__init__(country, database_table_name, row_type)

        with CursorFromConnectionFromPool() as cur:
            try:
                query = f'SELECT * FROM {legislator_table_name}'
                cur.execute(query)
                legislator_results = cur.fetchall()
            except Exception as e:
                sys.exit(f'An exception occurred retrieving legislator table from database. \
                \nHas the legislator data been collected for this state yet?\n{e}')

        self.legislators = pd.DataFrame(legislator_results)

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

    def add_topics(self, bill_text):

        """
          Pulls a model from an S3 bucket as a temporary file
          Uses the model to classify the bill text
          Returns the dataframe with the topics filled in

          If you want a different model just upload it to the S3
          and change the code in this function to implement that model instead

          Currently this function is being called in both the US and CA prov/terr write_data functions,
          at some point might want two different models/ add topic functions for the different countries

          Takes either a list of dictionaries or a list of rows

          """
        # convert input into dataframe form
        df = pd.DataFrame(bill_text)
        print('Loading model...')
        s3 = boto3.client('s3')
        # load model from S3 bucket named bill-topic-classifier-sample
        with tempfile.NamedTemporaryFile(mode='w+b', delete=False) as f:
            s3.download_fileobj('bill-topic-classifier-sample', 'bert_data_dem4.pt', f)
            mlmodel = f.name
            # print(mlmodel)

        print('Model loaded.')

        df = pd.DataFrame(df)

        possible_labels = ['government operations', 'health', 'education', 'macroeconomics', '',
                           'international affairs', 'civil rights', 'social welfare', 'public lands',
                           'defense', 'domestic commerce', 'law and crime', 'culture', 'transportation',
                           'environment', 'labor', 'housing', 'technology', 'immigration', 'energy',
                           'agriculture', 'foreign trade']
        # the possible labes (CAP topics) are assigned numbers
        label_dict = {'government operations': 0, 'health': 1, 'education': 2, 'macroeconomics': 3, '': 4,
                      'international affairs': 5, 'civil rights': 6, 'social welfare': 7, 'public lands': 8,
                      'defense': 9,
                      'domestic commerce': 10, 'law and crime': 11, 'culture': 12, 'transportation': 13,
                      'environment': 14,
                      'labor': 15, 'housing': 16, 'technology': 17, 'immigration': 18, 'energy': 19, 'agriculture': 20,
                      'foreign trade': 21}
        for index, possible_label in enumerate(possible_labels):
            label_dict[possible_label] = index

        # default initial value is empty string, or informal for all topic entries
        df = df.assign(topic="")
        l = df.topic.replace(label_dict)

        df = df.assign(label=l)

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
        # get pretrained model
        device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
        model = BertForSequenceClassification.from_pretrained("bert-base-uncased",
                                                              num_labels=len(label_dict),
                                                              output_attentions=False,
                                                              output_hidden_states=False)
        model.to(device)

        model.load_state_dict(torch.load(mlmodel, map_location=torch.device('cpu')))

        model.eval()

        loss_val_total = 0

        # make predictions
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

            predictions.append(logits)

        predictions = np.concatenate(predictions, axis=0)

        i = 0
        # fill the dataframe topic column, line by line
        for pred in predictions:
            # print('text:')
            txt = eval_texts[i]

            # print(txt)
            # print("predicted label:")
            pred_label = (possible_labels[np.argmax(pred)])
            # print(pred_label)
            # put predicted value in its row in the topic column of the dataframe
            df.loc[i, 'topic'] = pred_label
            #
            i = i + 1
        # get rid of this label row that was only used for classification
        # df = df.drop(columns=['label'])
        # print(df)
        # return the dataframe to a list of dictionaries
        # dicts = df.to_dict('records')

        return df['topic']


# endregion

# region US Scraper Utils
##########################################
# US SCRAPER UTILS
##########################################


class USFedLegislatorScraperUtils(LegislatorScraperUtils):
    """
    Utilities to help with collecting and storing American federal legislator data.
    """

    def __init__(self, database_table_name='us_fed_legislators'):
        super().__init__('us', database_table_name, USLegislatorRow())

    def get_party_id(self, party_name):
        """
        Return party ID based on a given party. Party must be a full name, such as "Liberal" or "Republicans".
        """
        return self.get_attribute('party', 'party', party_name)

    def get_state_id(self, state_abbreviation):
        """Returns state ID based on a given state abbreviation."""
        return self.get_attribute('division', 'abbreviation', state_abbreviation)

    def write_data(self, data, database_table=None):
        """
        Inserts legislator data into database table. Data must be either a list of Row objects or dictionaries.
        """
        table = database_table if database_table else self.database_table_name
        Persistence.write_us_fed_legislators(data, table)


class USStateLegislatorScraperUtils(USFedLegislatorScraperUtils):
    """
    Scraper utilities for collecting US state legislator data.
    """

    def __init__(self, state_abbreviation, database_table_name='us_state_legislators'):
        super().__init__(database_table_name)
        self.state = state_abbreviation
        self.state_id = self.get_state_id(state_abbreviation)

    def initialize_row(self):
        """Create a Row object filled with default values."""
        row = super().initialize_row()
        row.state = self.state
        row.state_id = self.state_id
        return row


class USFedLegislationScraperUtils(LegislationScraperUtils):
    """
    Scraper used for collecting US Federal legislation data.
    """

    def __init__(self, database_table_name='us_fed_legislation', legislator_table_name='us_fed_legislators'):
        super().__init__('us', database_table_name,
                         legislator_table_name, USLegislationRow())

    def write_data(self, data, database_table=None) -> None:
        """ 
        Takes care of inserting legislation data into database. Must be a list of Row objects or dictionaries.
        """
        # data = self.add_topics(data)
        df = pd.DataFrame(data)
        df['topic'] = self.add_topics(df['bill_text'])
        # df['principal_sponsor_id'] = df['principal_sponsor_id'].fillna(0)
        df['principal_sponsor_id'] = pd.Series(df['principal_sponsor_id'], dtype=int)
        # df['principal_sponsor_id'] = df['principal_sponsor_id'].replace(0, None)

        # print(df['principal_sponsor_id'])

        data = df.to_dict('records')

        table = database_table if database_table else self.database_table_name
        Persistence.write_us_fed_legislation(data, table)


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
##########################################
# CANADIAN SCRAPER UTILS
##########################################


class CAFedLegislatorScraperUtils(LegislatorScraperUtils):
    """
    Utilities to help with collecting and storing legislator data.
    """

    def __init__(self, database_table_name='ca_fed_legislators', row_type=CAFedLegislatorRow()):
        """
        The state_abbreviation, database_table_name, and country come from
        the config.cfg file and must be updated to work properly with your legislation
        data collector.
        """
        super().__init__('ca', database_table_name, row_type)

    def get_party_id(self, party_name, location=None):
        """Return the party ID for a given party and location. Party name must be the party full name."""
        if not location:
            raise exceptions.MissingLocationException(self.get_party_id)
        df = self.parties
        try:
            return int(df.loc[(df['party'] == party_name) & (df['location'] == location), 'id'].values[0])
        except IndexError:
            raise IndexError(f'No party_id found while searching party_name={party_name}, location={location}')

    def get_prov_terr_id(self, prov_terr_abbrev):
        """Returns the province/territory ID for a given province/territory abbreviation."""
        return self.get_attribute('division', 'abbreviation', prov_terr_abbrev)

    def get_region(self, prov_terr_abbrev):
        """Returns the region for a given province/territory abbreviation."""
        return self.get_attribute('division', 'abbreviation', prov_terr_abbrev, 'region')

    def get_prov_terr_abbrev(self, prov_terr):
        """Returns the province/territory abbreviation given the full name of a given province/territory."""
        return self.get_attribute('division', 'division', prov_terr, 'abbreviation')

    def write_data(self, data, database_table=None):
        """
        Inserts legislator data into database. Data must be either a ist of Row objects or dictionaries.
        """
        table = database_table if database_table else self.database_table_name
        Persistence.write_ca_fed_legislators(data, table)


class CAProvTerrLegislatorScraperUtils(CAFedLegislatorScraperUtils):
    def __init__(self, prov_terr_abbreviation, database_table_name='ca_provterr_legislators'):
        """
        Create a ScraperUtils for collection Canadian province and territory legislator data.
        """
        super().__init__(database_table_name, CALegislatorRow())
        self.province_territory = prov_terr_abbreviation
        self.province_territory_id = self.get_prov_terr_id(
            prov_terr_abbreviation)
        self.region = self.get_region(prov_terr_abbreviation)

    def get_party_id(self, party_name):
        """Return the party ID for the party from this province or territory. Party name must be the party full name."""
        return super().get_party_id(party_name, self.province_territory)

    def initialize_row(self):
        """Create a Row object filled with default values."""
        row = super().initialize_row()
        row.province_territory = self.province_territory
        row.province_territory_id = self.province_territory_id
        row.region = self.region
        return row

    def write_data(self, data, database_table=None):
        """
        Inserts legislator data into database. Data must be either a ist of Row objects or dictionaries.
        """
        table = database_table if database_table else self.database_table_name
        Persistence.write_ca_prov_terr_legislators(data, table)


class CAFedLegislationScraperUtils(LegislationScraperUtils):
    def __init__(self, database_table_name='ca_fed_legislation', legislator_table_name='ca_fed_legislators',
                 row_type=CAFedLegislationRow()):
        super().__init__('ca', database_table_name, legislator_table_name, row_type)

    def get_region(self, prov_terr_abbrev):
        """Returns the province/territory region based on a given province/territory abbreviation."""
        return self.get_attribute('division', 'abbreviation', prov_terr_abbrev, 'region')

    def get_prov_terr_abbrev(self, prov_terr):
        """Returns province/territory abbreviation based on the full name of a given province or territory"""
        return self.get_attribute('division', 'division', prov_terr, 'abbreviation')

    def write_data(self, data, database_table=None) -> None:
        """
        Takes care of inserting legislation data into database. Data must be either a list or Row objects or dictionaries.
        """
        
        df = pd.DataFrame(data)
        df['topic'] = self.add_topics(df['bill_text'])
        data = df.to_dict('records')

        table = database_table if database_table else self.database_table_name
        Persistence.write_ca_fed_legislation(data, table)


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

    def write_data(self, data, database_table=None) -> None:
        """
        Takes care of inserting legislation data into database. Must be a list of Row objects or dictionaries.
        """
        # data = self.add_topics(data)
        df = pd.DataFrame(data)
        df['topic'] = self.add_topics(df['bill_text'])
        data = df.to_dict('records')

        table = database_table if database_table else self.database_table_name
        Persistence.write_ca_prov_terr_legislation(data, table)
# endregion

class PDF_Reader():
    '''
    This class requires you to set the page width and page height ratio by specifying the
    width/height of the page (in inches).
    '''
    def get_pdf_pages(self, pdf_url, headers):
        pdf_url_response = self.__initialize_pdf_reader(pdf_url, headers)
        pdf = pdfplumber.open(io.BytesIO(pdf_url_response.content))
        pdf_pages = pdf.pages
        self.page_width = float(pdf_pages[0].width)
        self.page_height = float(pdf_pages[0].height)
        return pdf_pages

    def __initialize_pdf_reader(self, pdf_url, headers):
        return requests.get(pdf_url, headers, stream=True)

    def set_page_width_ratio(self, width_in_inch):
        self.page_width_to_inch_ratio = self.page_width / float(width_in_inch)

    def set_page_half(self, page_half_in_inch):
        self.page_half = page_half_in_inch * self.page_width_to_inch_ratio

    def set_page_height_ratio(self, height_in_inch):
        self.page_height_to_inch_ratio = self.page_height / float(height_in_inch)

    def set_page_top_spacing_in_inch(self, top_spacing_in_inch):
        self.top_spacing = float(top_spacing_in_inch) * self.page_height_to_inch_ratio

    def set_page_bottom_spacing_in_inch(self, bottom_spacing_in_inch):
        self.bottom_spacing = float(bottom_spacing_in_inch) * self.page_height_to_inch_ratio

    def set_left_column_end_and_right_column_start(self, column1_end, column2_start):
        self.left_column_end = column1_end * self.page_width_to_inch_ratio
        self.right_column_start = column2_start * self.page_width_to_inch_ratio

    def is_column(self, page):
        margin_top = page.crop((self.left_column_end, 0, self.right_column_start, self.top_spacing))
        text = margin_top.extract_text()
        if text == None or len(text.strip()) == 0:
            return True
        else:
            return False

    def get_text(self, page, bilingual_separated_by_double_space=False):
        if self.is_page_empty(page):
            return ""
        if self.is_column(page) and not self.is_page_empty(page):
            return self.get_eng_half(page, bilingual_separated_by_double_space)
        if not self.is_column(page) and not self.is_page_empty(page):
            return self.remove_page_number(page)

    def is_page_empty(self, page):
        text = self.remove_page_number(page)
        if text == None:
            return True
        else:
            return False

    def get_eng_half(self, page, is_double_space_separator):
        if is_double_space_separator:
            return self.__get_eng_half_with_double_space_separator(page)
        eng_half = page.crop((0, 0, self.page_half, self.page_height - self.bottom_spacing))
        return eng_half.extract_text()

    def __get_eng_half_with_double_space_separator(self, page):
        page_entire_text =  self.remove_page_number(page)
        page_lines = page_entire_text.split('\n')
        return self.__get_separated_text(page_lines)

    def __get_separated_text(self, page_lines):
        return_string = ''
        for text_line in page_lines:
            text_line = text_line.strip().split('  ')
            if text_line[0] == "•" and len(text_line) != 2 and len(text_line) != 1:
                return_string += ' ' + ' '.join(text_line[:2])
            elif len(text_line) == 1:
                text_line = ''
            else:
                text_line = text_line[0]
                return_string += ' ' + ''.join(text_line)
        return self.__clean_up_text(return_string)

    def __clean_up_text(self, text):
        text = text.replace('\n', '')
        return text.strip()

    def remove_page_number(self, page):
        page_number_removed = page.crop((0, 0, self.page_width, self.page_height - self.bottom_spacing))
        return page_number_removed.extract_text()

class PDF_Table_Reader(PDF_Reader):
    def get_table(self, pages):
        tables = []
        for page in pages:
            table_only_in_page = page.crop((0, self.top_spacing, self.page_width, self.page_height - self.bottom_spacing))
            tables.append(table_only_in_page.extract_table())
        return tables

# region Election Scraper Utils

##########################################
# PREVIOUS ELECTION SCRAPER UTILS
##########################################

class ElectionScraperUtils(ScraperUtils):
    def __init__(self, country: str, table_name: str):
        super().__init__(country, table_name, row_type=ElectionRow())

    def write_data(self, data, database_table=None) -> None:
        """
        Takes care of inserting previous_election data into database. Must be a list of Row objects or dictionaries.
        """
        table = database_table if database_table else self.database_table_name
        Persistence.write_previous_election_data(data, table)

class ElectoralDistrictScraperUtils(ScraperUtils):
    def __init__(self, country: str, table_name: str):
        super().__init__(country, table_name, row_type=ElectoralDistrictsRow())

    def write_data(self, data, database_table=None) -> None:
        """
        Takes care of inserting previous_election data into database. Must be a list of Row objects or dictionaries.
        """
        table = database_table if database_table else self.database_table_name
        Persistence.write_electoral_districts_data(data, table)

class ElectorsScraperUtils(ScraperUtils):
    def __init__(self, country: str, table_name: str):
        super().__init__(country, table_name, row_type=ElectorsRow())

    def write_data(self, data, database_table=None) -> None:
        """
        Takes care of inserting electors data into database. Must be a list of Row objects or dictionaries.
        """
        table = database_table if database_table else self.database_table_name
        Persistence.write_electors(data, table)

class ElectionVotesScraperUtils(ScraperUtils):
    def __init__(self, country: str, table_name: str):
        super().__init__(country, table_name, row_type=ElectionVotesRow())

    def write_data(self, data, database_table=None) -> None:
        """
        Takes care of inserting election votes data into database. Must be a list of Row objects or dictionaries.
        """
        table = database_table if database_table else self.database_table_name
        Persistence.write_election_votes(data, table)

class CandidatesScraperUtils(ScraperUtils):
    def __init__(self, country: str):
        table_name = f'{country.lower()}_candidates'
        super().__init__(country, table_name, row_type=CandidatesRow())

        with CursorFromConnectionFromPool() as cur:
            try:
                query = 'SELECT * FROM ca_electoral_districts'
                cur.execute(query)
                electoral_districts = cur.fetchall()

                query = f'SELECT * FROM ca_legislators'
                cur.execute(query)
                legislators = cur.fetchall()

            except Exception as e:
                sys.exit(
                    f'An exception occurred retrieving tables from database:\n{e}')

        self.electoral_districts = pd.DataFrame(electoral_districts)
        self.legislators = pd.DataFrame(legislators)

    def write_data(self, data, database_table=None) -> None:
        """
        Takes care of inserting previous_election data into database. Must be a list of Row objects or dictionaries.
        """
        table = database_table if database_table else self.database_table_name
        Persistence.write_candidate_data(data, table)

class CandidatesElectionDetails(ScraperUtils):
    def __init__(self, country: str):
        table_name = f'{country.lower()}_candidate_election_details'
        super().__init__(country, table_name, row_type=CandidateElectionDetailsRow())

        with CursorFromConnectionFromPool() as cur:
            try:
                query = 'SELECT * FROM ca_electoral_districts'
                cur.execute(query)
                electoral_districts = cur.fetchall()

                query = f'SELECT * FROM ca_candidates'
                cur.execute(query)
                candidates = cur.fetchall()

                query = f'SELECT * FROM ca_elections'
                cur.execute(query)
                elections = cur.fetchall()

            except Exception as e:
                sys.exit(
                    f'An exception occurred retrieving tables from database:\n{e}')

        self.electoral_districts = pd.DataFrame(electoral_districts)
        self.candidates = pd.DataFrame(candidates)
        self.elections = pd.DataFrame(elections)

    def write_data(self, data, database_table=None) -> None:
        """
        Takes care of inserting previous_election data into database. Must be a list of Row objects or dictionaries.
        """
        table = database_table if database_table else self.database_table_name
        Persistence.write_candidate_election_details_data(data, table)


class FinancialContributionsScraperUtils(ScraperUtils):
    def __init__(self, country: str, table_name: str):
        super().__init__(country, table_name, row_type=FinancialContributionsRow())

    def write_data(self, data, database_table=None) -> None:
        """
        Takes care of inserting election votes data into database. Must be a list of Row objects or dictionaries.
        """
        table = database_table if database_table else self.database_table_name
        Persistence.write_financial_contributions(data, table)


class CandidateElectionFinancesScraperUtils(ScraperUtils):
    def __init__(self, country: str, table_name: str):
        super().__init__(country, table_name, row_type=CandidateElectionFinancesRow())

    def write_data(self, data, database_table=None) -> None:
        """
        Takes care of inserting election votes data into database. Must be a list of Row objects or dictionaries.
        """
        table = database_table if database_table else self.database_table_name
        Persistence.write_candidate_election_finances(data, table)


class InflowScraperUtils(ScraperUtils):
    def __init__(self, country: str, table_name: str):
        super().__init__(country, table_name, row_type=InflowsRow())

    def write_data(self, data, database_table=None) -> None:
        """
        Takes care of inserting data into database. Must be a list of Row objects or dictionaries.
        """
        table = database_table if database_table else self.database_table_name
        Persistence.write_inflows(data, table)


class CandidateElectionVotesScraperUtils(ScraperUtils):
    def __init__(self, country: str, table_name: str):
        super().__init__(country, table_name, row_type=CandidateElectionVotesRow())

    def write_data(self, data, database_table=None) -> None:
        """
        Takes care of inserting data into database. Must be a list of Row objects or dictionaries.
        """
        table = database_table if database_table else self.database_table_name
        Persistence.write_candidate_election_votes(data, table)


class OutflowScraperUtils(ScraperUtils):
    def __init__(self, country: str, table_name: str):
        super().__init__(country, table_name, row_type=OutflowsRow())

    def write_data(self, data, database_table=None) -> None:
        """
        Takes care of inserting data into database. Must be a list of Row objects or dictionaries.
        """
        table = database_table if database_table else self.database_table_name
        Persistence.write_outflows(data, table)


class BankReconciliationUtils(ScraperUtils):
    def __init__(self, country: str, table_name: str):
        super().__init__(country, table_name, row_type=BankReconciliationRow())

    def write_data(self, data, database_table=None) -> None:
        """
        Takes care of inserting data into database. Must be a list of Row objects or dictionaries.
        """
        table = database_table if database_table else self.database_table_name
        Persistence.write_bank_reconciliation(data, table)


class BankAccountUtils(ScraperUtils):
    def __init__(self, country: str, table_name: str):
        super().__init__(country, table_name, row_type=BankAccountRow())

    def write_data(self, data, database_table=None) -> None:
        """
        Takes care of inserting data into database. Must be a list of Row objects or dictionaries.
        """
        table = database_table if database_table else self.database_table_name
        Persistence.write_bank_account(data, table)
# end region
