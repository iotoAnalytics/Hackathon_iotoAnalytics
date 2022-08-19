# import atexit
import boto3
import copy
import dateutil.parser as dparser
import exceptions
import functools
import io
import numpy as np
import pandas as pd
#import pdfplumber
import random
import re
import requests
import sys
import tempfile
import time
# import torch

from bs4 import BeautifulSoup as soup
from database import Database, CursorFromConnectionFromPool, Persistence
from dataclasses import dataclass, field
from datetime import date, datetime
from nameparser import HumanName
from pandas.core.computation.ops import UndefinedVariableError
from rows import *
from transformers import BertTokenizer, BertForSequenceClassification
from torch.utils.data import TensorDataset, DataLoader, SequentialSampler
from urllib.request import urlopen as uReq
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

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
    
# region Municipal scraper utils
##########################################
# MUNICIPAL SCRAPER UTILS
##########################################

class MunicipalUtils(ScraperUtils):
    """Base scraper class. Contains methods common to all legislator scrapers."""

    def __init__(self, database_table_name):
        super().__init__('ca', database_table_name, row_type=MunicipalRow())
    
    # get attribute?
    
    def write_sea_aq_meeting(self,data,database_table=None):
        table = database_table if database_table else self.database_table_name
        Persistence.write_aq_meeting_data(data, table)

    def write_la_aq_meeting(self,data,database_table=None):
        table = database_table if database_table else self.database_table_name
        Persistence.write_aq_meeting_data(data, table)

    def write_pho_aq_meeting(self,data,database_table=None):
        table = database_table if database_table else self.database_table_name
        Persistence.write_aq_meeting_data(data, table)

    def write_spb_aq_meeting(self,data,database_table=None):
        table = database_table if database_table else self.database_table_name
        Persistence.write_aq_meeting_data(data, table)

    def write_hono_aq_meeting(self,data,database_table=None):
        table = database_table if database_table else self.database_table_name
        Persistence.write_aq_meeting_data(data, table)

    def write_cos_aq_meeting(self,data,database_table=None):
        table = database_table if database_table else self.database_table_name
        Persistence.write_aq_meeting_data(data, table)

    def write_den_aq_meeting(self,data,database_table=None):
        table = database_table if database_table else self.database_table_name
        Persistence.write_aq_meeting_data(data, table)

    def write_mke_aq_meeting(self,data,database_table=None):
        table = database_table if database_table else self.database_table_name
        Persistence.write_aq_meeting_data(data, table)

    def write_wac_aq_meeting(self,data,database_table=None):
        table = database_table if database_table else self.database_table_name
        Persistence.write_aq_meeting_data(data, table)
    
    def write_cha_aq_meeting(self,data,database_table=None):
        table = database_table if database_table else self.database_table_name
        Persistence.write_aq_meeting_data(data, table)

    def write_pit_aq_meeting(self,data,database_table=None):
        table = database_table if database_table else self.database_table_name
        Persistence.write_aq_meeting_data(data, table)

    def write_chi_aq_meeting(self,data,database_table=None):
        table = database_table if database_table else self.database_table_name
        Persistence.write_aq_meeting_data(data, table)

    def write_rva_aq_meeting(self,data,database_table=None):
        table = database_table if database_table else self.database_table_name
        Persistence.write_aq_meeting_data(data, table)

    def write_oak_aq_meeting(self,data,database_table=None):
        table = database_table if database_table else self.database_table_name
        Persistence.write_aq_meeting_data(data, table)
# end region