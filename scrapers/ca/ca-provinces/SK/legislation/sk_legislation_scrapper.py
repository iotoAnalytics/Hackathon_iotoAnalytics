import pandas as pd
import bs4
from urllib.request import Request
from bs4 import BeautifulSoup
import psycopg2
from nameparser import HumanName
from request_url import UrlRequest
import requests
import datefinder
import unidecode
from multiprocessing import Pool
import datetime
import re
import numpy as np
from datetime import datetime
import sys, os
from pathlib import Path
from legislation_scraper_utils import CAProvinceTerrLegislationScraperUtils

prov_terr_abbreviation = 'SK'
database_table_name = 'ca_sk_legislation'
legislator_table_name = 'ca_sk_legislators'
scraper_utils = CAProvinceTerrLegislationScraperUtils(prov_terr_abbreviation,
                                                      database_table_name,
                                                      legislator_table_name)