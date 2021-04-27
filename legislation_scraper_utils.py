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
from database import Database, CursorFromConnectionFromPool, Persistence
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

