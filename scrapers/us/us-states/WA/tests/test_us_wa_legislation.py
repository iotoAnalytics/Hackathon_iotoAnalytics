import pytest
import sys
from pathlib import Path
import os

NODES_TO_LEGISLATION_FOLDER = 1
path_to_root = Path(os.path.abspath(__file__)).parents[NODES_TO_LEGISLATION_FOLDER]
sys.path.insert(0, str(path_to_root))

from legislation.us_wa_legislation import DataOrganize

class TestRow:
    DataOrganize().set_rows()