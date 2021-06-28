import sys
import os
from pathlib import Path
import data_collector

NODES_TO_ROOT = 5
path_to_root = Path(os.path.abspath(__file__)).parents[NODES_TO_ROOT]
sys.path.insert(0, str(path_to_root))

import pandas as pd

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

def program_driver():
    all_bills = data_collector.program_driver()
    print(len(all_bills))

if __name__ == "__main__":
    program_driver()