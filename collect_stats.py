import sys
import os
from pathlib import Path
import boto3
from database import Persistence

# Get path to the root directory so we can import necessary modules

# makes dummy table in database
# Persistence.write_sam_data_test('test_table_sam')
# print('Done!')

# grab all file names from us_legislators folder
state_lst = [x.lower() for x in os.listdir("./scrapers/us/us-states")]
print(state_lst)