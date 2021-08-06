from pathlib import Path
import os
import sys
import candidates

NODES_TO_ROOT = 3
path_to_root = Path(os.path.abspath(__file__)).parents[NODES_TO_ROOT]
sys.path.insert(0, str(path_to_root))

import pandas as pd
from rows import CandidatesRow
from database import Database, CursorFromConnectionFromPool, Persistence

test_row1 = CandidatesRow()
test_row1.name_first = "Blake"
test_row1.name_last = "Richards"
test_row1.name_full = "Blake Richards"
test_row1.current_party_id = 2
test_row1.current_electoral_district_id = 49

print(candidates.Organizer().get_goverlytics_id(test_row1))