import pytest
import sys
from pathlib import Path
import os
import json

NODES_TO_LEGISLATION_FOLDER = 1
path_to_root = Path(os.path.abspath(__file__)).parents[NODES_TO_LEGISLATION_FOLDER]
sys.path.insert(0, str(path_to_root))

from legislation.us_wa_legislation import DataOrganize

@pytest.fixture
def sample_data(request):
    event_file = Path.cwd() / "scrapers" / "us" / "us-states" / "WA" / "tests" / "fixtures" / "sample_data.json"
    with open(event_file) as f:
        return json.load(f)

class TestRow:
    def test_current_biennium(self, sample_data):
        data = DataOrganize()
        data.set_rows(sample_data)

        assert data.row.goverlytics_id == "WA_20212022_1001"
        assert data.row.session == "2021-22" 
        assert data.row.bill_name == '1001'
        assert data.row.source_url == "http://lawfilesext.leg.wa.gov/biennium/2021-22/Pdf/Bills/House_Bills/1001.pdf"
        assert data.row.chamber_origin == 'House'
        assert type(data.row.committees) == list
        assert data.row.committees == [
            {"chamber": "House", "committee": "House Appropriations"}, 
            {"chamber": "Senate", "committee": "Senate Law & Justice"}, 
            {"chamber": "Senate", "committee": "Senate Ways & Means"}
        ]
        assert data.row.bill_type == "Bill"
        assert data.row.bill_title == "AN ACT Relating to establishing a law enforcement professional development outreach grant program;"
        assert data.row.current_status == "Effective date 7/25/2021."
        assert data.row.principal_sponsor_id == 52953
        assert data.row.principal_sponsor == "Jacquelin Maycumber"
        assert type(data.row.sponsors) == list
        assert data.row.sponsors == ['Maycumber', 'Lovick', 'Ryu', 'Boehnke', 'Leavitt', 'Lekanoff', 'Tharinger', 'Goodman', 'Young', 'Graham', 'Cody', 'Robertson', 'Johnson']
        assert type(data.row.actions) == list
        assert data.row.actions[0]['date'] == "2021-01-11"
        assert data.row.actions[0]['action_by'] == 'chamber'
        assert data.row.actions[1]['action_by'] == 'committee'
        assert data.row.actions[-1]['action_by'] == 'secretary of state'
        assert data.row.actions[-2]['action_by'] == 'secretary of state'
        assert data.row.actions[-3]['action_by'] == 'governor'
        assert data.row.actions[-5]['action_by'] == 'president'
        assert data.row.actions[-6]['action_by'] == 'speaker'
        assert type(data.row.votes) == list
        assert data.row.votes[0]['date'] == '2021-04-06'
        assert data.row.votes[0]['total'] == '49'