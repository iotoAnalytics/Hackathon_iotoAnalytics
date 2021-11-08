import unittest

from rows import *

class TestLegislationRow(unittest.TestCase):
    def setUp(self):
        self.ca_legislation_row = LegislationRow()

    def test_valid_string(self):
        self.ca_legislation_row.goverlytics_id = "test"
        self.assertTrue(isinstance(self.ca_legislation_row.goverlytics_id, str))
        
    def test_invalid_string(self):
        with self.assertRaises(TypeError):
            self.ca_legislation_row.goverlytics_id = 11111

    def test_valid_date_introduced(self):
        self.ca_legislation_row.date_introduced = "2022-11-01"
        self.assertTrue(isinstance(self.ca_legislation_row.date_introduced, str))
    
    def test_invalid_date_introduced(self):
        with self.assertRaises(ValueError):
            self.ca_legislation_row.date_introduced = "11-01-2022"

    def test_invalid_source_url(self):
        with self.assertRaises(ValueError):
            self.ca_legislation_row.source_url = ""

    def test_valid_committees(self):
        self.ca_legislation_row.committees = [{"chamber":"x", "committee":"y"}, {"chamber":"a", "committee":"b"}]
        self.assertTrue(isinstance(self.ca_legislation_row.committees, list))
        self.assertTrue(all(isinstance(committee, dict) for committee in self.ca_legislation_row.committees))

    def test_invalid_committees(self):
        with self.assertRaises(TypeError):
            self.ca_legislation_row.committees = [[], []]

        with self.assertRaises(ValueError):
            self.ca_legislation_row.committees = [{"committee":"y"}]

        with self.assertRaises(ValueError):
            self.ca_legislation_row.committees = [{"chamber":"x"}]
    
        with self.assertRaises(ValueError):
            self.ca_legislation_row.committees = [{"chamber":[], "committee":[]}]

    def test_valid_int(self):
        self.ca_legislation_row.principal_sponsor_id = 1111
        self.assertTrue(isinstance(self.ca_legislation_row.principal_sponsor_id, int))\

    def test_invalid_int(self):
        with self.assertRaises(TypeError):
            self.ca_legislation_row.principal_sponsor_id = "1111"

    def test_valid_principal_sponsor(self):
        self.ca_legislation_row.principal_sponsor = "John Smith"
        self.assertTrue(isinstance(self.ca_legislation_row.principal_sponsor, str))

    def test_invalid_principal_sponsor(self):
        with self.assertRaises(ValueError):
            self.ca_legislation_row.principal_sponsor = ""

    def test_valid_sponsors(self):
        self.ca_legislation_row.sponsors = ["john", "david", "beth"]
        self.assertTrue(all(isinstance(sponsor, str) for sponsor in self.ca_legislation_row.sponsors))

    def test_invalid_sponsors(self):
        with self.assertRaises(TypeError):
            self.ca_legislation_row.sponsors = [1, 2, 3]

    def test_valid_sponsor_ids(self):
        self.ca_legislation_row.sponsor_ids = [1, 2, 3]
        self.assertTrue(all(isinstance(sponsor_id, int) for sponsor_id in self.ca_legislation_row.sponsor_ids))

    def test_invalid_sponsor_ids(self):
        with self.assertRaises(TypeError):
            self.ca_legislation_row.sponsors_id = ["1ad", "2asd", "3asda"]

    def test_valid_actions(self):
        sample_action = {
            "date": "2022-11-01",
            "description": "test",
            "action_by": "John"
        }
        self.ca_legislation_row.actions = [sample_action, sample_action]
        self.assertTrue(all(isinstance(action, dict) for action in self.ca_legislation_row.actions))

    def test_invalid_actions(self):
        sample_action = {
            "dte": "2022-11-01",
            "description": "test",
            "action_by": "John"
        }
        with self.assertRaises(ValueError):
            self.ca_legislation_row.actions = [sample_action, sample_action]

        sample_action = {
            "date": "01-22-1111",
            "description": "test",
            "action_by": "John"
        }
        with self.assertRaises(ValueError):
            self.ca_legislation_row.actions = [sample_action, sample_action]

        sample_action = {
            "date": "01-22-1111",
            "description": "test",
        }
        with self.assertRaises(ValueError):
            self.ca_legislation_row.actions = [sample_action, sample_action]

            sample_action = {
            "date": "01-22-1111",
            "action_by": "John"
        }
        with self.assertRaises(ValueError):
            self.ca_legislation_row.actions = [sample_action, sample_action]
            
    def test_valid_votes(self):
        votes_data = {
            'date': '2022-11-01',
            'description': 'test',
            'yea': 1,
            'nay': 0,
            'nv' : 99,
            'absent': 69,
            'total' : 1 + 99 + 69,
            'passed': 1,
            'chamber': 'house',
            'votes': [
                {
                    'goverlytics_id': 12345,
                    'legislator': 'John',
                    'vote_text': 'Y'
                },
                {
                    'goverlytics_id': 12346,
                    'legislator': 'David',
                    'vote_text': 'N'
                }
            ]
        }
        
        self.ca_legislation_row.votes = [votes_data, votes_data]
        self.assertTrue(all(isinstance(vote, dict) for vote in self.ca_legislation_row.votes))
        
    def test_invalid_votes(self):
        votes_data = {
            'date': '01-01-1111',
            'description': 'test',
            'yea': 1,
            'nay': 0,
            'nv' : 99,
            'absent': 69,
            'total' : 1 + 99 + 69,
            'passed': 1,
            'chamber': 'house',
            'votes': [
                {
                    'goverlytics_id': 12345,
                    'legislator': 'John',
                    'vote_text': 'Y'
                },
                {
                    'goverlytics_id': 12346,
                    'legislator': 'David',
                    'vote_text': 'N'
                }
            ]
        }
        with self.assertRaises(ValueError):
            self.ca_legislation_row.votes = [votes_data, votes_data]
        
        votes_data['date'] = '2021-11-11'
        
        votes_data.pop('description')
        with self.assertRaises(ValueError):
            self.ca_legislation_row.votes = [votes_data, votes_data]
            
        votes_data['description'] = 'test'
        votes_data.pop('yea')
        with self.assertRaises(ValueError):
            self.ca_legislation_row.votes = [votes_data, votes_data]
            
        votes_data["yea"] = 1
        votes_data["total"] = 0
        with self.assertRaises(ValueError):
            self.ca_legislation_row.votes = [votes_data, votes_data]
            
        votes_data["total"] = 169
        votes_data["votes"] = [[], []]
        with self.assertRaises(TypeError):
            self.ca_legislation_row.votes = [votes_data, votes_data]
            
class TestUSLegislationRow(unittest.TestCase):
    def setUp(self) -> None:
        self.row = USLegislationRow()
        
    def test_valid_state_id(self):
        self.row.state_id = 1
        self.assertTrue(isinstance(self.row.state_id, int))
        
    def test_invalid_state_id(self):
        with self.assertRaises(TypeError):
            self.row.state_id = "1"
            
    def test_valid_state(self):
        self.row.state = "AL"
        self.assertTrue(isinstance(self.row.state, str))
        
    def test_invalid_state(self):
        with self.assertRaises(ValueError):
            self.row.state = "ALABAMA"
            
class TestCaLegislationRow(unittest.TestCase):
    def setUp(self) -> None:
        self.row = CALegislationRow()
        self.fedrow = CAFedLegislationRow()
        
    def test_valid_state_id(self):
        self.row.province_territory_id = 1
        self.assertTrue(isinstance(self.row.province_territory_id, int))
        
    def test_invalid_state_id(self):
        with self.assertRaises(TypeError):
            self.row.province_territory_id = "1"
            
    def test_valid_state(self):
        self.row.province_territory = "BC"
        self.assertTrue(isinstance(self.row.province_territory, str))
        
    def test_invalid_state(self):
        with self.assertRaises(ValueError):
            self.row.province_territory = "BRITISH COLUMBIA"
            
    def test_sponsor_gender(self):
        self.fedrow.sponsor_gender = 'M'
        self.fedrow.sponsor_gender = 'F'
        self.fedrow.sponsor_gender = 'O'
        
        with self.assertRaises(ValueError):
            self.fedrow.sponsor_gender = 'MALE'
            
    def test_pm_name_full(self):
        self.fedrow.pm_name_full = 'Justin Trudeau'
        self.assertTrue(isinstance(self.fedrow.pm_name_full, str))
        
    def test_invalid_pm_name_full(self):
        with self.assertRaises(ValueError):
            self.fedrow.pm_name_full = ''
        with self.assertRaises(ValueError):
            self.fedrow.pm_name_full = 'Trudeau'
            
    def test_statute_year(self):
        self.fedrow.statute_year = 2020
        with self.assertRaises(TypeError):
            self.fedrow.statute_year = '2021'
        with self.assertRaises(ValueError):
            self.fedrow.statute_year = 202
            
    def test_publicatino(self):
        self.fedrow.publications = ["test1", "test2", "test3"]
        self.assertTrue(isinstance(self.fedrow.publications, List))
        self.assertTrue(all(isinstance(pub, str) for pub in self.fedrow.publications))
        
    def test_invalid_publication(self):
        with self.assertRaises(TypeError):
            self.fedrow.publications = [{1:2}, {3:4}]
            
    def test_last_major_eevnt(self):
        last_major_event = {
            "date": "2021-11-01",
            "status": "test",
            "chamber": "house",
            "committee": "test",
            "meeting_number": 1
        }
        self.fedrow.last_major_event = last_major_event
        self.assertTrue(isinstance(self.fedrow.last_major_event, dict))
        
    def test_invalid_last_major_eevnt(self):
        last_major_event = {
            "date": "11-11-2021",
            "status": "test",
            "chamber": "house",
            "committee": "test",
            "meeting_number": 1
        }
        with self.assertRaises(ValueError):
            self.fedrow.last_major_event = last_major_event
        
        last_major_event.pop("status")
        with self.assertRaises(ValueError):
            self.fedrow.last_major_event = last_major_event

class TestLegislatorRow(unittest.TestCase):
    def setUp(self) -> None:
        self.row = LegislatorRow()
    
    def test_gender(self):
        self.row.gender = 'M'
        self.row.gender = 'F'
        self.row.gender = 'O'
        
        with self.assertRaises(ValueError):
            self.row.gender = 'U'
            
    def test_years_active(self):
        self.years_active = [2011, 2012, 2016]
        
    def test_invalid_years_active(self):
        with self.assertRaises(TypeError):
            self.row.years_active = {}
            
        with self.assertRaises(TypeError):
            self.row.years_active = [2011, 2012, 2016, '2021']
            
        with self.assertRaises(ValueError):
            self.row.years_active = [11, 12, 16]
            
    def test_committees(self):
        committee = {
            "role": "chair",
            "committee": "test"
        }
        self.row.committees = [committee, committee]
        self.assertTrue(isinstance(self.row.committees, List))
        self.assertTrue(all(isinstance(comm, dict) for comm in self.row.committees))
        
    def test_invalid_committees(self):
        committee = {
            "committee": "test"
        }
        with self.assertRaises(ValueError):
            self.row.committees = [committee, committee]
            
        committee = {}
        with self.assertRaises(TypeError):
            self.row.committees = committee
            
    def test_phone_numbers(self):
        numbers = {
            "office": "office",
            "number": "123-456-1231"
        }
        self.row.phone_numbers = [numbers, numbers]
        
        numbers["number"] = "1-123-456-7123"
        self.row.phone_numbers = [numbers, numbers]
        
        numbers["number"] = "52-123-456-7123"
        self.row.phone_numbers = [numbers, numbers]
        
    def test_invalid_number_format(self):
        numbers = {
            "office": "office",
            "number": "(123)456-1231"
        }
        with self.assertRaises(ValueError):
            self.row.phone_numbers = [numbers, numbers]
            
        numbers["number"] = "123-4567891"
        with self.assertRaises(ValueError):
            self.row.phone_numbers = [numbers, numbers]
            
    def test_valid_email(self):
        email = "test@gmail.com"
        self.row.email = email
        
        email = "test.test@gmail.sub.com"
        self.row.email = email
        
        email = "test123@gmail1.wv.com"
        self.row.email = email
        
        email = "test_123.11apples@gov.io"
        self.row.email = email
    
    def test_invalid_email(self):
        email = "test@gmail"
        with self.assertRaises(ValueError):
            self.row.email = email
        
        email = "test"
        with self.assertRaises(ValueError):
            self.row.email = email
        
        email = "@gmail.com"
        with self.assertRaises(ValueError):
            self.row.email = email
            
    def test_birthday(self):
        self.row.birthday = None
        self.row.birthday = "2021-11-01"
        