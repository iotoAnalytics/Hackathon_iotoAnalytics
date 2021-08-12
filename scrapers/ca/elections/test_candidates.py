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

def test_in_check_list_and_is_same_person():
    print("#################\nTest: test_in_check_list_and_is_same_person\n#################")
    election_date = '2019-10-29'

    organizer = candidates.Organizer()
    row1 = CandidatesRow()
    row1.name_first = "John"
    row1.name_last = "Smith"
    row1.name_full = "John Smith"
    row1.current_party_id = 3
    row1.current_electoral_district_id = 6

    row2 = CandidatesRow()
    row2.name_first = "John"
    row2.name_last = "Smith"
    row2.name_full = "John Smith"
    row2.current_party_id = 2
    row2.current_electoral_district_id = 5

    organizer.get_goverlytics_id(row1, election_date)
    result = organizer.get_goverlytics_id(row2, election_date)
    print_test_result('test_in_check_list_and_is_same_person', result, None)

    checked_list = organizer.checked_list
    expected_checked_list = {'John Smith' : 
                                [
                                    {
                                        'party': 'Bloc Québécois',
                                        'electoral_district': 'Banff--Airdrie',
                                        'most_recent_election_date': election_date
                                    }
                                ]
                            }
    print(checked_list)
    print_test_result('test_in_check_list_and_is_same_person', checked_list, expected_checked_list)

def test_in_check_list_and_is_not_same_person():
    print("#################\nTest: test_in_check_list_and_is_not_same_person\n#################")
    election_date = '2019-10-29'

    organizer = candidates.Organizer()
    row1 = CandidatesRow()
    row1.name_first = "John"
    row1.name_last = "Smith"
    row1.name_full = "John Smith"
    row1.current_party_id = 3
    row1.current_electoral_district_id = 6

    row2 = CandidatesRow()
    row2.name_first = "John"
    row2.name_last = "Smith"
    row2.name_full = "John Smith"
    row2.current_party_id = 2
    row2.current_electoral_district_id = 5

    organizer.get_goverlytics_id(row1, election_date)
    result = organizer.get_goverlytics_id(row2, election_date)
    print_test_result('test_in_check_list_and_is_same_person', result, -10)

    checked_list = organizer.checked_list
    expected_checked_list = {"John Smith" : 
                                [
                                    {
                                        "party": 'Bloc Québécois',
                                        "electoral_district": 'Banff--Airdrie',
                                        'most_recent_election_date': election_date
                                    },
                                    {
                                        "party": 'Conservative',
                                        "electoral_district": 'Avignon--La Mitis--Matane--Matapédia',
                                        'most_recent_election_date': election_date
                                    }
                                ]
                            }
    print_test_result('test_in_check_list_and_is_same_person', checked_list, expected_checked_list)
    
def test_not_in_check_list_not_in_db():
    print("#################\nTest: test_not_in_check_list_not_in_db\n#################")
    election_date = '2019-10-29'

    organizer = candidates.Organizer()
    row1 = CandidatesRow()
    row1.name_first = "John"
    row1.name_last = "Smith"
    row1.name_full = "John Smith"
    row1.current_party_id = 3
    row1.current_electoral_district_id = 6

    result = organizer.get_goverlytics_id(row1, election_date)
    print_test_result('test_not_in_check_list_not_in_db', result, -10)

    checked_list = organizer.checked_list
    expected_checked_list = {"John Smith" : 
                                [
                                    {
                                        "party": 'Bloc Québécois',
                                        "electoral_district": 'Banff--Airdrie',
                                        'most_recent_election_date': election_date
                                    }
                                ]
                            }
    print_test_result('test_not_in_check_list_not_in_db', checked_list, expected_checked_list)

def test_not_in_checked_list_in_db_multiple_and_no_match_exists():
    print("#################\nTest: test_not_in_checked_list_in_db_multiple_and_no_match_exists\n#################")
    election_date = '2019-10-29'

    organizer = candidates.Organizer()
    df_add_row = {'goverlytics_id':99999, 'name_full':'Blake Richards', 'name_last':'Richards', 'name_first':'Blake', 'name_middle':'', 'name_suffix':'', 'riding':'Calgary Rocky Ridge', 'party_id':6}
    organizer.legislators_df = organizer.legislators_df.append(df_add_row, ignore_index=True) 

    row1 = CandidatesRow()
    row1.name_first = "Blake"
    row1.name_last = "Richards"
    row1.name_full = "Blake Richards"
    row1.current_party_id = 10
    row1.current_electoral_district_id = 12

    result = organizer.get_goverlytics_id(row1, election_date)
    print_test_result('test_not_in_checked_list_in_db_multiple_and_no_match_exists', result, -10)

    checked_list = organizer.checked_list
    expected_checked_list = {"Blake Richards" : 
                                [
                                    {
                                        "party": 'Progressive Senate Group',
                                        "electoral_district": 'Bécancour--Nicolet--Saurel',
                                        'most_recent_election_date': election_date
                                    }
                                ]
                            }
    print_test_result('test_not_in_checked_list_in_db_multiple_and_no_match_exists', checked_list, expected_checked_list)

def test_not_in_checked_list_in_db_multiple_and_match_exists():
    print("#################\nTest: test_not_in_checked_list_in_db_multiple_and_match_exists\n#################")
    election_date = '2019-10-29'

    organizer = candidates.Organizer()
    df_add_row = {'goverlytics_id':99999, 'name_full':'Blake Richards', 'name_last':'Richards', 'name_first':'Blake', 'name_middle':'', 'name_suffix':'', 'riding':'Calgary Rocky Ridge', 'party_id':6}
    empty_df = pd.DataFrame()
    organizer.legislators_df = empty_df.append(df_add_row, ignore_index=True) 
    organizer.legislators_df = organizer.legislators_df.append(df_add_row, ignore_index=True) 

    row1 = CandidatesRow()
    row1.name_first = "Blake"
    row1.name_last = "Richards"
    row1.name_full = "Blake Richards"
    row1.current_party_id = 10
    row1.current_electoral_district_id = 12

    result = organizer.get_goverlytics_id(row1, election_date)
    print_test_result('test_not_in_checked_list_in_db_multiple_and_match_exists', result, 99999)

    checked_list = organizer.checked_list
    expected_checked_list = {"Blake Richards" : 
                                [
                                    {
                                        "party": 'Independent',
                                        "electoral_district": 'Calgary Rocky Ridge',
                                        'most_recent_election_date': election_date
                                    }
                                ]
                            }
    print_test_result('test_not_in_checked_list_in_db_multiple_and_match_exists', checked_list, expected_checked_list)

def test_not_in_checked_list_in_db_single_and_no_match_exists_party_diff():
    print("#################\nTest: test_not_in_checked_list_in_db_single_and_no_match_exists_party_diff\n#################")
    election_date = '2019-10-29'

    organizer = candidates.Organizer()

    row1 = CandidatesRow()
    row1.name_first = "Blake"
    row1.name_last = "Richards"
    row1.name_full = "Blake Richards"
    row1.current_party_id = 10
    row1.current_electoral_district_id = 6

    result = organizer.get_goverlytics_id(row1, election_date)
    print_test_result('test_not_in_checked_list_in_db_single_and_no_match_exists_party_diff', result, -10)

    checked_list = organizer.checked_list
    expected_checked_list = {"Blake Richards" : 
                                [
                                    {
                                        "party": 'Progressive Senate Group',
                                        "electoral_district": 'Banff--Airdrie',
                                        'most_recent_election_date': election_date
                                    }
                                ]
                            }
    print_test_result('test_not_in_checked_list_in_db_single_and_no_match_exists_party_diff', checked_list, expected_checked_list)

def test_not_in_checked_list_in_db_single_and_no_match_exists_district_diff():
    print("#################\nTest: test_not_in_checked_list_in_db_single_and_no_match_exists_district_diff\n#################")
    election_date = '2019-10-29'

    organizer = candidates.Organizer()

    row1 = CandidatesRow()
    row1.name_first = "Blake"
    row1.name_last = "Richards"
    row1.name_full = "Blake Richards"
    row1.current_party_id = 2
    row1.current_electoral_district_id = 12

    result = organizer.get_goverlytics_id(row1, election_date)
    print_test_result('test_not_in_checked_list_in_db_single_and_no_match_exists_district_diff', result, -10)

    checked_list = organizer.checked_list
    expected_checked_list = {"Blake Richards" : 
                                [
                                    {
                                        "party": 'Conservative',
                                        "electoral_district": 'Bécancour--Nicolet--Saurel',
                                        'most_recent_election_date': election_date
                                    }
                                ]
                            }
    print_test_result('test_not_in_checked_list_in_db_single_and_no_match_exists_district_diff', checked_list, expected_checked_list)

def test_not_in_checked_list_in_db_single_and_match_exists_district_diff():
    print("#################\nTest: test_not_in_checked_list_in_db_single_and_match_exists_district_diff\n#################")
    election_date = '2019-10-29'

    organizer = candidates.Organizer()
    df_add_row = {'goverlytics_id':99999, 'name_full':'Blake Richards', 'name_last':'Richards', 'name_first':'Blake', 'name_middle':'', 'name_suffix':'', 'riding':'Calgary Rocky Ridge', 'party_id':6}
    empty_df = pd.DataFrame()
    organizer.legislators_df = empty_df.append(df_add_row, ignore_index=True) 
    organizer.legislators_df = organizer.legislators_df.append(df_add_row, ignore_index=True) 

    row1 = CandidatesRow()
    row1.name_first = "Blake"
    row1.name_last = "Richards"
    row1.name_full = "Blake Richards"
    row1.current_party_id = 2
    row1.current_electoral_district_id = 12

    row2 = CandidatesRow()
    row2.name_first = "Blake"
    row2.name_last = "Richards"
    row2.name_full = "Blake Richards"
    row2.current_party_id = 3
    row2.current_electoral_district_id = 5

    print(organizer.get_goverlytics_id(row1, election_date))
    print(organizer.get_goverlytics_id(row2, None))

    # print_test_result('test_not_in_checked_list_in_db_single_and_match_exists_district_diff', result, 40609)

    checked_list = organizer.checked_list
    expected_checked_list = {"Blake Richards" : 
                                [
                                    {
                                        "party": 'Conservative',
                                        "electoral_district": 'Banff--Airdrie',
                                        'most_recent_election_date': election_date
                                    }
                                ]
                            }
    print_test_result('test_not_in_checked_list_in_db_single_and_match_exists_district_diff', checked_list, expected_checked_list)
    print(organizer.get_rows())
def test_not_in_checked_list_in_db_single_and_match_exists_both_party_and_district_match():
    print("#################\nTest: test_not_in_checked_list_in_db_single_and_match_exists_both_party_and_district_match\n#################")
    election_date = '2019-10-29'

    organizer = candidates.Organizer()

    row1 = CandidatesRow()
    row1.name_first = "Blake"
    row1.name_last = "Richards"
    row1.name_full = "Blake Richards"
    row1.current_party_id = 2
    row1.current_electoral_district_id = 6

    result = organizer.get_goverlytics_id(row1, election_date)
    print_test_result('test_not_in_checked_list_in_db_single_and_match_exists_both_party_and_district_match', result, 40609)

    checked_list = organizer.checked_list
    expected_checked_list = {"Blake Richards" : 
                                [
                                    {
                                        "party": 'Conservative',
                                        "electoral_district": 'Banff--Airdrie',
                                        'most_recent_election_date': election_date
                                    }
                                ]
                            }
    print_test_result('test_not_in_checked_list_in_db_single_and_match_exists_both_party_and_district_match', checked_list, expected_checked_list)


def print_test_result(test_name, result, expected):
    if result == expected:
        print(f"{test_name} passed")
    else:
        print(f"{test_name} failed")

if __name__ == '__main__':
    # test_in_check_list_and_is_same_person()
    # test_in_check_list_and_is_not_same_person()
    # test_not_in_check_list_not_in_db()
    # test_not_in_checked_list_in_db_multiple_and_no_match_exists()
    # test_not_in_checked_list_in_db_multiple_and_match_exists()
    # test_not_in_checked_list_in_db_single_and_no_match_exists_party_diff()
    # test_not_in_checked_list_in_db_single_and_no_match_exists_district_diff()
    test_not_in_checked_list_in_db_single_and_match_exists_district_diff()
    # test_not_in_checked_list_in_db_single_and_match_exists_both_party_and_district_match()