import re
from typing import Iterable
import pytest
import datetime
import os
import sys
from pathlib import Path

NODES_TO_LEGISLATION_FOLDER = 1
path_to_root = Path(os.path.abspath(__file__)).parents[NODES_TO_LEGISLATION_FOLDER].joinpath("legislation")
sys.path.insert(0, str(path_to_root))

import data_collector 
from data_collector import PreProgramFunction
from data_collector import AllDocumentsByClass 
from data_collector import MainFunctions
from data_collector import SponsorFromBillId
from data_collector import BillDetailsFromBillId
from data_collector import GetVotes
from data_collector import GetCommittees
from data_collector import GetActions

class TestGetBiennium:
    def test_when_current_year_is_odd(self):
        biennium = PreProgramFunction().get_biennium(2023)
        assert biennium == '2023-24'

    def test_when_current_year_is_even(self):
        biennium = PreProgramFunction().get_biennium(2024)
        assert biennium == '2023-24'

class TestGetAllDocumentsByClass:
    def test_status_code(self):
        url = 'http://wslwebservices.leg.wa.gov/LegislativeDocumentService.asmx/GetAllDocumentsByClass'
        params = {
            "biennium": PreProgramFunction().get_biennium(2021),
            "documentClass": "Bills"
        }
        r = MainFunctions().get_request(url, params)
        data_collector.scraper_utils.crawl_delay(data_collector.crawl_delay)
        assert r.status_code == 200

    def test_correct_fields_in_return_data(self):
        bill_information = AllDocumentsByClass().get_all_bill_information_lxml()
        bill_information_contents = self.__get_bill_info_contents(bill_information)
        expected_contents = {
            'name',
            'shortfriendlyname',
            'biennium',
            'longfriendlyname',
            'description',
            'type',
            'class',
            'htmurl',
            'htmcreatedate',
            'htmlastmodifieddate',
            'pdfurl',
            'pdfcreatedate',
            'pdflastmodifieddate',
            'billid'
        }
        for information in bill_information_contents:
            assert set(information) == set(expected_contents)

    def __get_bill_info_contents(self, information):
        return_list = []
        for bill in information:
            bill_info = []
            for child in bill.children:
                if child.name:
                    bill_info.append(child.name)
            return_list.append(set(bill_info))
        return return_list

    def test_get_relevant_bill_information(self):
        relevant_bill_information = AllDocumentsByClass().get_relevant_bill_information()
        expected_contents = {
            'bill_number',
            'htmurl',
            'pdfurl',
            'bill_id'
        }
        for bill in relevant_bill_information:
            assert set(bill.keys()) == set(expected_contents)

    def test_get_relevant_bill_information_only_if_bill_number_has_no_suffix(self):
        relevant_bill_information = AllDocumentsByClass().get_relevant_bill_information()
        for bill in relevant_bill_information:
            assert not re.search(r'\D', bill['bill_number'])

@pytest.fixture(scope="class")
def instance_variable_for_sponsor_test(request):
    request.cls.url = 'http://wslwebservices.leg.wa.gov/LegislationService.asmx/GetSponsors'
    request.cls.params = {
            "biennium": PreProgramFunction().get_biennium(2021)
    }

class TestGetSponsor:
    def test_get_sponsor_request(self, sample_bill_info, instance_variable_for_sponsor_test):
        billId = sample_bill_info[0]['bill_id']
        self.params["billId"] = billId
        r = MainFunctions().get_request(self.url, self.params)
        data_collector.scraper_utils.crawl_delay(data_collector.crawl_delay)
        assert r.status_code == 200

    def test_correct_fields_in_return_data(self, sample_bill_info):
        bill_id_to_check = sample_bill_info[0]['bill_id']
        sponsor_information = SponsorFromBillId().get_sponsor_information_for_bill_lxml(bill_id_to_check)
        sponsor_information_contents = self.__get_sponsor_info_contents(sponsor_information)
        expected_contents = {
            'id',
            'name',
            'longname',
            'agency',
            'acronym',
            'type',
            'order',
            'phone',
            'email',
            'firstname',
            'lastname',
        }
        for information in sponsor_information_contents:
            assert set(information) == set(expected_contents)

    def __get_sponsor_info_contents(self, information):
        return_list = []
        for sponsor in information:
            sponsor_info = []
            for child in sponsor.children:
                if child.name:
                    sponsor_info.append(child.name)
            return_list.append(set(sponsor_info))
        return return_list

    def test_get_relevant_bill_information(self):
        relevant_sponsor_information = SponsorFromBillId().get_relevant_bill_information('HB 1000')
        expected_contents = {
            'firstname',
            'lastname', 
            'type'
        }
        for sponsor in relevant_sponsor_information:
            assert set(sponsor.keys()) == set(expected_contents)

    def test_add_sponsor_information_to_bill_information(self, sample_bill_info):
        sample_bill = sample_bill_info[0]
        SponsorFromBillId().add_sponsor_info_to_bill(sample_bill)
        assert sample_bill['sponsors'] != None

    def test_pool_function(self, sample_bill_info):
        all_bills = MainFunctions().append_data_to_bills(SponsorFromBillId().add_sponsor_info_to_bill,
                                             sample_bill_info)
        for bill in all_bills:
            assert bill['sponsors'] != None

@pytest.fixture(scope="class")
def instance_variable_for_bill_detail_test(request):
    request.cls.url = 'http://wslwebservices.leg.wa.gov/LegislationService.asmx/GetLegislation'
    request.cls.params = {
            "biennium": PreProgramFunction().get_biennium(2021)
    }

class TestGetBillDetails:
    def test_get_detail_request(self, sample_bill_info, instance_variable_for_bill_detail_test):
        bill_number = sample_bill_info[0]['bill_number']
        self.params["billNumber"] = int(bill_number)
        r = MainFunctions().get_request(self.url, self.params)
        data_collector.scraper_utils.crawl_delay(data_collector.crawl_delay)
        assert r.status_code == 200

    def test_get_relevant_bill_information(self, sample_bill_info):
        bill_number = sample_bill_info[0]['bill_number']
        relevant_bill_details = BillDetailsFromBillId().get_relevant_bill_information(bill_number)
        expected_contents = {
            'bill_type',
            'bill_origin', 
            'bill_current_status',
            'bill_description',
            'legal_title',
        }
        assert set(relevant_bill_details.keys()) == set(expected_contents)

    def test_add_bill_details_to_bill_information(self, sample_bill_info):
        sample_bill = sample_bill_info[0]
        BillDetailsFromBillId().add_bill_details_to_bill(sample_bill)
        assert sample_bill['bill_title'] != None
        assert sample_bill['chamber_origin'] != None
        assert sample_bill['current_status'] != None
        assert sample_bill['bill_description'] != None
        assert sample_bill['bill_type'] != None

    def test_pool_function(self, sample_bill_info):
        all_bills = MainFunctions().append_data_to_bills(BillDetailsFromBillId().add_bill_details_to_bill,
                                             sample_bill_info)
        for bill in all_bills:
            assert bill['bill_title'] != None
            assert bill['chamber_origin'] != None
            assert bill['current_status'] != None
            assert bill['bill_description'] != None
            assert bill['bill_type'] != None

@pytest.fixture(scope="class")
def instance_variable_for_bill_votes_test(request):
    request.cls.url = 'http://wslwebservices.leg.wa.gov/LegislationService.asmx/GetRollCalls'
    request.cls.params = {
            "biennium": PreProgramFunction().get_biennium(2021)
    }
    request.cls.all_bills = AllDocumentsByClass().get_data()

class TestGetVotes:
    def test_get_votes_request(self, sample_bill_info, instance_variable_for_bill_votes_test):
        bill_number = sample_bill_info[0]['bill_number']
        self.params["billNumber"] = int(bill_number)
        r = MainFunctions().get_request(self.url, self.params)
        data_collector.scraper_utils.crawl_delay(data_collector.crawl_delay)
        assert r.status_code == 200

    def test_process_no_vote_data(self):
        votes = []
        assert GetVotes().get_relevant_votes_information(votes) == []

    def test_rollcall_has_all_necessary_data(self):
        # this test is slow so testS a subset.
        for bill in self.all_bills[:50]:
            vote_data = GetVotes().get_votes_information_lxml(bill['bill_number'])
            if vote_data:
                for vote in vote_data:
                    assert vote.find('agency')
                    assert vote.find('votedate')
                    assert vote.find('yeavotes')
                    assert vote.find('membersvoting')
                    assert vote.find('nayvotes')
                    assert vote.find('absentvotes')
                    assert vote.find('excusedvotes')
                    assert vote.find('votes')
    
    def test_get_relevant_votes_information(self):
        expected_params = {
            'date': '',
            'description': '',
            'yea': '',
            'nay': '',
            'nv': '',
            'absent': '',
            'total':'',
            'passed': '',
            'chamber': '',
            'votes': [],
        }

        for bill in self.all_bills[:50]:
            vote_data = GetVotes().get_votes_information_lxml(bill['bill_number'])
            relevant_vote_information = GetVotes().get_relevant_votes_information(vote_data)

            if not vote_data:
                assert relevant_vote_information == []
            else:
                for element in relevant_vote_information:
                    assert element.keys() == expected_params.keys()

    def test_add_vote_data_to_bill_information(self, sample_bill_info):
        sample_bill = sample_bill_info[0]
        GetVotes().add_vote_data_to_bill(sample_bill)
        assert sample_bill['votes'] != None

    def test_pool_function(self, sample_bill_info):
        all_bills = MainFunctions().append_data_to_bills(GetVotes().add_vote_data_to_bill,
                                             sample_bill_info)
        for bill in all_bills:
            assert bill['votes'] != None

@pytest.fixture(scope="class")
def instance_variable_for_committees_test(request):
    request.cls.url = 'http://wslwebservices.leg.wa.gov/CommitteeActionService.asmx/GetCommitteeReferralsByBill'
    request.cls.params = {
            "biennium": PreProgramFunction().get_biennium(2021)
    }

class TestGetCommittee:
    def test_get_committee_request(self, sample_bill_info, instance_variable_for_committees_test):
        bill_number = sample_bill_info[1]['bill_number']
        self.params["billNumber"] = int(bill_number)
        r = MainFunctions().get_request(self.url, self.params)
        data_collector.scraper_utils.crawl_delay(data_collector.crawl_delay)
        assert r.status_code == 200

    def test_get_committtees_data(self, sample_bill_info, instance_variable_for_committees_test):
        bill_number = sample_bill_info[1]['bill_number']
        assert isinstance(GetCommittees().get_committees_data_lxml(bill_number), Iterable)

    def test_committee_data_has_all_info(self, sample_bill_info):
            for bill in sample_bill_info:
                committee_data = GetCommittees().get_committees_data_lxml(bill['bill_number'])
                if committee_data:
                    for committee in committee_data:
                        assert committee.find('id')
                        assert committee.find('longname')
                        assert committee.find('agency')
 
    def test_get_relevant_committees_information(self, sample_bill_info):
        expected_params = {
            'chamber',
            'committee'
        }

        for bill in sample_bill_info:
            committee_data = GetCommittees().get_committees_data_lxml(bill['bill_number'])
            relevant_committee_information = GetCommittees().get_relevant_committee_information(committee_data)

            if not committee_data:
                assert relevant_committee_information == []
            else:
                for element in relevant_committee_information:
                    assert set(element.keys()) == expected_params

    def test_add_committee_data_to_bill_information(self, sample_bill_info):
        sample_bill = sample_bill_info[0]
        GetCommittees().add_committee_data_to_bill(sample_bill)
        assert sample_bill['committees'] != None

    def test_pool_function(self, sample_bill_info):
        all_bills = MainFunctions().append_data_to_bills(GetCommittees().add_committee_data_to_bill,
                                             sample_bill_info)
        for bill in all_bills:
            assert bill['committees'] != None

@pytest.fixture(scope="class")
def instance_variable_for_actions_test(request):
    request.cls.url = 'http://wslwebservices.leg.wa.gov/LegislationService.asmx/GetLegislativeStatusChangesByBillId'
    request.cls.params = {
            "biennium": PreProgramFunction().get_biennium(2021),
            "beginDate": datetime.datetime(2021, 1, 1),
            "endDate": datetime.datetime.now()
    }

class TestGetActions:
    def test_get_actions_request(self, sample_bill_info, instance_variable_for_actions_test):
        bill_id = sample_bill_info[1]['bill_id']
        self.params["billId"] = bill_id
        r = MainFunctions().get_request(self.url, self.params)
        data_collector.scraper_utils.crawl_delay(data_collector.crawl_delay)
        assert r.status_code == 200

    def test_get_actions_data(self, sample_bill_info, instance_variable_for_actions_test):
        bill_id = sample_bill_info[1]['bill_id']
        assert isinstance(GetActions().get_actions_data_lxml(bill_id), Iterable)

    def test_action_data_has_all_info(self, sample_bill_info):
            for bill in sample_bill_info:
                action_data = GetActions().get_actions_data_lxml(bill['bill_number'])
                if action_data:
                    for action in action_data:
                        assert action.find('actiondate')
                        assert action.find('historyline')

    def test_get_relevant_actions_information(self, sample_bill_info):
        expected_params = {
            'actiondate',
            'historyline'
        }

        for bill in sample_bill_info:
            actions_data = GetActions().get_actions_data_lxml(bill['bill_number'])
            relevant_actions_information = GetActions().get_relevant_actions_information(actions_data)

            if not actions_data:
                assert relevant_actions_information == []
            else:
                for element in relevant_actions_information:
                    assert set(element.keys()) == expected_params

    def test_add_actions_data_to_bill_information(self, sample_bill_info):
        sample_bill = sample_bill_info[0]
        GetActions().add_actions_data_to_bill(sample_bill)
        assert sample_bill['actions'] != None

    def test_pool_function(self, sample_bill_info):
        all_bills = MainFunctions().append_data_to_bills(GetActions().add_actions_data_to_bill,
                                             sample_bill_info)
        for bill in all_bills:
            assert bill['actions'] != None
