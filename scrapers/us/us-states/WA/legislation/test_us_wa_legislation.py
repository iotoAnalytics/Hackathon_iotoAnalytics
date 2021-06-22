import re
import pytest
import us_wa_legislation
from us_wa_legislation import PreProgramFunction
from us_wa_legislation import AllDocumentsByClass 
from us_wa_legislation import MainFunctions
from us_wa_legislation import SponsorFromBillId
from us_wa_legislation import BillDetailsFromBillId

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
        r = MainFunctions().request_page(url, params)
        us_wa_legislation.scraper_utils.crawl_delay(us_wa_legislation.crawl_delay)
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

@pytest.fixture(scope="module")
def sample_bill_info():
    return [
        {
            'bill_number': '1002', 
            'htmurl': 'http://lawfilesext.leg.wa.gov/biennium/2021-22/Htm/Bills/House Bills/1002.htm', 
            'pdfurl': 'http://lawfilesext.leg.wa.gov/biennium/2021-22/Pdf/Bills/House Bills/1002.pdf', 
            'bill_id': 'HB 1002'
        },
        {
            'bill_number': '1000', 
            'htmurl': 'http://lawfilesext.leg.wa.gov/biennium/2021-22/Htm/Bills/House Bills/1002.htm', 
            'pdfurl': 'http://lawfilesext.leg.wa.gov/biennium/2021-22/Pdf/Bills/House Bills/1002.pdf', 
            'bill_id': 'HB 1000'
        }
    ]

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
        r = MainFunctions().request_page(self.url, self.params)
        us_wa_legislation.scraper_utils.crawl_delay(us_wa_legislation.crawl_delay)
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
        r = MainFunctions().request_page(self.url, self.params)
        us_wa_legislation.scraper_utils.crawl_delay(us_wa_legislation.crawl_delay)
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