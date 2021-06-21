import pytest
import us_wa_legislation
from us_wa_legislation import PreProgramFunction
from us_wa_legislation import AllDocumentsByClass 
from us_wa_legislation import MainFunctions

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
            'name',
            'longfriendlyname', 
            'htmurl',
            'pdfurl',
            'billid'
        }
        for bill in relevant_bill_information:
            assert set(bill.keys()) == set(expected_contents)

class TestGetSponsor:
    def test_status_code(self):
        url = 'http://wslwebservices.leg.wa.gov/LegislationService.asmx?op=GetSponsors'
        params = {
            "biennium": PreProgramFunction().get_biennium(2021),
            "billId": "HB 1002"
        }
        r = MainFunctions().request_page(url, params)
        us_wa_legislation.scraper_utils.crawl_delay(us_wa_legislation.crawl_delay)
        assert r.status_code == 200