from random import sample
import pytest
import us_wa_legislation
from us_wa_legislation import PreProgramFunction
from us_wa_legislation import AllDocumentsByClass 
from us_wa_legislation import MainFunctions
from us_wa_legislation import SponsorFromBillId

# class TestGetBiennium:
#     def test_when_current_year_is_odd(self):
#         biennium = PreProgramFunction().get_biennium(2023)
#         assert biennium == '2023-24'

#     def test_when_current_year_is_even(self):
#         biennium = PreProgramFunction().get_biennium(2024)
#         assert biennium == '2023-24'

# class TestGetAllDocumentsByClass:
#     def test_status_code(self):
#         url = 'http://wslwebservices.leg.wa.gov/LegislativeDocumentService.asmx/GetAllDocumentsByClass'
#         params = {
#             "biennium": PreProgramFunction().get_biennium(2021),
#             "documentClass": "Bills"
#         }
#         r = MainFunctions().request_page(url, params)
#         us_wa_legislation.scraper_utils.crawl_delay(us_wa_legislation.crawl_delay)
#         assert r.status_code == 200

#     def test_correct_fields_in_return_data(self):
#         bill_information = AllDocumentsByClass().get_all_bill_information_lxml()
#         bill_information_contents = self.__get_bill_info_contents(bill_information)
#         expected_contents = {
#             'name',
#             'shortfriendlyname',
#             'biennium',
#             'longfriendlyname',
#             'description',
#             'type',
#             'class',
#             'htmurl',
#             'htmcreatedate',
#             'htmlastmodifieddate',
#             'pdfurl',
#             'pdfcreatedate',
#             'pdflastmodifieddate',
#             'billid'
#         }
#         for information in bill_information_contents:
#             assert set(information) == set(expected_contents)

#     def __get_bill_info_contents(self, information):
#         return_list = []
#         for bill in information:
#             bill_info = []
#             for child in bill.children:
#                 if child.name:
#                     bill_info.append(child.name)
#             return_list.append(set(bill_info))
#         return return_list

#     def test_get_relevant_bill_information(self):
#         relevant_bill_information = AllDocumentsByClass().get_relevant_bill_information()
#         expected_contents = {
#             'name',
#             'longfriendlyname', 
#             'htmurl',
#             'pdfurl',
#             'billid'
#         }
#         for bill in relevant_bill_information:
#             assert set(bill.keys()) == set(expected_contents)

@pytest.fixture(scope="class")
def sample_bill_info(request):
    request.cls.sample_bill_info = [
        {
            'name': '1002', 
            'longfriendlyname': 'House Bill 1002', 
            'htmurl': 'http://lawfilesext.leg.wa.gov/biennium/2021-22/Htm/Bills/House Bills/1002.htm', 
            'pdfurl': 'http://lawfilesext.leg.wa.gov/biennium/2021-22/Pdf/Bills/House Bills/1002.pdf', 
            'billid': 'HB 1002'
        },
        {
            'name': '1000', 
            'longfriendlyname': 'House Bill 1000', 
            'htmurl': 'http://lawfilesext.leg.wa.gov/biennium/2021-22/Htm/Bills/House Bills/1002.htm', 
            'pdfurl': 'http://lawfilesext.leg.wa.gov/biennium/2021-22/Pdf/Bills/House Bills/1002.pdf', 
            'billid': 'HB 1000'
        }]
    request.cls.url = 'http://wslwebservices.leg.wa.gov/LegislationService.asmx?op=GetSponsors'
    request.cls.params = {
        "biennium": PreProgramFunction().get_biennium(2021)
    }

class TestGetSponsor:
    def test_get_sponsor_request(self, sample_bill_info):
        billId = self.sample_bill_info[0]['billid']
        self.params["billId"] = billId
        r = MainFunctions().request_page(self.url, self.params)
        us_wa_legislation.scraper_utils.crawl_delay(us_wa_legislation.crawl_delay)
        assert r.status_code == 200

    def test_correct_fields_in_return_data(self, sample_bill_info):
        bill_id_to_check = self.sample_bill_info[0]['billid']
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

    def test_add_sponsor_information_to_bill_information(self):
        sample_bill = self.sample_bill_info[0]
        SponsorFromBillId().add_sponsor_info_to_bill(sample_bill)
        assert sample_bill['sponsors'] != None

    def test_pool_function(self, sample_bill_info):
        all_bills = MainFunctions().append_data_to_bills(SponsorFromBillId().add_sponsor_info_to_bill,
                                             self.sample_bill_info)
        for bill in all_bills:
            assert bill['sponsors'] != None