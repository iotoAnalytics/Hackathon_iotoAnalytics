import pytest

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