import prov_terr_prev_fed_election as main_file
import pytest

class TestMisc:
    def test_correct_num_province_territories(self):
        num = len(main_file.PROVINCE_TERRITORIES)
        assert num == 13

class TestPreparationClass:
    def test_get_election_links(self):
        assert type(main_file.Preparation().get_election_links()) == list

class TestMainScraper:
    links = ['content.aspx?section=ele&document=index&dir=pas/43ge&lang=e', 
             'content.aspx?section=ele&document=index&dir=pas/42ge&lang=e', 
             'content.aspx?section=ele&document=index&dir=pas/41ge&lang=e', 
             'content.aspx?section=ele&document=index&dir=pas/40ge&lang=e', 
             'content.aspx?section=ele&document=index&dir=pas/39ge&lang=e', 
             'content.aspx?section=ele&document=index&dir=pas/38e&lang=e', 
             'content.aspx?section=ele&document=index&dir=pas/37g&lang=e', 
             'content.aspx?section=ele&document=index&dir=pas/tge&lang=e']            

    