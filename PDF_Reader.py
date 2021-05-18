import sys
import os
import io
from pathlib import Path

NODES_TO_ROOT = 5
path_to_root = Path(os.path.abspath(__file__)).parents[NODES_TO_ROOT]
sys.path.insert(0, str(path_to_root))

import pdfplumber

class PDF_Reader():
    '''
    This class requires you to set the page width and page height ratio by specifying the
    width/height of the page (in inches).
    '''
    def get_pdf_pages(self, pdf_url_response_content):
        pdf = pdfplumber.open(io.BytesIO(pdf_url_response_content))  
        pdf_pages = pdf.pages
        self.page_width = float(pdf_pages[0].width)
        self.page_height = float(pdf_pages[0].height)
        return pdf_pages

    def set_page_width_ratio(self, width_in_inch):
        self.page_width_to_inch_ratio = self.page_width / float(width_in_inch)

    def set_page_half(self, page_half_in_inch):
        self.page_half = page_half_in_inch * self.page_width_to_inch_ratio

    def set_page_height_ratio(self, height_in_inch):
        self.page_height_to_inch_ratio = self.page_height / float(height_in_inch)

    def set_page_top_margin_in_inch(self, top_margin_in_inch):
        self.top_margin = float(top_margin_in_inch) * self.page_height_to_inch_ratio

    def set_left_column_end_and_right_column_start(self, column1_end, column2_start):
        self.left_column_end = column1_end * self.page_width_to_inch_ratio
        self.right_column_start = column2_start * self.page_width_to_inch_ratio

    def is_column(self, page):
        margin_top = page.crop((self.left_column_end, 0, self.right_column_start, self.top_margin))
        text = margin_top.extract_text()
        if text == None:
            return True
        else:
            return False
        
    def is_page_empty(self, page):
        text = page.extract_text()
        if text == None:
            return True
        else:
            return False

    def get_eng_half(self, page):
        eng_half = page.crop((0, 0, self.page_half, page.height))
        return eng_half.extract_text()