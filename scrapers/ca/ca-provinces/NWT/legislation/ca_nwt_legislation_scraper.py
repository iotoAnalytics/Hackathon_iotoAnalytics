import sys
import os
from pathlib import Path

NODES_TO_ROOT = 5
path_to_root = Path(os.path.abspath(__file__)).parents[NODES_TO_ROOT]
sys.path.insert(0, str(path_to_root))

from scraper_utils import CAProvinceTerrLegislationScraperUtils
import pdfplumber
import io
import requests

scraper_utils = CAProvinceTerrLegislationScraperUtils('NT',
                                                      'ca_nt_legislation_test',
                                                      'ca_nt_legislators')

response = requests.get('https://www.ntassembly.ca/sites/assembly/files/21-03-12.pdf', headers=scraper_utils._request_headers, stream=True)
pdf = pdfplumber.open(io.BytesIO(response.content))   

pages = pdf.pages
for page in pages:
    print(page.chars[0])