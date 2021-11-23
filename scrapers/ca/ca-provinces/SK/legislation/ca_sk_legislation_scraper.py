import requests
import re
from io import BytesIO
import pdfx
import pdfplumber
from scraper_utils import CAProvinceTerrLegislationScraperUtils

prov_terr_abbreviation = 'SK'
database_table_name = 'ca_sk_legislation'
legislator_table_name = 'ca_sk_legislators'
scraper_utils = CAProvinceTerrLegislationScraperUtils(prov_terr_abbreviation,
                                                      database_table_name,
                                                      legislator_table_name)
pdf_link = 'https://www.legassembly.sk.ca/media/1398/progress-of-bills.pdf'
crawl_delay = scraper_utils.get_crawl_delay(pdf_link)


def get_bill_info(link):
    response = requests.get(
        link, stream=True, headers=scraper_utils.request_headers)
    pdf = pdfplumber.open(BytesIO(response.content))
    item_lst = []
    for _ in range(0, len(pdf.pages)):
        for item in pdf.pages[_].extract_text().split('\n'):
            try:
                if '*' in item:
                    if int(item[0:2]):
                        num = int(item[0:2])
                    elif int(item[0]):
                        num = int(item[0])
                    item_dict = {'num': num, 'row': item}
                    item_lst.append(item_dict)
                elif int(item[0:3]) and int(item[0:3]) != 202:
                    num = int(item[0:3])
                    item_dict = {'num': num, 'row': item}
                    item_lst.append(item_dict)
            except Exception:
                pass
    scraper_utils.crawl_delay(crawl_delay)
    return item_lst


def get_bill_dict(list):
    bill_dict_lst = []
    for _ in range(0, len(list)):
        url = list[_]
        response = requests.get(
            url, stream=True, headers=scraper_utils.request_headers)
        pdf = pdfplumber.open(BytesIO(response.content))
        title = ''
        sponsor = ''
        bill_text = ''
        for item in pdf.pages:
            text = item.extract_text()
            bill_text += text
            lst = text.split('\n')
            for el in lst:
                if 'Short title' in el:
                    num = lst.index(el) + 1
                    title = lst[num].replace('This Act may be cited as', '')
                    title = title.replace('1', '').replace('-', '').strip()
                elif 'Honourable' in el or 'Mr.' in el:
                    num = lst.index(el)
                    sponsor = lst[num].replace('Honourable', '')
                    sponsor = sponsor.split('L’honorable')[0].strip()
                    sponsor = sponsor.replace('Mr.', '').strip()
        bill_text = ' '.join(bill_text.split('\n'))
        #         bill_text.replace('\n', ' ')
        bill_dict = {'title': title, 'sponsor': sponsor,
                     'bill_text': bill_text, 'url': url}
        bill_dict_lst.append(bill_dict)
    return bill_dict_lst


def com_dict(key):
    try:
        return {
            'CF': 'Committee of Finance',
            'CW': 'Committee of the Whole',
            'CCA': 'Crown and Central Agencies',
            'ECO': 'Economy',
            'HOS': 'House Services',
            'HUS': 'Human Services',
            'IAJ': 'Intergovernmental Affairs and Justice',
            'PBC': 'Private Bills',
            'PAC': 'Public Accounts'
        }[key]
    except KeyError:
        return ''


def month_dict(month):
    month_dict = {
        'Jan': '01',
        'Feb': '02',
        'Mar': '03',
        'Apr': '04',
        'May': '05',
        'Jun': '06',
        'Jul': '07',
        'Aug': '08',
        'Sep': '09',
        'Oct': '10',
        'Nov': '11',
        'Dec': '12'
    }
    return month_dict[month]


def fix_dates(lst):
    date_lst = []
    for date in lst:
        date_split = date.split()
        year = date_split[2]
        month = month_dict(date_split[0])
        day = date_split[1].replace(',', '')
        fixed_date = year + '-' + month + '-' + day
        date_lst.append(fixed_date)
    return date_lst


def date_diff(date_1, date_2):
    month_1 = int(date_1.split('-')[1])
    month_2 = int(date_2.split('-')[1])
    day_1 = int(date_1.split('-')[2])
    day_2 = int(date_2.split('-')[2])
    if abs(month_1 - month_2) <= 1 and abs(day_1 - day_2) <= 1:
        return True
    else:
        return False


def append_other_dicts(lst, s):
    if len(lst) == 0:
        return []
    elif s == 1:
        if len(lst) == 1:
            return [{'date': lst[0], 'action_by': '', 'description': '3rd Reading'}]
        elif len(lst) == 2:
            return [{'date': lst[0], 'action_by': '', 'description': '3rd Reading'},
                    {'date': lst[1], 'action_by': '', 'description': 'Royal Assent'}]
    elif s == 0:
        if len(lst) == 1:
            return [{'date': lst[0], 'action_by': '', 'description': '2nd Reading'}]
        elif len(lst) == 2:
            return [{'date': lst[0], 'action_by': '', 'description': '2nd Reading'},
                    {'date': lst[1], 'action_by': '', 'description': '3rd Reading'}]
        elif len(lst) == 3:
            return [{'date': lst[0], 'action_by': '', 'description': '2nd Reading'},
                    {'date': lst[1], 'action_by': '',
                        'description': '3rd Reading'},
                    {'date': lst[2], 'action_by': '', 'description': 'Royal Assent'}]


# need to make function that makes other action dicts
def actions_list(lst):
    action_dict_lst = []
    action_dict = {'date': lst[0], 'action-by': "",
                   'description': '1st Reading'}
    action_dict_lst.append(action_dict)
    s = 0
    if len(lst) > 1:
        if date_diff(lst[0], lst[1]):
            second_dict = {
                'date': lst[1], 'action-by': '', 'description': 'Royal Rec.'}
            action_dict_lst.append(second_dict)
        else:
            second_dict = {
                'date': lst[1], 'action-by': '', 'description': '2nd Reading'}
            action_dict_lst.append(second_dict)
            s = 1
        for item in append_other_dicts(lst[2:], s):
            action_dict_lst.append(item)
    return action_dict_lst


def match_lst(bill_dict, bill_info_lst):
    for item in bill_dict:
        num = int(item['url'].split('-')[1].replace('.pdf', '').strip())
        sponsor = item['sponsor'].split()
        actions = []
        com = ''
        for el in bill_info_lst:
            if el['num'] == num:
                row = el['row']
                dates = row.split(sponsor[0])
                if len(dates) == 1:
                    dates = row.split(sponsor[1])
                dates = dates[1]
                # print(dates)
                if re.search('[A-Z]{2}', dates):
                    key = re.search('[A-Z]{2}', dates).group()
                    com = com_dict(key)
                dates_lst = re.findall(
                    '[A-Z][a-z]{2}\s[0-9]{2}\,\s[0-9]{4}', dates)
                if re.search('[A-Z][a-z]{2}\s[0-9]{2}\,\s\sA', dates):
                    missing = re.search(
                        '[A-Z][a-z]{2}\s[0-9]{2}\,\s\sA', dates).group()
                    dates_lst.append(missing.replace(' A', '') + str(2020))
                dates_lst = fix_dates(dates_lst)
                actions = actions_list(dates_lst)
        item['actions'] = actions
        item['com'] = com
    return bill_dict


def make_row(bill_el):
    row = scraper_utils.initialize_row()
    sponsor = bill_el['sponsor']
    names = sponsor.split()
    row.principal_sponsor = sponsor
    row.principal_sponsor_id = scraper_utils.get_legislator_id(
        name_first=names[0], name_last=names[1])
    row.actions = bill_el['actions']
    row.source_url = bill_el['url']
    row.bill_text = bill_el['bill_text']
    row.bill_title = bill_el['title']
    row.current_status = bill_el['actions'][-1]['description']
    if 'Amendment' in bill_el['title']:
        row.bill_type = 'Amendment'
    elif 'Act' in bill_el['title']:
        row.bill_type = 'Act'
    bill_name = bill_el['url'].split('/')[-1].replace('.pdf', '')
    row.bill_name = bill_name
    session = re.search('[0-9]{2}', bill_name).group()
    row.session = session
    goverlytics_id = f'{prov_terr_abbreviation}_{session}_{bill_name}'
    row.goverlytics_id = goverlytics_id
    print('done row for: ' + bill_el['title'])
    if bill_el['com'] != '':
        row.committees = [{'chamber': '', 'committee': bill_el['com']}]
    return row


# Read PDF File
pdf = pdfx.PDFx(pdf_link)

# Get list of URL
full_lst = pdf.get_references_as_dict()['pdf']
pdf_lst = []
for item in full_lst:
    if 'EN.pdf' not in item:
        pdf_lst.append(item)
bill_info_lst = get_bill_info(pdf_link)
bill_dict = get_bill_dict(pdf_lst)
print('done lists')
data = [make_row(x) for x in match_lst(bill_dict, bill_info_lst)]
print('done data')
scraper_utils.write_data(data)
print('done')
