# Add new sessions to the list of session constants
REGULAR_SESSION_2021 = '2100'
FIRST_SPECIAL_SESSION_2020 = '201X'
SECOND_SPECIAL_SESSION_2017 = '172X'

SENATE = 'S'
HOUSE = 'H'

HOUSE_BILL = 'HB'
HOUSE_JOINT_RESOLUTION = 'HJR'
HOUSE_CONCURRENT_RESOLUTION = 'HCR'
HOUSE_RESOLUTION = 'HR'
SENATE_BILL = 'SB'
SENATE_JOINT_RESOLUTION = 'SJR'
SENATE_CONCURRENT_RESOLUTION = 'SCR'
SENATE_RESOLUTION = 'SR'

SENATE_DOMAIN = 'oksenate'
HOUSE_DOMAIN = 'okhouse'

def get_session(session_code):
    if session_code == REGULAR_SESSION_2021:
        return '2021 Regular Session'
    elif session_code == FIRST_SPECIAL_SESSION_2020:
        return '2020 First Special Session'
    elif session_code == SECOND_SPECIAL_SESSION_2017:
        return '2017 Second Special Session'

def get_chamber(chamber_code):
    if chamber_code == SENATE:
        return 'Senate'
    elif chamber_code == HOUSE:
        return 'House'

def get_bill_type(bill_code):
    if bill_code in (HOUSE_JOINT_RESOLUTION, HOUSE_CONCURRENT_RESOLUTION, HOUSE_RESOLUTION, SENATE_JOINT_RESOLUTION, SENATE_CONCURRENT_RESOLUTION, SENATE_RESOLUTION):
        return 'Resolution'
    elif bill_code in (HOUSE_BILL, SENATE_BILL):
        return 'Bill'

def get_sponsor_role_from_abbr(sponsor_role_abbr):
    if sponsor_role_abbr == SENATE:
        return 'Senator'
    elif sponsor_role_abbr == HOUSE:
        return 'Representative'

def get_sponsor_role_from_url(sponsor_url):
    if SENATE_DOMAIN in sponsor_url:
        return 'Senator'
    elif HOUSE_DOMAIN in sponsor_url:
        return 'Representative'