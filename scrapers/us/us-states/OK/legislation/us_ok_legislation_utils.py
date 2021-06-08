SENATE_DOMAIN = 'oksenate'
HOUSE_DOMAIN = 'okhouse'

# Add new sessions to the list of session constants
SESSIONS = {
    '2100': '2021 Regular Session',
    '201X': '2020 First Special Session',
    '172X': '2017 Second Special Session',
}

CHAMBERS = {
    'S': 'Senate',
    'H': 'House',
}

BILL_TYPES = {
    'HB': 'Bill',
    'HCR': 'Resolution',
    'HJR': 'Resolution',
    'HR': 'Resolution',
    'SB': 'Bill',
    'SCR': 'Resolution',
    'SJR': 'Resolution',
    'SR': 'Resolution',
}

SPONSOR_ROLES = {
    'S': 'Senator',
    'H': 'Representative',
}

def get_sponsor_role_from_url(sponsor_url):
    if SENATE_DOMAIN in sponsor_url:
        return 'Senator'
    elif HOUSE_DOMAIN in sponsor_url:
        return 'Representative'