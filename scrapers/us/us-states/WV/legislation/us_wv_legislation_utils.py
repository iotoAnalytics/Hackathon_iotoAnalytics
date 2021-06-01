SENATE_QUERY = '&senmem'
HOUSE_QUERY = '&hsemem'
SPEAKER_QUERY = 'Speaker'

# Add new sessions to the list of session constants
SESSIONS = {
    'RS': 'Regular Session',
}

BILL_NAME_ABRV = {
    'House Bill': 'HB',
    'House Concurrent Resolution': 'HCR',
    'House Joint Resolution': 'HJR',
    'House Resolution': 'HR',
    'Senate Bill': 'SB',
    'Senate Concurrent Resolution': 'SCR',
    'Senate Joint Resolution': 'SJR',
    'Senate Resolution': 'SR',
}

CHAMBERS = {
    'S': 'Senate',
    'H': 'House',
}

BILL_TEXT_VERSIONS = ('Enrolled Committee Substitute',
                    'Enrolled Version - Final Version',
                    'Committee Substitute',
                    'Engrossed Version',
                    'Engrossed Committee Substitute',
                    'Introduced Version - Originating in Committee',
                    'Introduced Version')

def get_sponsor_role_from_url(sponsor_url):
    if SPEAKER_QUERY in sponsor_url:
        return 'Speaker'
    elif SENATE_QUERY in sponsor_url:
        return 'Senator'
    elif HOUSE_QUERY in sponsor_url:
        return 'Delegate'