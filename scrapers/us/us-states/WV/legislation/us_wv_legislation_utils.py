SENATE_QUERY = '&senmem'
HOUSE_QUERY = '&hsemem'
SPEAKER_QUERY = 'Speaker'

# Add new sessions to the list of session constants
SESSIONS_FULL = {
    'RS': 'Regular Session',
    '1X': 'First Special Session'
}

BILL_TYPE_FULL = {
    'bill': 'Bill',
    'res': 'Resolution'
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

CHAMBERS_FULL = {
    'S': 'Senate',
    'H': 'House',
}

COMMITTEE_FULL = {
    'AGR': 'Agriculture and Rural Development',
    'BNI': 'Banking and Insurance',
    'ECD': 'Economic Development',
    'ED': 'Education',
    'EIM': 'Energy, Industry and Mining',
    'FIN': 'Finance',
    'GOV': 'Government Organization',
    'HNHR': 'Health and Human Resources',
    'JUD': 'Judiciary',
    'RUL': 'Rules',
    'IC': 'Interstate Cooperation',
    'MIL': 'Military',
    'NR': 'Natural Resources',
    'PEN': 'Pensions',
    'TNI': 'Transportation and Infrastructure',
    'WORK': 'Workforce',
    'ANR': 'Agriculture and Natural Resources',
    'ENG': 'Energy',
    'FD': 'Fire Departments and Emergency Medical Services',
    'GO': 'Government Organization',
    'PNR': 'Pensions and Retirement',
    'PS': 'Political Subdivisions',
    'DA': 'Prevention and Treatment of Substance Abuse',
    'SI': 'Senior, Children, and Family Issues',
    'SB': 'Small Business, Entrepreneurship and Economic Development',
    'VA': 'Veterans\' Affairs and Homeland Security',
    'WD': 'Workforce Development'
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