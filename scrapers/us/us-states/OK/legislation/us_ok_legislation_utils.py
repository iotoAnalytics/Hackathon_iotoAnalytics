REGULAR_SESSION_2021 = 2100

SENATE_CHAMBER = 'S'
HOUSE_CHAMBER = 'H'

SENATE_RESOLUTION = 'SR'

ENROLLED = 'SEC OF STATE'

def get_session(session_code):
    if session_code == REGULAR_SESSION_2021:
        return '2021 Regular Session'

def get_chamber(chamber_code):
    if chamber_code == SENATE_CHAMBER:
        return 'Senate'
    elif chamber_code == HOUSE_CHAMBER:
        return 'House'

def get_bill_type(bill_code):
    if bill_code == SENATE_RESOLUTION:
        return 'Resolution'

def get_status(status_code):
    if status_code == ENROLLED:
        return 'Enrolled, filed with Secretary of State'