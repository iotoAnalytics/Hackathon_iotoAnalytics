import abc
from datetime import date
import re

from torch import are_deterministic_algorithms_enabled
from rows import *

#TODO format to test for text and int, then remaining just as it is.

class RowValidator:
    @abc.abstractmethod
    def validate_row(self, row):
        pass

    def raise_exception(self, message):
        raise Exception(message)

class LegislationRowValidator(RowValidator):
    def validate_rows(self, rows):
        try:
            for row in rows:
                self.validate_row(row)
            print("All rows are valid!")
        except Exception as e:
            print(f"Error in bill: {row.bill_name}")
            print(e.with_traceback())

    def validate_universal_row_data(self, row):
        self._test_goverlytics_id(row)
        self._test_text_components(row)
        self._test_date_introduced(row)
        self._test_source_url(row)
        self._test_committees(row)
        self._test_principal_sponsor(row)
        self._test_sponsors(row)
        self._test_cosponsors(row)
        self._test_actions(row)
        self._test_votes(row)

    def _test_goverlytics_id(self, row):
        if type(row.goverlytics_id) != str:
            self.raise_exception("goverlytics_id must be a string")
        gov_id_split = row.goverlytics_id.split('_')
        if len(gov_id_split) != 3:
            self.raise_exception("Improper goverlytics_id format")

    def _test_text_components(self, row):
        if type(row.source_id) != str:
            self.raise_exception("source_id must be a string.")
        if type(row.bill_name) != str:
            self.raise_exception("bill_name must be a string.")
        if type(row.session) != str:
            self.raise_exception("session must be a string.")
        if type(row.chamber_origin) != str:
            self.raise_exception("chamber_origin must be a string.")
        if type(row.bill_type) != str:
            self.raise_exception("bill_type must be a string.")
        if type(row.bill_title) != str:
            self.raise_exception("bill_title must be a string.")
        if type(row.current_status) != str:
            self.raise_exception("current_status must be a string.")
        if type(row.bill_text) != str:
            self.raise_exception("bill_text must be a string.")
        if type(row.bill_description) != str:
            self.raise_exception("bill_description must be a string.")
        if type(row.bill_summary) != str:
            self.raise_exception("bill_summary must be a string.")

    def _test_date_introduced(self, row):
        date = row.date_introduced
        if date and not re.match(r'[0-9]{4}-[0-9]{2}-[0-9]{2}', date):
            self.raise_exception("Improper date formating in date_introduced")

    def _test_source_url(self, row):
        if row.source_url == None or len(row.source_url) == 0:
            self.raise_exception("Source_url must not be null or an empty string.")

    def _test_committees(self, row):
        committees = row.committees
        if type(committees) != list:
            self.raise_exception("committees data must be formatted as a list of dictionaries")
        for committee in committees:
            if type(committee) != dict:
                self.raise_exception("committees data must be formatted as a list of dictionaries")
            if not committee.get('chamber'):
                self.raise_exception("committees data must have chamber information")
            if not committee.get('committee'):
                self.raise_exception("committees data must have committee information")

    def _test_principal_sponsor(self, row):
        principal_sponsor_id = row.principal_sponsor_id
        if type(principal_sponsor_id) != int:
            self.raise_exception("principal_sponsor_id must be an int")
        
        principal_sponsor = row.principal_sponsor
        if type(principal_sponsor) != str:
            self.raise_exception("principal sponsor must be a string")
        principal_sponsor_name_split = principal_sponsor.split(' ')
        if len(principal_sponsor_name_split) <= 1:
            self.raise_exception("principal_sponsor name must be provided a full name.")

    def _test_sponsors(self, row):
        sponsors = row.sponsors
        if type(sponsors) != list:
            self.raise_exception("sponsors data must be formatted as a list of strings")
        for sponsor in sponsors:
            if type(sponsor) != str:
               self.raise_exception("sponsors data must be formatted as a list of strings")

        sponsors_id = row.sponsors_id
        if type(sponsors_id) != list:
            self.raise_exception("sponsors_id data must be formatted as a list of ints")
        for id in sponsors_id:
            if type(id) != int:
               self.raise_exception("sponsors_id data must be formatted as a list of ints")

    def _test_cosponsors(self, row):
        cosponsors = row.cosponsors
        if type(cosponsors) != list:
            self.raise_exception("cosponsors data must be formatted as a list of strings")
        for cosponsor in cosponsors:
            if type(cosponsor) != str:
               self.raise_exception("cosponsors data must be formatted as a list of strings")

        cosponsors_id = row.cosponsors_id
        if type(cosponsors_id) != list:
            self.raise_exception("cosponsors_id data must be formatted as a list of ints")
        for id in cosponsors_id:
            if type(id) != int:
               self.raise_exception("cosponsors_id data must be formatted as a list of ints")

    def _test_actions(self, row):
        actions = row.actions
        if type(actions) != list:
            self.raise_exception("actions data must be formatted as a list of dictionaries")
        for action in actions:
            if type(action) != dict:
                self.raise_exception("actions data must be formatted as a list of dictionaries")
            date = action['date']
            if not re.match(r'[0-9]{4}-[0-9]{2}-[0-9]{2}', date):
                self.raise_exception("Improper date formating in date")
            if not action['action_by'] or type(action['action_by']) != str:
                self.raise_exception("actions data must have action_by attribute in string format")
            if not action['description'] or type(action['description']) != str:
                self.raise_exception("action data requires a valid description in string format")

    def _test_votes(self, row):
        votes = row.votes
        if type(votes) != list:
            self.raise_exception("votes data must be formatted as a list of dictionaries")
        for vote in votes:
            if type(vote) != dict:
                self.raise_exception("votes data must be formatted as a list of dictionaries")
            date = vote['date']
            if not re.match(r'[0-9]{4}-[0-9]{2}-[0-9]{2}', date):
                self.raise_exception("Improper date formating in date")
            if type(vote['description']) != str or not vote['description']:
                self.raise_exception("votes data must have description attribute in string format")
            if type(vote['yea']) != int or type(vote['nay']) != int or \
                    type(vote['nv']) != int or type(vote['absent']) != int or type(vote['total']) != int:
                self.raise_exception('vote numbers for votes data must be in numeral format')
            if vote['yea'] + vote['nay'] + vote['nv'] + vote['absent'] != vote['total']:
                self.raise_exception('all the votes do not add up to the total')
            if vote['passed'] != 0 and vote['passed'] != 1:
                self.raise_exception('vote passed data must be 0 (not passed) or 1 (passed)')
            vote_data = vote['votes']
            self.__test_vote_data(vote_data)

    def __test_vote_data(self, vote_data):
        if type(vote_data) != list:
            self.raise_exception("voting data in votes must be formatted as a list of dictionaries")
        for vote in vote_data:
            if type(vote) != dict:
                self.raise_exception("voting data in votes must be formatted as a list of dictionaries")
            if not vote['goverlytics_id']:
                self.raise_exception("votes data must have a valid goverlytics_id")
            if not vote['legislator']:
                self.raise_exception("votes data must have a valid legislator")
            if not vote['votetext']:
                self.raise_exception("vote data must have a valid legislator vote")

class USLegislationRowValidator(LegislationRowValidator):
    def validate_row(self, row):
        self.validate_universal_row_data(row)
        self._test_state_id(row)
        self._test_state(row)

    def _test_state_id(self, row):
        if type(row.country_id) != int or not row.country_id:
            self.raise_exception("state_id must be an int")
    
    def _test_state(self, row):
        if type(row.state) != str or not row.state:
            self.raise_exception("state must be a string")
        if len(row.state) != 2:
            self.raise_exception("state must be in abbreviated form (ex. AL, TX)")

class CALegislationRowValidator(LegislationRowValidator):
    def validate_row(self, row):
        self.validate_universal_row_data(row)
        self._test_province_territory_id(row)
        self._test_province_territory(row)

    def _test_province_territory_id(self, row):
        if type(row.province_territory_id) != int or not row.province_territory_id:
            self.raise_exception("province_territory_id must be an int")

    def _test_province_territory(self, row):
        if type(row.province_territory) != str or not row.province_territory:
            self.raise_exception("province_territory must be a string")
        if len(row.province_territory) != 2:
            self.raise_exception("province_territory must be abbreviated (ex. BC, SK)")
        
    def _test_region(self, row):
        if type(row.region) != str or not row.region:
            self.raise_exception("region must be a string (ex. Prairies)")

class CAFedLegislationRowValidator(CALegislationRowValidator):
    def validate_row(self, row):
        self.validate_universal_row_data(row)
        self._test_province_territory_id(row)
        self._test_province_territory(row)
        self._test_sponsor_affiliation(row)
        self._test_sponsor_gender(row)
        self._test_pm_name_full(row)
        self._test_pm_party_id(row)
        self._test_statute_year(row)
        self._test_statute_chapter(row)
        self._test_publications(row)
        self._test_last_major_event(row)

    def _test_sponsor_affiliation(self, row):
        if type(row.sponsor_affiliation) != str or not row.sponsor_affiliation:
            self.raise_exception("sponsor_affiliation must be of type string (ex. Minister of Finance, York -- Simcoe")
        
    def _test_sponsor_gender(self, row):
        if type(row.sponsor_gender) != str or not row.sponsor_gender:
            self.raise_exception("sponsor_gender data must be of type string")
        
        if row.sponsor_gender != 'M' and row.sponsor_gender != 'F':
            self.raise_exception("sponsor_gender data must be either M or F (note: this may need to chane in the future)")

    def _test_pm_name_full(self, row):
        if type(row.pm_name_full) != str or not row.pm_name_full:
            self.raise_exception("pm_name_full data must be of type string")
        name_split = row.pm_name_full.split(' ')
        if len(name_split) <= 1:
            self.raise_exception("pm_name_full must be the full name of pm (ex. Justin Trudeau)")

    def _test_pm_party_id(self, row):
        if type(row.pm_party_id) != int or not row.pm_party_id:
            self.raise_exception("pm_party_id data must be of type int")

    def _test_statute_year(self, row):
        if type(row.statute_year) != int or not row.statute_year:
            self.raise_exception("statute_year data must of be type int")

    def _test_statute_chapter(self, row):
        if type(row.statute_chapter) != int or not row.statute_chapter:
            self.raise_exception("statute_chapter must be of type int")

    def _test_publications(self, row):
        if type(row.publications) != list or not row.publications:
            self.raise_exception("publications data must be a list of string (ex. ['First Reading', 'Royal Assent']")
        
        for publication in row.publications:
            if type(publication) != str:
                self.raise_exception("publications data must be a list of string (ex. ['First Reading', 'Royal Assent']")

    def _test_last_major_event(self, row):
        if type(row.last_major_event) != dict or not row.last_major_event:
            self.raise_exception("last_major_event data must be of type dict")
        date = row.last_major_event['date']
        if type(date) != type(datetime):
            self.raise_exception("date in last_major_event must be of type datetime")
        status = row.last_major_event['status']
        chamber = row.last_major_event['chamber']
        committee = row.last_major_event['committee']
        meeting_number = row.last_major_event['meeting_number']

        if type(status) != str or not status:
            self.raise_exception("status in last_major_event must be of type string")
        if type(chamber) != str or not chamber:
            self.raise_exception("chamber in last_major_event must be of type string")
        if type(committee) != str or not committee:
            self.raise_exception("committee in last_major_event must be of type string")
        if type(meeting_number) != int or not meeting_number:
            self.raise_exception("meeting_number in last_major_event must be of type int")

class LegislatorRowValidator(RowValidator):
    def validate_rows(self, rows):
        try:
            for row in rows:
                self.validate_row(row)
            print("All rows are valid!")
        except Exception as e:
            print(f"Error in legislator: {row.name_full}")
            print(e.with_traceback())

    def validate_universal_row_data(self, row):
        self._test_text_components(row)
        self._test_country_id(row)
        self._test_party_id(row)
        self._test_party(row)
        self._test_role(row)
        self._test_years_active(row)
        self._test_committees(row)
        self._test_phone_numbers(row)
        self._test_addresses(row)
        self._test_email(row)
        self._test_birthday(row)
        self._test_seniority(row)
        self._test_occupation(row)
        self._test_education(row)

    def _test_text_components(self, row):
        if type(row.source_id) != str:
            self.raise_exception("source_id must be of type str")
        if type(row.most_recent_term_id) != str:
            self.raise_exception("most_recent_term_id must be of type str")
        if type(row.source_url) != str:
            self.raise_exception("source_url must be of type str")
        if type(row.name_full) != str:
            self.raise_exception("name_full must be of type str")
        if type(row.name_last) != str:
            self.raise_exception("name_last must be of type str")
        if type(row.name_first) != str:
            self.raise_exception("source_url must be of type str")
        if type(row.name_middle) != str:
            self.raise_exception("name_middle must be of type str")
        if type(row.name_suffix) != str:
            self.raise_exception("name_suffix must be of type str")
        if type(row.country) != str:
            self.raise_exception("country data must be of type str")
        if type(row.military_experience) != str:
            self.raise_exception("military_experience data must be of type str")

    def _test_country_id(self, row):
        if type(row.country_id) != int:
            self.raise_exception("country_id must be of type int")

    def _test_party_id(self, row):
        if type(row.party_id) != int:
            self.raise_exception("party_id must of be of type int")

    def _test_party(self, row):
        if type(row.country) != str:
            self.raise_exception("party data must be of type str -- use full name of party")

    def _test_role(self, row):
        if type(row.role) != str:
            self.raise_exception("role data must be of type str")

    def _test_years_active(self, row):
        ya = row.years_active
        if type(ya) != list:
            self.raise_exception("years_active data must be of type list")
        for year in ya:
            if type(year) != int:
                self.raise_exception("years in years_active must be of format YYYY of type int")
        
    def _test_committees(self, row):
        if type(row.committees) != list:
            self.raise_exception("committees data must be a list of dicts")
        for committee in row.committees:
            if type(committee) != dict:
                self.raise_exception("committees data must be a list of dicts")
            if type(committee['role']) != str or not committee['role']:
                self.raise_exception("committees data must have valid role information as str")
            if type(committee['committee']) != str or not committee['committee']:
                self.raise_exception("committees data must have valid committee name as str")

    def _test_phone_numbers(self, row):
        if type(row.phone_numbers) != list:
            self.raise_exception("phone_numbers data must be a list of dicts")
        for numbers in row.phone_numbers:
            if type(numbers) != dict:
                self.raise_exception("phone_numbers data must be a list of dicts")
            if type(numbers['office']) != str or not numbers['office']:
                self.raise_exception("phone_numbers data must have a valid office information as str")
            if type(numbers['number']) != str or not numbers['number'] or not re.match(r'[0-9]{3}-[0-9]{3}-[0-9]{4}', numbers['number']):
                self.raise_exception("phone_numbers data must have a valid number information as str in format ###-###-####")

    def _test_addresses(self, row):
        if type(row.addresses) != list:
            self.raise_exception("addresses data must be a list of dicts")
        for address in row.addresses:
            if type(address) != dict:
                self.raise_exception("addresses data must be a list of dicts")
            if type(address['location']) != str or not address['location']:
                self.raise_exception("addresses data must have a valid location information as str")
            if type(address['address']) != str or not address['address']:
                self.raise_exception("addresses data must have a valid address information as str")

    def _test_email(self, row):
        if type(row.email) != str:
            self.raise_exception("email data must be of type str")
        if '@' not in row.email:
            self.raise_exception("email data must be be in proper format (ex. joeburns@cagov.com)")
    
    def _test_birthday(self, row):
        if row.birthday and type(row.birthday) != type(datetime):
            self.raise_exception(f"birthday data must be of type date - current: {type(row.birthday)}")

    def _test_seniority(self, row):
        if type(row.seniority) != int:
            self.raise_exception("seniority data must be of type int")
    
    def _test_occupation(self, row):
        if type(row.occupation) != list:
            self.raise_exception("occupation data must be a list of str")
        for element in row.occupation:
            if type(element) != str:
                self.raise_exception("occupation data must be a list of str")
        
    def _test_education(self, row):
        if type(row.education) != list:
            self.raise_exception(f"education data must be list of dicts")
        for element in row.education:
            if type(element['level']) != str:
                self.raise_exception("education data must have valid level information")
            if type(element['field']) != str:
                self.raise_exception("education data must have valid field information")
            if type(element['school']) != str:
                self .raise_exception("education data must have valid school information")

class USLegislatorRowValidator(LegislatorRowValidator):
    def validate_row(self, row):
        self.validate_universal_row_data(row)
        self._test_state_id(row)
        self._test_state(row)
        self._test_areas_served(row)
        self._test_district(row)

    def _test_state_id(self, row):
        if type(row.state_id) != int:
            self.raise_exception("state_id must be of type int")

    def _test_state(self, row):
        if type(row.state) != str:
            self.raise_exception("state must be of type str")
    
    def _test_areas_served(self, row):
        if type(row.areas_served) != list:
            self.raise_exception("areas_served must be a list of str")
        for area in row.areas_served:
            if type(area) != str:
                self.raise_exception("areas_served must be a list of str")
    
    def _test_district(self, row):
        if type(row.district) != str:
            self.raise_exception("district must be of type str")

class CALegislatorRowValidator(LegislatorRowValidator):
    def validate_row(self, row):
        self.validate_universal_row_data(row)
        self._test_province_territory_id(row)
        self._test_province_territory(row)
        self._test_region(row)
        self._test_riding(row)

    def _test_province_territory_id(self, row):
        if type(row.province_territory_id) != int:
            self.raise_exception("province_territory_id must be of type int")

    def _test_province_territory(self, row):
        if type(row.province_territory) != str:
            self.raise_exception("province_territory must be of type str")

    def _test_region(self, row):
        if type(row.region) != str:
            self.raise_exception("region data must be of type str")

    def _test_riding(self, row):
        if type(row.riding) != str:
            self.raise_exception("riding data must be of type str")

class CAFedLegislatorRowValidator(CALegislatorRowValidator):
    def validate_row(self, row):
        self.validate_universal_row_data(row)
        self._test_province_territory_id(row)
        self._test_province_territory(row)
        self._test_region(row)
        self._test_riding(row)

    def _test_offices_roles_as_mp(self, row):
        data = row.offices_roles_as_mp
        if data and type(data) != list:
            self.raise_exception("offices_roles_as_mp data must a list of str")
        
        for element in data:
            if data and type(element) != str:
                self.raise_exception("offices_roles_as_mp data must a list of str")

    def _test_parl_assoc_interparl_groups(self, row):
        data = row.parl_assoc_interparl_groups
        if data and type(data) != list:
            self.raise_exception("parl_assoc_interparl_groups data must a list of dicts")
        
        for element in data:
            if data and type(element) != dict:
                self.raise_exception("parl_assoc_interparl_groups data must a list of str")
            if data and (not element['role'] or type(element['role']) != str):
                self.raise_exception("parl_assoc_interparl_groups data must have valid role information")
            if data and (not element['title'] or type(element['title']) != str):
                self.raise_exception("parl_assoc_interparl_groups data must have valid title information")
            if data and (not element['organization'] or type(element['organization']) != str):
                self.raise_exception("parl_assoc_interparl_groups data must have valid organization information")


class Validator:
    validators = {
        type(CALegislationRow()): CALegislationRowValidator(),
        type(USLegislationRow()): USLegislationRowValidator(),
        type(CAFedLegislationRow()): CAFedLegislationRowValidator(),
        type(CALegislatorRow()): CALegislatorRowValidator(),
        type(USLegislatorRow()): USLegislatorRowValidator(),
        type(CAFedLegislatorRow()): CAFedLegislatorRowValidator()
    }

    def __init__(self, row_type):
        self.validator = self.assign_validator(row_type)

    def assign_validator(self, row_type) -> RowValidator:
        return self.validators.get(row_type)

    def validate_row(self, row):
        self.validator.validate_rows(row)

    def validate_rows(self, rows):
        self.validator.validate_rows(rows)
