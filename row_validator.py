import abc
import re
from rows import *

class RowValidator:
    @abc.abstractmethod
    def validate_rows(self, rows):
        pass

    def raise_exception(self, message):
        raise Exception(message)

class LegislationRowValidator(RowValidator):
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
        if not re.match(r'[0-9]{4}-[0-9]{2}-[0-9]{2}', date):
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


class USLegislationRowValidator(LegislationRowValidator):
    def validate_rows(self, row):
        self._test_goverlytics_id(row)
        self._test_text_components(row)
        self._test_date_introduced(row)
        self._test_source_url(row)
        self._test_committees(row)
        self._test_principal_sponsor(row)
        self._test_sponsors(row)
        self._test_cosponsors(row)
        self._test_actions(row)


class CALegislationRowValidator(LegislationRowValidator):
    pass

class CAFedLegislationRowValidator(CALegislationRowValidator):
    pass

class LegislatorRowValidator(RowValidator):
    pass

class USLegislatorRowValidator(LegislatorRowValidator):
    pass

class CALegislatorRowValidator(LegislatorRowValidator):
    pass

class CAFedLegislatorRowValidator(CALegislatorRowValidator):
    pass


class Validator:
    validators = {
        type(CALegislationRow()): CALegislationRowValidator(),
        type(USLegislationRow()): USLegislationRowValidator(),
        type(CAFedLegislationRow()): CAFedLegislationRowValidator(),
        type(CALegislatorRow()): CALegislatorRowValidator(),
        type(USLegislatorRow()): USLegislatorRowValidator(),
        type(CAFedLegislatorRow()): CAFedLegislatorRowValidator()
    }

    def __init__(self, row):
        self.row = row
        validator = self.assign_validator()
        validator.validate_rows(self.row)
        print("All validation passed!")

    def assign_validator(self) -> RowValidator:
        row_type = type(self.row)
        return self.validators.get(row_type)

