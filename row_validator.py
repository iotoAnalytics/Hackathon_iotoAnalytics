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
        if type(row.source_id) != str or type(row.source_id) == None:
            self.raise_exception("source_id must be a string.")
        if type(row.bill_name) != str or type(row.bill_name) == None:
            self.raise_exception("bill_name must be a string.")
        if type(row.session) != str or type(row.session) == None:
            self.raise_exception("session must be a string.")
        if type(row.chamber_origin) != str or type(row.chamber_origin) == None:
            self.raise_exception("chamber_origin must be a string.")
        if type(row.bill_type) != str or type(row.bill_type) == None:
            self.raise_exception("bill_type must be a string.")
        if type(row.bill_title) != str or type(row.bill_title) == None:
            self.raise_exception("bill_title must be a string.")
        if type(row.current_status) != str or type(row.current_status) == None:
            self.raise_exception("current_status must be a string.")

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

class USLegislationRowValidator(LegislationRowValidator):
    def validate_rows(self, row):
        self._test_goverlytics_id(row)
        self._test_text_components(row)
        self._test_date_introduced(row)
        self._test_source_url(row)
        self._test_committees(row)


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

