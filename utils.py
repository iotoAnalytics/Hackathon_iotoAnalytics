from datetime import date, datetime
import json


"""
Assorted utility functions and classes used by Legislation and Legislator scraper utils.
Author: Justin Tendeck
"""


class DotDict(dict):
    """
    Allows a dictionary to be accessible using the dot operator.
    Eg: Instead of: name = my_dict['name'], use: name = my_dict.name

    Author: Daniel Farrel
    Source: https://stackoverflow.com/questions/2352181/how-to-use-a-dot-to-access-members-of-dictionary
    """
    __getattr__ = dict.get
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__



def json_serial(obj):
    """
    Serializes objects so they may be placed into JSON format.

    Author: Jay Taylor
    Source: https://stackoverflow.com/questions/11875770/how-to-overcome-datetime-datetime-not-json-serializable
    """
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))
