from datetime import date, datetime

# DotDic By Daniel Farrel
# https://stackoverflow.com/questions/2352181/how-to-use-a-dot-to-access-members-of-dictionary
class DotDict(dict):
    __getattr__ = dict.get
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__

def json_serial(self, obj):
    """
    Serializes date/datetime object. This is used to convert date and datetime objects to
    a format that can be digested by the database.
    """
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))