import json
import logging
import datetime
import pytz


logger = logging.getLogger(__name__)


def print_json(jobj):
    print(json.dumps(jobj, indent=2))


# Based on https://wiki.python.org/moin/SortingListsOfDictionaries and others
def multikeysort(items, columns):
    from operator import itemgetter
    from functools import cmp_to_key

    comparers = [((itemgetter(col[1:].strip()), -1) if col.startswith('-') else (itemgetter(col.strip()), 1)) for col in columns]

    # recreate cmp from python2
    def cmp(a, b):
        return (a > b) - (a < b)

    def comparer(left, right):
        for fn, mult in comparers:
            result = cmp(fn(left), fn(right))
            if result:
                return mult * result
        else:
            return 0
    return sorted(items, key=cmp_to_key(comparer))


def array_to_dict(rows, key="openstack_id"):
    """ Convert a list of dicts into a dict based on key"""
    result = {}
    for row in rows:
        if key in row:
            key_value = row.pop(key)
            result[key_value] = row
        else:
            logger.debug('%s is not in %s', key, row)

    return result


def repack(dict_obj, key_map, rm_keys=[]):
    """ Repackage a dict object by renaming and removing keys"""
    for k, v in key_map.items():
        dict_obj[v] = dict_obj.pop(k)
    for k in rm_keys:
        del dict_obj[k]

    return dict_obj


def parse_date_string(date_string, tz='Australia/Adelaide', fmt='%Y%m%d'):
    """Parse a date string of a given timezone and return timestamp of it
    at the begining of the day:

    20170101 (Sunday 1 January  00:00:00 ACDT 2017) -> 1483191000

    date_string: string, e.g. 20170101 or 2017-01-01
    tz: string, official time zone string, default is Australia/Adelaide
    fmt: string, the format of date_string, default is %Y%m%d
    return int timestamp
    """
    target_tz = pytz.timezone(tz)
    local_date = datetime.datetime.strptime(date_string, fmt)
    target_date = target_tz.localize(local_date)
    return int(target_date.timestamp())
