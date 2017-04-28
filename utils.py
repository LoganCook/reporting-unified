import json
import logging


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
