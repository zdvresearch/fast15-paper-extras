#!/usr/bin/env python

__author__ = 'meatz'

import os
import sys
import gzip
import json
import hashlib
import resource
import datetime



#from guppy import hpy

from CacheBuckets import CacheBuckets
from StorageSystem import StorageSystem

START_TIME      = 0
USER_ID         = 1
HOST_ID         = 2
PROCESS_ID      = 3
REQUEST         = 4
PARAMS          = 5
FILE_SIZE       = 6
EXECUTION_TIME  = 7
ADDITIONAL_INFO = 8


KB = 1024
MB = 1024 * KB
GB = 1024 * MB
TB = 1024 * GB


def get_md5(s, hexdigest=False):
    # return s
    x = s.encode('utf-8')
    m = hashlib.md5()
    m.update(x)
    if hexdigest:
        return m.hexdigest()
    else:
        return m.digest()

def get_config():
    return """
{
  "buckets": {
    "big": [
      1000,
      10000,
      1099511627776,
      "LRUCache"
    ],
    "small": [
      0,
      1000,
      53687091200,
      "LRUCache"
    ]
  },
  "cache_config": "A_LRUCache",
  "name": "foo-A_LRUCache",
  "trace": "/home/meatz/ecmwf_traces/foo.gz"
}
"""
#     return """
# {
#   "buckets": {
#     "big": [
#       16777216,
#       536870912,
#       1099511627776,
#       "LRUCache"
#     ],
#     "enormous": [
#       536870912,
#       9223372036854775807,
#       2199023255552,
#       "LRUCache"
#     ],
#     "small": [
#       262144,
#       16777216,
#       53687091200,
#       "LRUCache"
#     ],
#     "tiny": [
#       0,
#       262144,
#       10737418240,
#       "LRUCache"
#     ]
#   },
#   "cache_config": "A_LRUCache",
#   "name": "foo-A_LRUCache",
#   "trace": "/home/meatz/ecmwf_traces/foo.gz"
# }
# """


def get_lines():
    lines = []
    lines.append("1328284036|a|b|292384|PUT|/a.grb|500|0|-")
    lines.append("1328289395|a|b|389894|GET|/a.grb|2000|1|")
    lines.append("1328289399|a|b|246088|PUT|/a.grb|3000|3|-")

    for x in lines:
        yield x


def dump(s):
    print(json.dumps(s, indent=4,sort_keys=True))

def main():

    config = json.loads(get_config())

    cache_buckets = config["buckets"]
    # for bucket_name, bucket_config in cache_buckets.items():
    #     print (bucket_name, bucket_config)

    cache = CacheBuckets(cache_buckets)
    storage = StorageSystem(cache)

    for line in get_lines():
        elems = str(line).split('|')
        epoch = int(elems[START_TIME])
        next_line = int(elems[NEXT_LINE])
        obj_id = get_md5(elems[PARAMS].strip(), hexdigest=True)

    #
        if elems[REQUEST] == 'GET':
            if elems[FILE_SIZE].isdigit():
                size = int(elems[FILE_SIZE])
                uid = elems[USER_ID]
                storage.get_object(obj_id, uid, size, epoch, next_line)
            else:
                print("bad line: %s" % (line))
        elif elems[REQUEST] == 'PUT':
            if elems[FILE_SIZE].isdigit():
                uid = elems[USER_ID]
                size = int(elems[FILE_SIZE])
                storage.put_object(obj_id, uid, size, epoch, next_line)
            else:
                print("bad line: %s" % (line))
        elif elems[REQUEST] == 'DEL':
            uid = elems[USER_ID]
            storage.del_object(obj_id, uid, epoch)
        elif elems[REQUEST] == 'RENAME':
            uid = elems[USER_ID]
            from_obj_id = get_md5(elems[PARAMS].split(" ")[0].strip(), hexdigest=True)
            to_obj_id = get_md5(elems[PARAMS].split(" ")[1].strip(), hexdigest=True)
            # print ("rename %s \n\t --> %s" % (from_obj_id, to_obj_id))
            storage.rename_object(from_obj_id, to_obj_id)
        else:
            print("bad line: %s" % (line))

    dump(storage.get_stats_total())

    return 0

if __name__ == "__main__":
    sys.exit(main())
