#!/usr/bin/env python
"""
    reads in the obfuscated_sorted_ecfs.log.gz
    spans up the fat map that contains infos of each obj_id found.
    create a file that contains an entry for each obj_d life_cycle
    each line has some tags and the get requests since the first upload (or since first get if no initial put exists)
    no_put;(tmp)|10;100;2440;50500

"""
import gzip
from collections import defaultdict
import sys
import time
import os
import resource
import hashlib
import json
import math

# add ecmwf_utils to python path
util_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
print (util_path)
sys.path.append(util_path)

from ecmwf_util import Stats

MONITOR_LINES = 100000

# GET = 0
# PUT = 1
# DEL = 2
# SIZE = 3

GET = 'g'
PUT = 'p'
DEL = 'd'
SIZE = 's'

KB = 1024
MB = KB * 1024
GB = MB * 1024
TB = GB * 1024
PB = TB * 1024

ECMWF_GROUPS = [
    ("Tiny", 0, 512*KB),
    ("Small", 512*KB, 1*MB),
    ("Medium", 1*MB, 8*MB),
    ("Large", 8*MB, 48*MB),
    ("Huge", 48*MB, 1*GB),
    ("Enormous", 1*GB, 100000*PB)
]

def get_ecmwf_file_size_group_name(bytes):
    for x in ECMWF_GROUPS:
        if x[1] <= bytes and x[2] >= bytes:
            return x[0]
    return "Unknown"


m = defaultdict(list)

class Timer():
    def __init__(self, s):
        self.s = s

    def __enter__(self):
        self.start = time.time()

    def __exit__(self, *args):
        print ("%s: %fs" % (self.s, (time.time() - self.start)))



def get_md5_tmp(path):
    """
        return the md5 sum of the path.
        if the path is a temporary directory, the md5 is prefixed with "tmp_"
    """
    m = hashlib.md5()
    m.update(path)

    if path.__contains__("temp") or path.__contains__("tmp"):
        return "tmp_%r" % m.hexdigest()

    return m.hexdigest()


def parse_file(source_file):

    """
        the lines
    """
    START_TIME      = 0
    USER_ID         = 1
    HOST_ID         = 2
    PROCESS_ID      = 3
    REQUEST         = 4
    PARAMS          = 5
    FILE_SIZE       = 6
    EXECUTION_TIME  = 7
    ADDITIONAL_INFO = 8


    ## prepare the fat dict: m
    with Timer("Preparing fat dict"):
        with gzip.open(source_file, 'r') as sf:
            plines = 0
            t = time.time()
            for line in sf:
                plines += 1
                if plines % MONITOR_LINES == 0:
                    print ("processed lines: %d, stored elems: %r mem: %rMB, lines/s: %r" %
                     (plines,
                      len(m),
                      float(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss) / 1024,
                      int(MONITOR_LINES / (time.time() - t))
                     )
                    )
                    t = time.time()

                # if plines == 1000000:
                #     break

                elems = line.split('|')

                if len(elems) <= FILE_SIZE:
                    continue

                if not elems[START_TIME].isdigit():
                    print ("bad line: %s" % line)
                    continue

                ts = int(elems[START_TIME])

                size = elems[FILE_SIZE]
                if size.isdigit():
                    size = int(size)
                else:
                    size = 0
                # request_size_group_name = stats.get_file_size_group_name(stats.get_file_size_group(size))

                # if get_md5(elems[PARAMS].strip(), hexdigest=False) == "ec6380a5e32a0578c1c12b63c8491828":
                #     print (elems)

                if elems[REQUEST] == 'GET':
                    obj_id = get_md5_tmp(elems[PARAMS].strip())
                    if obj_id not in m or len(m[obj_id]) == 0:
                        m[obj_id].append((SIZE, size))
                        m[obj_id].append((GET, ts))
                    else:
                        m[obj_id].append((GET, ts))

                elif elems[REQUEST] == 'PUT':
                    obj_id = get_md5_tmp(elems[PARAMS].strip())

                    # a put request either creates a new file or overwrites a previous file.
                    m[obj_id].append((SIZE, size))
                    m[obj_id].append((PUT, ts))

                elif elems[REQUEST] == 'DEL':
                    obj_id = get_md5_tmp(elems[PARAMS].strip())
                    m[obj_id].append((DEL, ts))

                elif elems[REQUEST] == 'RENAME':

                    from_obj_id = get_md5_tmp(elems[PARAMS].split(" ")[0].strip())
                    to_obj_id = get_md5_tmp(elems[PARAMS].split(" ")[1].strip())

                    if from_obj_id not in m:
                        continue

                    #only move the last copy of the file.
                    mtuples = list()
                    for x in reversed(m[from_obj_id]):
                        if x[0] == GET:
                            mtuples.append(x)
                        elif x[0] == PUT:
                            mtuples.append(x)
                        elif x[0] == SIZE:
                            mtuples.append(x)
                            break
                        else:
                            break

                    # remove old entries
                    for y in mtuples:
                        m[from_obj_id].pop()
                    if len(m[from_obj_id]) == 0:
                        m.pop(from_obj_id)

                    # sort in the new entries
                    mtuples.reverse()

                    if to_obj_id not in m:
                        m[to_obj_id] = list()
                    m[to_obj_id] += mtuples
                else:
                    # print("bad line: %s" % (line))
                    pass

stats = defaultdict(int)

def analyze_fat_map(out_file):
    """
    analyze the fat dict m:
    
    The tuples element contains the full lifes of a file path

    A complete Case is:
    SIZE PUT GET GET DELETE

    But incomplete lists can be found as well.

    a) SIZE GET GET DELETE
    b) DEL
    c) SIZE GET GET GET
    d) SIZE PUT DEL
    e) SIZE PUT

    and multiple different combinations of them.
"""

    for obj_id, tuples in m.items():
        if len(tuples) == 0:
            continue

        life_cycle = list()

        for x in tuples:
            if x[0] == SIZE:
                life_cycle = list()
                life_cycle.append(x)
            if x[0] == PUT:
                # check if a previous put exists
                for y in life_cycle:
                    if y[0] == PUT:
                        write_life_cycle(obj_id, life_cycle, out_file)
                        life_cycle = list()
                        break
                life_cycle.append(x)
            elif x[0] == GET:
                life_cycle.append(x)
            elif x[0] == DEL:
                life_cycle.append(x)
                write_life_cycle(obj_id, life_cycle, out_file)
                life_cycle = list()
        
        if len(life_cycle) > 0:
            write_life_cycle(obj_id, life_cycle, out_file)


def write_life_cycle(obj_id, life_cycle, out_file):

    is_tmp = False
    if obj_id.startswith("tmp_"):
        is_tmp = True

    # print("life_cycle: %r" % (life_cycle))
    stats["num_life_cycles"] += 1
    accesses = 0
    file_size = 0
    put_ts = 0
    del_ts = 0
    for x in life_cycle:
        if x[0] == GET:
            accesses += 1
        elif x[0] == PUT:
            put_ts = x[1]
        elif x[0] == SIZE:
            file_size = x[1]
        elif x[0] == DEL:
            del_ts = x[1]

    # calc time between accesses.
    if accesses >= 1:
        stats["life_cycles_with_accesses"] += 1
        access_times_since_put = list()
        if put_ts > 0:
            for x in life_cycle:
                if x[0] == GET:
                    access_times_since_put.append(x[1] - put_ts)
        else:
            stats["life_cycles_without_put"] += 1
            tmp_ts = 0
            for x in life_cycle:
                if x[0] == GET:
                    if tmp_ts == 0:
                        tmp_ts = x[1]
                    else:
                        access_times_since_put.append(x[1] - put_ts)

        if len(access_times_since_put) > 0:

            tags = get_ecmwf_file_size_group_name(file_size)
            if put_ts == 0:
                tags += ";no_put"
            if is_tmp:
                tags += ";tmp"

            out_file.write("%s|%s\n" % (tags, ";".join([str(y) for y in access_times_since_put])))

    else:
        stats["life_cycles_with_no_accesses"] += 1

if __name__ == "__main__":

    print ("Version 002")

    if len(sys.argv) == 1:
        print ("usage: trace.gz results.list.txt")
        print ("reads all the trace files and writes a pickled file with source access analysis.")
        sys.exit(1)

    source_file = os.path.abspath(sys.argv[1])

    if not os.path.exists(source_file):
        print("target file: %s does not exist" % source_file)
        sys.exit(1)


    target_file = os.path.abspath(sys.argv[2])
    if os.path.exists(target_file):
        print("Warning: target file: %s already exist" % target_file)

    parse_file(source_file)

    with gzip.open(target_file, 'wb') as out_file:
        analyze_fat_map(out_file)

    print(json.dumps(stats, indent=4, sort_keys=True))
