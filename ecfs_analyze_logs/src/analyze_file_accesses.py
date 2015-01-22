#!/usr/bin/env python
"""
    works on the filtered/merged/obfuscated files.
    for every day, get stats for number of files put/get/del, amount of bytes written / read / deleted.
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


m = defaultdict(list)
stats = Stats.Stats()


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
    return None

def get_file_size_group_name(group_id):
        ffrom = 2**group_id
        fto = 2**(group_id+1)
        return "%dKB-%dKB" % (ffrom, fto)


class Timer():
    def __init__(self, s):
        self.s = s

    def __enter__(self):
        self.start = time.time()

    def __exit__(self, *args):
        print ("%s: %fs" % (self.s, (time.time() - self.start)))


def get_file_size_group(bytes):
    """
    We are interested in the file size/request distribution.
    The log2 of the bytes will group the size into
    :param bytes:
    :return:
    """
    x = int(bytes)
    if x < 1024:  # less than 1KB
        return 0
    return int(math.log((x/1024), 2))


def get_md5(s, hexdigest=False):
    # return s
    m = hashlib.md5()
    m.update(s)
    if hexdigest:
        return m.hexdigest()
    else:
        return m.digest()


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
                    size =  0
                # request_size_group_name = stats.get_file_size_group_name(stats.get_file_size_group(size))

                # if get_md5(elems[PARAMS].strip(), hexdigest=False) == "ec6380a5e32a0578c1c12b63c8491828":
                #     print (elems)

                if elems[REQUEST] == 'GET':
                    obj_id = get_md5(elems[PARAMS].strip(), hexdigest=False)
                    if obj_id not in m or len(m[obj_id]) == 0:
                        m[obj_id].append((SIZE, size))
                        m[obj_id].append((GET, ts))
                    else:
                        m[obj_id].append((GET, ts))

                elif elems[REQUEST] == 'PUT':
                    obj_id = get_md5(elems[PARAMS].strip(), hexdigest=False)

                    # a put request either creates a new file or overwrites a previous file.
                    m[obj_id].append((SIZE, size))
                    m[obj_id].append((PUT, ts))

                elif elems[REQUEST] == 'DEL':
                    obj_id = get_md5(elems[PARAMS].strip(), hexdigest=False)
                    m[obj_id].append((DEL, ts))

                elif elems[REQUEST] == 'RENAME':

                    from_obj_id = get_md5(elems[PARAMS].split(" ")[0].strip())
                    to_obj_id = get_md5(elems[PARAMS].split(" ")[1].strip())

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


results = dict()
results["access_count"] = defaultdict(dict)
results["life_times"] = defaultdict(dict)
results["total_known_objects"] = 0
results["options"] = dict()
results["options"]["file_size_groups"] = dict()

for k in ECMWF_GROUPS:
    results["options"]["file_size_groups"][k[0]] = dict()
    results["options"]["file_size_groups"][k[0]]["from"] = k[1]
    results["options"]["file_size_groups"][k[0]]["to"] = k[2]


def analyze_fat_map():
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
                for y in life_cycle:
                    if y[0] == PUT:
                        count_file_lifecycle(obj_id, life_cycle, out_file)
                        life_cycle = list()
                        break
                life_cycle.append(x)
            elif x[0] == GET:
                life_cycle.append(x)
            elif x[0] == DEL:
                life_cycle.append(x)
                count_file_lifecycle(life_cycle)
                life_cycle = list()
        
        if len(life_cycle) > 0:
            count_file_lifecycle(life_cycle)

    results["stats"] = stats.to_dict()


def count_file_lifecycle(life_cycle):
    results["total_known_objects"] += 1

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

    if accesses > 1:
        atimes = list()
        for x in range(len(life_cycle)-1):
            if life_cycle[x][0] == GET:
                if life_cycle[x+1][0] == GET:
                    atimes.append(life_cycle[x+1][1] - life_cycle[x][1])

        for xx in ['total', get_ecmwf_file_size_group_name(file_size)]:
            for a in atimes:
                stats.updateStatsTotal(("file_time_between_gets", xx), a)




    for xx in ['total', get_ecmwf_file_size_group_name(file_size)]:
        if not accesses in results["access_count"][xx]:
            results["access_count"][xx][accesses] = 1
        else:
            results["access_count"][xx][accesses] += 1

        stats.updateStatsTotal(("file_size", xx), file_size)

    if put_ts > 0 and del_ts > 0:
        file_lifetime = del_ts - put_ts
        for xx in ['total', get_ecmwf_file_size_group_name(file_size)]:
            stats.updateStatsTotal(("file_lifetime", xx), file_lifetime)


def write_results(target_file):
    with open(target_file, 'w') as tf:
        json.dump(results, tf, indent=2, sort_keys=True)
    print(json.dumps(results, indent=2, sort_keys=True))


if __name__ == "__main__":

    if len(sys.argv) == 1:
        print ("usage: trace.gz results.json")
        print ("reads all the trace files and writes a pickled file with source access analysis.")
        sys.exit(1)

    source_file = os.path.abspath(sys.argv[1])

    if not os.path.exists(source_file):
        print("target file: %s does not exist" % source_file)
        sys.exit(1)


    target_file = os.path.abspath(sys.argv[2])
    if os.path.exists(target_file):
        print("Warning: target file: %s already exist" % target_file)
    #     sys.exit(1)

    parse_file(source_file)
    analyze_fat_map()
    write_results(target_file)