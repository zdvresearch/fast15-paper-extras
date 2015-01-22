#!/usr/bin/env python
"""
    works on the filtered/merged/obfuscated files.
    for every day, get stats for number of files put/get/del, amount of bytes written / read / deleted.
"""
import gzip
import json
import sys
import time
import os
import resource
import datetime
import math

MONITOR_LINES = 100000

KB = 1024
MB = KB * 1024
GB = MB * 1024
TB = GB * 1024

ZDV_GROUPS = [
    ("Tiny", 0, 128*KB),
    ("Small", 128*KB, 1*MB),
    ("Medium", 1*MB, 8*MB),
    ("Large", 8*MB, 128*MB),
    ("Huge", 128*MB, 1*GB),
    ("Enormous", 1*GB, 100000*TB)
]

ECMWF_GROUPS = [
    ("Tiny", 0, 512*KB),
    ("Small", 512*KB, 1*MB),
    ("Medium", 1*MB, 8*MB),
    ("Large", 8*MB, 48*MB),
    ("Huge", 48*MB, 1*GB),
    ("Enormous", 1*GB, 100000*TB)
]

class Stats:
    def __init__(self):
        self.s = dict()

    def update(self, description, value, ts):

        dt = datetime.datetime.utcfromtimestamp(ts)

        for t in ["totals", dt.strftime("%Y"), dt.strftime("%Y-%m"), dt.strftime("%Y-%m-%d")]:
            if t not in self.s:
                self.s[t] = StatsTree()
            self.s[t].update(description, value)

    def to_dict(self):
        r = dict()

        for k, v in self.s.items():
            # assert all v are StatsTrees
            r[k] = v.flatten()
        return r

    def get_file_size_group_name(self, group_id):
        ffrom = 2**group_id
        fto = 2**(group_id+1)
        return "%dKB-%dKB" % (ffrom, fto)

    def get_file_size_group(self, bytes):
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

    def get_ecmwf_file_size_group_name(self, bytes):
        for x in ECMWF_GROUPS:
            if x[1] <= bytes and x[2] >= bytes:
                return x[0]
        return None

    def get_zdv_file_size_group_name(self, bytes):
        for x in ZDV_GROUPS:
            if x[1] <= bytes and x[2] >= bytes:
                return x[0]
        return None

class StatsTree:
    def __init__(self):
        self.stats = dict()

    def update(self, description, value):
        p = self.stats
        depth = len(description)
        for d in range(depth):
            if d == depth -1:
                if description[d] not in p:
                    p[description[d]] = StatsElem()
                p[description[d]].update(value)
            else:
                if description[d] not in p:
                    p[description[d]] = dict()
                p = p[description[d]]

    def flatten(self):
        """
            return a dict + native datatypes only representation of this StatsTree
        """
        d = dict()
        for k, v in self.stats.items():
            d[k] = self.to_dict(v)
        return d


    def to_dict(self, sd):
        """
            internal helper to recursivly dictify all childs
        """
        if isinstance(sd, StatsElem):
            return sd.to_dict()

        d = dict()
        for k, v in sd.items():
            d[k] = self.to_dict(v)
        return d


class StatsElem:
    def __init__(self):
        self.min = sys.maxsize
        self.max = 0
        self.count = 0
        self.sum = 0

    def update(self, value):
        self.min = min (self.min, value)
        self.max = max(self.max, value)
        self.count += 1
        self.sum += value

    def to_dict(self):
        s = dict()
        if self.count > 0:
            s["min"] = self.min
            s["max"] = self.max
            s["count"] = self.count
            s["avg"] = float(self.sum) / self.count
            s["sum"] = self.sum
        return s

    def __str__(self):
        return json.dumps(self.get_summary(), indent=2, sort_keys=True)


def generate_stats(stats, trace_file_path):

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

    with gzip.open(trace_file_path, 'r') as source_file:
        plines = 0
        t = time.time()
        for line in source_file:
            plines += 1
            if plines % MONITOR_LINES == 0:
                print ("processed lines: %d, mem: %rMB, lines/s: %r" %
                 (plines,
                  resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024 / 1024,
                  int(MONITOR_LINES / (time.time() - t))
                 )
                )
                t = time.time()

            elems = line.split('|')

            if not elems[START_TIME].isdigit():
                print "bad line: %s" % line
                continue

            ts = int(elems[START_TIME])

            size = elems[FILE_SIZE]
            if size.isdigit():
                size = int(size)
            else:
                size =  0

            request_size_group_name = stats.get_file_size_group_name(stats.get_file_size_group(size))
            ecmwf_request_size_group_name = stats.get_ecmwf_file_size_group_name(size)
            zdv_request_size_group_name = stats.get_zdv_file_size_group_name(size)


            exectime = elems[EXECUTION_TIME]
            if exectime.isdigit():
                exectime = int(exectime)
            else:
                exectime = 0

            if elems[REQUEST] == 'GET':
                from_tape = elems[ADDITIONAL_INFO].__contains__('tape')

                if not from_tape:
                    # stats.update(("get", "by_database", db, x), v, request["startdate"])
                    stats.update(("get", "bytes_disk"), size, ts)
                    stats.update(("get", "requests_disk"), 1, ts)
                    stats.update(("get", "execution_time_disk"), exectime, ts)

                    stats.update(("get", "by_size_disk", "num_requests", "log", request_size_group_name), 1, ts)
                    stats.update(("get", "by_size_disk", "size", "log", request_size_group_name), size, ts)

                    stats.update(("get", "by_size_disk", "num_requests", "ecmwf", ecmwf_request_size_group_name), 1, ts)
                    stats.update(("get", "by_size_disk", "size", "ecmwf", ecmwf_request_size_group_name), size, ts)

                    stats.update(("get", "by_size_disk", "num_requests", "zdv", zdv_request_size_group_name), 1, ts)
                    stats.update(("get", "by_size_disk", "size", "zdv", zdv_request_size_group_name), size, ts)
                else:
                    stats.update(("get", "bytes_tape"), size, ts)
                    stats.update(("get", "requests_tape"), 1, ts)
                    stats.update(("get", "execution_time_tape"), exectime, ts)

                    stats.update(("get", "by_size_tape", "num_requests", "log", request_size_group_name), 1, ts)
                    stats.update(("get", "by_size_tape", "size", "log", request_size_group_name), size, ts)
                    stats.update(("get", "by_size_tape", "num_requests", "ecmwf", ecmwf_request_size_group_name), 1, ts)
                    stats.update(("get", "by_size_tape", "size", "ecmwf", ecmwf_request_size_group_name), size, ts)

                stats.update(("get", "bytes_total"), size, ts)
                stats.update(("get", "requests_total"), 1, ts)
                stats.update(("get", "by_size_total", "num_requests", "log", request_size_group_name), 1, ts)
                stats.update(("get", "by_size_total", "size", "log", request_size_group_name), size, ts)

                stats.update(("get", "by_size_total", "num_requests", "ecmwf", ecmwf_request_size_group_name), 1, ts)
                stats.update(("get", "by_size_total", "size", "ecmwf", ecmwf_request_size_group_name), size, ts)

                stats.update(("get", "by_size_total", "num_requests", "zdv", zdv_request_size_group_name), 1, ts)
                stats.update(("get", "by_size_total", "size", "zdv", zdv_request_size_group_name), size, ts)

            elif elems[REQUEST] == 'PUT':
                if not elems[FILE_SIZE].isdigit():
                    print ("bad line: %s" % line)
                    continue

                # request_size_group_name = stats.get_file_size_group_name(stats.get_file_size_group(size))

                stats.update(("put", "bytes"), size, ts)
                stats.update(("put", "requests"), 1, ts)
                stats.update(("put", "execution_time"), exectime, ts)
                stats.update(("put", "by_size", "num_requests", "log", request_size_group_name), 1, ts)
                stats.update(("put", "by_size", "size", "log", request_size_group_name), size, ts)
                stats.update(("put", "by_size", "num_requests", "ecmwf", ecmwf_request_size_group_name), 1, ts)
                stats.update(("put", "by_size", "size", "ecmwf", ecmwf_request_size_group_name), size, ts)
                stats.update(("put", "by_size", "num_requests", "zdv", zdv_request_size_group_name), 1, ts)
                stats.update(("put", "by_size", "size", "zdv", zdv_request_size_group_name), size, ts)
            elif elems[REQUEST] == 'DEL':
                stats.update(("del", "requests"), 1, ts)
                stats.update(("del", "bytes"), size, ts)
                stats.update(("del", "by_size", "num_requests", "log", request_size_group_name), 1, ts)
                stats.update(("del", "by_size", "size", "log", request_size_group_name), size, ts)
                stats.update(("del", "by_size", "num_requests", "ecmwf", ecmwf_request_size_group_name), 1, ts)
                stats.update(("del", "by_size", "size", "ecmwf", ecmwf_request_size_group_name), size, ts)
                stats.update(("del", "by_size", "num_requests", "zdv", zdv_request_size_group_name), 1, ts)
                stats.update(("del", "by_size", "size", "zdv", zdv_request_size_group_name), size, ts)
            elif elems[REQUEST] == 'RENAME':
                pass
            else:
                print ("bad line: %s" % (line))

if __name__ == "__main__":

    name = sys.argv[0]
    stats = Stats()

    if len(sys.argv) == 1:
        print "usage: results.json trace.gz next_trace.gz ..."
        print "reads all the trace files and outputs some stats about them."
        sys.exit(1)

    outfile = os.path.abspath(sys.argv[1])
    if os.path.exists(outfile):
        print("target file: %s already exist" % outfile)
        sys.exit(1)

    for i in range (2, len(sys.argv)):
        trace_file_path = os.path.abspath(sys.argv[i])

        if not os.path.exists(trace_file_path):
            print("trace_file: %s does not exist" % trace_file_path)
            sys.exit(1)

    for i in range (2, len(sys.argv)):
        trace_file_path = os.path.abspath(sys.argv[i])
        generate_stats(stats, trace_file_path)

    # from daily stats, create monthly / yearly / totals
    s = stats.to_dict()

    with open(outfile, 'w') as out:
        json.dump(s, out, indent=2, sort_keys=True)

    print json.dumps(s, indent=2, sort_keys=True)
