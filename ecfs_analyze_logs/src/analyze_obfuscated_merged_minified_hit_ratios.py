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

    def update_disk_size(self, description, value, ts):

        dt = datetime.datetime.utcfromtimestamp(ts)

        t = dt.strftime("%Y-%m-%d")
        if t not in self.s:
            self.s[t] = StatsTree()
        self.s[t].update_disk_size(description, value)

    def update_tape_size(self, description, value, ts):

        dt = datetime.datetime.utcfromtimestamp(ts)

        t = dt.strftime("%Y-%m-%d")
        if t not in self.s:
            self.s[t] = StatsTree()
        self.s[t].update_tape_size(description, value)

    def update_tape_requests(self, description, value, ts):

        dt = datetime.datetime.utcfromtimestamp(ts)

        t = dt.strftime("%Y-%m-%d")
        if t not in self.s:
            self.s[t] = StatsTree()
        self.s[t].update_tape_requests(description, value)

    def update_disk_requests(self, description, value, ts):

        dt = datetime.datetime.utcfromtimestamp(ts)

        t = dt.strftime("%Y-%m-%d")
        if t not in self.s:
            self.s[t] = StatsTree()
        self.s[t].update_disk_requests(description, value)

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


    def update_disk_size(self, description, value):
        p = self.stats
        depth = len(description)
        for d in range(depth):
            if d == depth -1:
                if description[d] not in p:
                    p[description[d]] = StatsElem()
                p[description[d]].update_disk_size(value)
            else:
                if description[d] not in p:
                    p[description[d]] = dict()
                p = p[description[d]]

    def update_tape_size(self, description, value):
        p = self.stats
        depth = len(description)
        for d in range(depth):
            if d == depth -1:
                if description[d] not in p:
                    p[description[d]] = StatsElem()
                p[description[d]].update_tape_size(value)
            else:
                if description[d] not in p:
                    p[description[d]] = dict()
                p = p[description[d]]

    def update_tape_requests(self, description, value):
        p = self.stats
        depth = len(description)
        for d in range(depth):
            if d == depth -1:
                if description[d] not in p:
                    p[description[d]] = StatsElem()
                p[description[d]].update_tape_requests(value)
            else:
                if description[d] not in p:
                    p[description[d]] = dict()
                p = p[description[d]]

    def update_disk_requests(self, description, value):
        p = self.stats
        depth = len(description)
        for d in range(depth):
            if d == depth -1:
                if description[d] not in p:
                    p[description[d]] = StatsElem()
                p[description[d]].update_disk_requests(value)
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
        self.disk_size = 0
        self.disk_requests = 0
        self.tape_size = 0
        self.tape_requests = 0

    def update_disk_size(self, value):
        self.disk_size += value

    def update_disk_requests(self, value):
        self.disk_requests += value


    def update_tape_size(self, value):
        self.tape_size += value

    def update_tape_requests(self, value):
        self.tape_requests += value


    def to_dict(self):
        s = dict()

        s["disk_size"] = self.disk_size
        s["disk_requests"] = self.disk_requests
        s["tape_size"] = self.tape_size
        s["tape_requests"] = self.tape_requests

        ratio_bytes = 0
        if (self.disk_size + self.tape_size) > 0:
            ratio_bytes = float(self.disk_size) / (self.disk_size + self.tape_size)
        s["hit_ratio_bytes"] = ratio_bytes

        ratio_requests = 0
        if (self.disk_requests + self.tape_requests) > 0:
            ratio_requests = float(self.disk_requests) / (self.disk_requests + self.tape_requests)
        s["hit_ratio_requests"] = ratio_requests

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

            elems = line.decode().split('|')

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

            request_size_group_name = stats.get_file_size_group_name(stats.get_file_size_group(size))
            ecmwf_request_size_group_name = stats.get_ecmwf_file_size_group_name(size)
            zdv_request_size_group_name = stats.get_zdv_file_size_group_name(size)

            if elems[REQUEST] == 'GET':
                from_tape = elems[ADDITIONAL_INFO].__contains__('tape')

                if not from_tape:
                    stats.update_disk_size(("total",), size, ts)
                    stats.update_disk_requests(("total", ), 1, ts)
                    
                    stats.update_disk_size(("log", request_size_group_name), size, ts)
                    stats.update_disk_requests(("log", request_size_group_name), 1, ts)
                    
                    stats.update_disk_size(("ecmwf", ecmwf_request_size_group_name), size, ts)
                    stats.update_disk_requests(("ecmwf", ecmwf_request_size_group_name), 1, ts)
                    
                    stats.update_disk_size(("zdv", zdv_request_size_group_name), size, ts)
                    stats.update_disk_requests(("zdv", zdv_request_size_group_name), 1, ts)
                else:
                    stats.update_tape_size(("total",), size, ts)
                    stats.update_tape_requests(("total", ), 1, ts)
                    
                    stats.update_tape_size(("log", request_size_group_name), size, ts)
                    stats.update_tape_requests(("log", request_size_group_name), 1, ts)
                    
                    stats.update_tape_size(("ecmwf", ecmwf_request_size_group_name), size, ts)
                    stats.update_tape_requests(("ecmwf", ecmwf_request_size_group_name), 1, ts)
                    
                    stats.update_tape_size(("zdv", zdv_request_size_group_name), size, ts)
                    stats.update_tape_requests(("zdv", zdv_request_size_group_name), 1, ts)

if __name__ == "__main__":

    name = sys.argv[0]
    stats = Stats()

    if len(sys.argv) == 1:
        print ("usage: results.json trace.gz next_trace.gz ...")
        print ("reads all the trace files and outputs some stats about them.")
        sys.exit(1)

    outfile = os.path.abspath(sys.argv[1])
    if os.path.exists(outfile):
        print("target file: %s already exist" % outfile)
        # sys.exit(1)

    for i in range(2, len(sys.argv)):
        trace_file_path = os.path.abspath(sys.argv[i])

        if not os.path.exists(trace_file_path):
            print("trace_file: %s does not exist" % trace_file_path)
            sys.exit(1)

    for i in range(2, len(sys.argv)):
        trace_file_path = os.path.abspath(sys.argv[i])
        generate_stats(stats, trace_file_path)

    # from daily stats, create monthly / yearly / totals
    s = stats.to_dict()
    print(json.dumps(s, indent=4, sort_keys=True))

    with open(outfile, 'w') as out:
        json.dump(s, out, indent=2, sort_keys=True)




