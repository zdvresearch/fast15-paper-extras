#!/usr/bin/env python
"""
    works on the raw log files files.
    for every day, get stats for number of files put/get/del, amount of bytes written / read / deleted.
"""
import gzip
import json
import sys
import time
import os
import resource
import datetime


MONITOR_LINES = 100000

class Stats:
    def __init__(self):
        self.s = dict()

    def update(self, description, value):

        # dt = datetime.datetime.utcfromtimestamp(ts)

        for t in ["totals"]:
            if t not in self.s:
                self.s[t] = StatsTree()
            self.s[t].update(description, value)

    def to_dict(self):
        r = dict()

        for k, v in self.s.items():
            # assert all v are StatsTrees
            r[k] = v.flatten()
        return r


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
    # START_TIME      = 0
    # USER_ID         = 1
    # HOST_ID         = 2
    # PROCESS_ID      = 3
    # REQUEST         = 4
    # PARAMS          = 5
    # FILE_SIZE       = 6
    # EXECUTION_TIME  = 7
    # ADDITIONAL_INFO = 8

    PREFIX = 0
    USER_ID = 1
    HOST_ID = 2
    PROCESS_ID = 3
    CMD_TYPE = 4
    CMD = 5
    RETURN_CODE = 6
    START_TIME = 7
    END_TIME = 8
    FILE_SIZE = 9
    ADDITIONAL_INFO = 10 # for xact.end will conain [tape(CC1088]


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

            valid = False
                # ...|end.xact|transaction|return-code|start-time|end-time|further-data|file size: bytes|preliminary data|completion data|(empty/null field)|server:port|client architecture|project account
            if len(elems) > 4:
                if elems[CMD_TYPE] == "end.xact":
                    if elems[RETURN_CODE].isdigit() and int(elems[RETURN_CODE]) == 0:
                        if elems[CMD].startswith("get:"):
                            cached_on_disk = elems[ADDITIONAL_INFO].__contains__('tape')
                            size = int(elems[FILE_SIZE])
                            if cached_on_disk:
                                # stats.update(("get", "by_database", db, x), v, request["startdate"])
                                stats.update(("get", "bytes_disk"), size)
                                stats.update(("get", "requests_disk"), 1)
                            else:
                                stats.update(("get", "bytes_tape"), size)
                                stats.update(("get", "requests_tape"), 1)
                        elif elems[CMD].startswith("put:"):
                            if not elems[FILE_SIZE].isdigit():
                                print "bad line: %s" % line
                                continue
                            size = int(elems[FILE_SIZE])

                            stats.update(("put", "bytes"), size)
                            stats.update(("put", "requests"), 1)


if __name__ == "__main__":

    name = sys.argv[0]
    stats = Stats()

    if len(sys.argv) == 1:
        print "usage: trace_file next_trace next_trace ..."
        print " reads all the trace files and outputs some stats about them."
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
