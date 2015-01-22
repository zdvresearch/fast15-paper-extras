#!/usr/bin/env python2

"""analyze.py: Reads and counts ecfs metadata db dump."""
__author__ = "Matthias Grawinkel"
__status__ = "Production"


import sys
import os
import gzip
import resource
import re
import time

from collections import Counter

from file_size_groups import *

MONITOR_LINES=100000

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
    return None

by_size = Counter()
by_count = Counter()

def count(source_file):

    object_re = re.compile("^\s*([\d]+)\s*([\d]+)\s*([\d]+)\s*([\d]+)\s*([\d]+-[\d]+)\s*([\d]+-[\d]+)\s*([\d]+-[\d]+)\s*([a-zA-Z0-9/.\-]+)\s*x'([[a-zA-Z0-9/.\-]+)'\s*$")

    with gzip.open(source_file, 'r') as source:
        t = time.time()
        plines = 0
        for line in source:
            plines += 1
            if plines % MONITOR_LINES == 0:
                print ("processed lines: %d, mem: %rMB, lines/s: %r:" %
                 (plines,
                  float(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss) / 1024 / 1024,
                  int(MONITOR_LINES / (time.time() - t))
                 )
                )
                t = time.time()

            # find all pathes in the line and replace their elements by their md5 sums
            m = object_re.match(line)

            if m:
                r = m.groups()

                size = int(r[0])
                group = get_ecmwf_file_size_group_name(size)
                by_size[group] += size
                by_count[group] += 1

    print("by_size")
    print(json.dumps(by_size, indent=2, sort_keys=True))
    
    print("by_count")
    print(json.dumps(by_count, indent=2, sort_keys=True))


if __name__ == "__main__":

    if len(sys.argv) != 2:
        print("usage: %s source_file target.gz" % sys.argv[0])
        sys.exit(1)

    source_file = os.path.abspath(sys.argv[1])

    print("source_file == %s" % (source_file))

    if not os.path.exists(source_file):
        print("source: %s does not exist" % source_file)
        sys.exit(1)

    count(source_file)
