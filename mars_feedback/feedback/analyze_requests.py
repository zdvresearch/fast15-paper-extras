#!/usr/bin/env python

__author__ = 'meatz'

import numpy as np


import csv
import gzip
import sys
import time
import os
import re
import json
import time
import calendar
import datetime
from collections import defaultdict
from collections import Counter
import fnmatch


# add ecmwf_utils to python path
util_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
print (util_path)
sys.path.append(util_path)

from ecmwf_util import Statistics

def prettyfy(number):
    d = float(number)
    if d - int(d) > 0:
        return '{:,.2f}'.format(d)
    return '{:,d}'.format(int(d))

results = {}

tapes_counter = Counter()

results["total_counted_requests"] = 0
results["total_requests_with_fdb"] = 0
results["total_requests_with_tape"] = 0
results["total_requests_with_disk"] = 0

results["total_requests_with_fdb_only"] = 0
results["total_requests_with_tape_only"] = 0
results["total_requests_with_disk_only"] = 0

exectimes_with_tape = Counter()
exectimes_no_tape = Counter()

def dump(todo_list_retrieves):
    TS = 0
    FIELDS = 1 
    FIELDS_ONLINE = 2
    FIELDS_OFFLINE = 3
    BYTES = 4
    BYTES_ONLINE = 5
    BYTES_OFFLINE = 6
    TAPES = 7
    TAPE_FILES = 8
    EXEC_TIME = 9
    DATABASE = 10


    retrieves_files_read_cnt = 0
    for sf in source_files:
        retrieves_files_read_cnt += 1

        # if retrieves_files_read_cnt == 3:
        #     return
        with gzip.open(sf, 'rt') as csv_file:
            reader = csv.reader(csv_file, delimiter=';')
            next(reader)  # skip header
            
            for row in reader:
                fields = int(row[FIELDS]) + int(row[FIELDS_ONLINE]) + int(row[FIELDS_OFFLINE])
                
                bytes = int(row[BYTES])
                bytes_online = int(row[BYTES_ONLINE])
                bytes_offline = int(row[BYTES_OFFLINE])

                tapes = int(row[TAPES])
                exec_time = int(row[EXEC_TIME])

                if bytes > 0:
                    if bytes > (1024 * 1024 * 1024 * 1024) :
                        print ("skipping line: %s" % row)
                        pass
                    else:
                        results["total_counted_requests"] += 1
                        tapes_counter[tapes] += 1

                        if bytes > 0 and (bytes_online + bytes_offline) != bytes:
                            results["total_requests_with_fdb"] += 1
                        if bytes_online > 0:
                            results["total_requests_with_disk"] += 1
                        if bytes_offline > 0:
                            results["total_requests_with_tape"] += 1
                        
                        if bytes > 0 and bytes_online == 0 and bytes_offline == 0:
                            results["total_requests_with_fdb_only"] += 1
                        
                        if bytes > 0 and bytes_online == bytes and bytes_offline == 0:
                            results["total_requests_with_disk_only"] += 1

                        if bytes > 0 and bytes_online == 0 and bytes_offline == bytes:
                            results["total_requests_with_tape_only"] += 1

                        if tapes > 0:
                            exectimes_with_tape[exec_time] += 1
                        else:
                            exectimes_no_tape[exec_time] += 1


        print("%s finished reading retrieves_file: %d : %s" % (datetime.datetime.now(), retrieves_files_read_cnt, sf))


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print ("usage: /path/to/*retrieves.csv.gz")
        sys.exit(1)

    source_dir = os.path.abspath(sys.argv[1])

    source_files = [os.path.join(dirpath, f)
        for dirpath, dirnames, files in os.walk(source_dir)
        for f in fnmatch.filter(files, '*.retrieves.csv.gz')]


    dump(source_files)

    results["fraction_of_requests_with_tape_percent"] = prettyfy(float(results["total_requests_with_tape"]) / results["total_counted_requests"] * 100)


    er = {}
    er["with_tape"] = {}
    er["no_tape"] = {}
    er["tapes_counter"] = {}


    elems = list(exectimes_no_tape.elements())
    er["no_tape"]["P05"] = prettyfy(Statistics.percentile(elems, 0.05))
    er["no_tape"]["P25"] = prettyfy(Statistics.percentile(elems, 0.25))
    er["no_tape"]["P50"] = prettyfy(Statistics.percentile(elems, 0.50))
    er["no_tape"]["P95"] = prettyfy(Statistics.percentile(elems, 0.95))
    er["no_tape"]["P99"] = prettyfy(Statistics.percentile(elems, 0.99))
    er["no_tape"]["mean"] = Statistics.get_meanconf_string(elems)

    elems = list(exectimes_with_tape.elements())
    er["with_tape"]["P05"] = prettyfy(Statistics.percentile(elems, 0.05))
    er["with_tape"]["P25"] = prettyfy(Statistics.percentile(elems, 0.25))
    er["with_tape"]["P50"] = prettyfy(Statistics.percentile(elems, 0.50))
    er["with_tape"]["P95"] = prettyfy(Statistics.percentile(elems, 0.95))
    er["with_tape"]["P99"] = prettyfy(Statistics.percentile(elems, 0.99))
    er["with_tape"]["mean"] = Statistics.get_meanconf_string(elems)
    
    tapes_counter
    elems = list(tapes_counter.elements())
    er["tapes_counter"]["P05"] = prettyfy(Statistics.percentile(elems, 0.05))
    er["tapes_counter"]["P25"] = prettyfy(Statistics.percentile(elems, 0.25))
    er["tapes_counter"]["P50"] = prettyfy(Statistics.percentile(elems, 0.50))
    er["tapes_counter"]["P95"] = prettyfy(Statistics.percentile(elems, 0.95))
    er["tapes_counter"]["P99"] = prettyfy(Statistics.percentile(elems, 0.99))
    er["tapes_counter"]["mean"] = Statistics.get_meanconf_string(elems)
    
    results["tape_exectimes"] = er

    print (json.dumps(results, indent=2))