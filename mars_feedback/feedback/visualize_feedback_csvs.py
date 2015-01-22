#!/usr/bin/env python

__author__ = 'meatz'

import matplotlib
matplotlib.use('Agg')

from matplotlib import pyplot
from matplotlib import dates

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

def to_terabyte(val):
    return (float(val) / 1024 / 1024 / 1024 / 1024)

def to_petabyte(val):
    return (float(val) / 1024 / 1024 / 1024 / 1024 / 1024)

def to_millions(val):
    return (float(val) / 1000 / 1000)

def to_billions(val):
    return (float(val) / 1000 / 1000 / 1000)


def cdf_over_request_size(retrieve_bytes_sum, retrieve_bytes, retrieve_bytes_online, retrieve_bytes_offline, archives_bytes_total, target_graphs_dir):
    
    x_vals = defaultdict(list)
    y_vals = defaultdict(list)

    source_files = [(retrieve_bytes_sum, "Retrieved bytes total", 'b', '-'),
                    (retrieve_bytes,"Retrieved bytes cache", 'b', '--'), 
                    (retrieve_bytes_online, "Retrieved bytes disk", 'b', '-.'), 
                    (retrieve_bytes_offline, "Retrieved bytes tape", 'b', ':'), 
                    (archives_bytes_total, "Archived bytes", 'r',"-")]

    for p in source_files:
        y_temp = 0
        print (type(p), p[1])
        for key in sorted(p[0].keys()):
            x_vals[p[1]].append(key)
            y_temp += to_petabyte(key * p[0][key])
            y_vals[p[1]].append(y_temp)

    fig, ax = pyplot.subplots()
    
    ax.set_xscale('log')
    ax.set_xlabel('Request size in bytes', fontsize=20)

    for p in source_files:
        key_name = p[1]
        col = p[2]
        linestyle = p[3]
        ax.plot(x_vals[key_name], y_vals[key_name], linestyle, linewidth=2, color=col, label=key_name)        

    ax.legend(loc="upper left") 
    ax.set_ylabel('Accumulated PBytes', fontsize=20)
    
    ax.tick_params(labelsize=16)

    ax.yaxis.grid(True)
    ax.xaxis.grid(True)
  
    sizes = fig.get_size_inches()
    fig.set_size_inches(sizes[0]*1.3, sizes[1])

    pyplot.tight_layout()
    outfile = os.path.join(target_graphs_dir, "cdf_feedback_requests_over_bytes.pdf")
    pyplot.savefig(outfile)
    print("saved %s" % (outfile))
    pyplot.close()


retrieve_stats = dict()
retrieve_stats["fields"] = defaultdict(Counter)
retrieve_stats["bytes"] = defaultdict(Counter)
retrieve_stats["bytes_sum"] = defaultdict(Counter)
retrieve_stats["bytes_online"] = defaultdict(Counter)
retrieve_stats["bytes_offline"] = defaultdict(Counter)

archives_stats = dict()
archives_stats["fields"] = defaultdict(Counter)
archives_stats["bytes"] = defaultdict(Counter)

archives_files_read_cnt = 0
retrieves_files_read_cnt = 0

def load_retrieves(sf):
    global retrieves_files_read_cnt
    retrieves_files_read_cnt += 1
    print("%d: %s start reading retrieves_file: %d : %s" % (os.getpid(), datetime.datetime.now(), retrieves_files_read_cnt, sf))
    
    
    with gzip.open(sf, 'r') as csv_file:
        reader = csv.reader(csv_file, delimiter=';')
        next(reader)  # skip header
        TS = 0
        FIELDS = 1 
        FIELDS_ONLINE = 2
        FIELDS_OFFLINE = 3
        BYTES = 4
        BYTES_ONLINE = 5
        BYTES_OFFLINE = 6
        EXEC_TIME = 7
        DATABASE = 8

        # for row in reader:
        #     fields = int(row[FIELDS]) + int(row[FIELDS_ONLINE]) + int(row[FIELDS_OFFLINE])
        #     bytes_sum = int(row[BYTES]) + int(row[BYTES_ONLINE]) + int(row[BYTES_OFFLINE])

        #     if bytes_sum > (1024 * 1024 * 1024 * 1024) :
        #         # print ("skipping line: %s" % row)
        #         pass
        #     else:
        #         retrieve_stats["fields"][row[DATABASE]][fields] += 1
        #         retrieve_stats["bytes_sum"][row[DATABASE]][bytes_sum] += 1
        #         retrieve_stats["bytes"][row[DATABASE]][int(row[BYTES])] += 1
        #         retrieve_stats["bytes_online"][row[DATABASE]][int(row[BYTES_ONLINE])] += 1
        #         retrieve_stats["bytes_offline"][row[DATABASE]][int(row[BYTES_OFFLINE])] += 1

        for row in reader:
            # fields = int(row[FIELDS]) + int(row[FIELDS_ONLINE]) + int(row[FIELDS_OFFLINE])
            # bytes_sum = int(row[BYTES]) + int(row[BYTES_ONLINE]) + int(row[BYTES_OFFLINE])

            if int(row[BYTES]) + int(row[BYTES_ONLINE]) + int(row[BYTES_OFFLINE]) > (1024 * 1024 * 1024 * 1024 * 10) :
                # print ("skipping line: %s" % row)
                # there are some very strange lines here... no requests larger than 10TB.
                pass
            else:
                retrieve_stats["fields"][row[DATABASE]][int(row[FIELDS])] += 1
                retrieve_stats["bytes_sum"][row[DATABASE]][int(row[BYTES])] += 1
                retrieve_stats["bytes"][row[DATABASE]][int(row[BYTES]) - int(row[BYTES_ONLINE]) - int(row[BYTES_OFFLINE])] += 1
                retrieve_stats["bytes_online"][row[DATABASE]][int(row[BYTES_ONLINE])] += 1
                retrieve_stats["bytes_offline"][row[DATABASE]][int(row[BYTES_OFFLINE])] += 1
    print("%d: %s finished reading retrieves_file: %d : %s" % (os.getpid(), datetime.datetime.now(), retrieves_files_read_cnt, sf))


def load_archives(sf):
    global archives_files_read_cnt
    archives_files_read_cnt += 1
    # print ("%s reading archives_file: %d" % (datetime.datetime.now(), archives_files_read_cnt))
    print("%d: %s start reading archives_file: %d : %s" % (os.getpid(), datetime.datetime.now(), archives_files_read_cnt, sf))
    
    
    with gzip.open(sf, 'r') as csv_file:
        reader = csv.reader(csv_file, delimiter=';')
        next(reader)  # skip header

        TS = 0
        FIELDS = 1
        BYTES = 2
        EXEC_TIME = 3
        DATABASE = 4

        for row in reader:
            fields = int(row[FIELDS]) 
            bytes = int(row[BYTES])

            if bytes > (1024 * 1024 * 1024 * 1024) :
                print ("skipping line: %s" % row)
            else:
                archives_stats["fields"][row[DATABASE]][fields] += 1
                archives_stats["bytes"][row[DATABASE]][bytes] += 1

    print("%d: %s finished reading archives_file: %d : %s" % (os.getpid(), datetime.datetime.now(), archives_files_read_cnt, sf))


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print ("usage: reader.stats.json graphs_dir")
        sys.exit(1)

    print("%d, version 11" % (os.getpid()))
    # LIMIT = 20
    LIMIT = None  #20

    source_dir = sys.argv[1]  # that contains the cvs
    target_graphs_dir = sys.argv[2]

    todo_list_retrieves = [os.path.join(dirpath, f)
        for dirpath, dirnames, files in os.walk(source_dir)
        for f in fnmatch.filter(files, '*.retrieves.csv.gz')]

    todo_list_archives = [os.path.join(dirpath, f)
        for dirpath, dirnames, files in os.walk(source_dir)
        for f in fnmatch.filter(files, '*.archives.csv.gz')]

    print("Loading retrieves.csv.gz files")
    
    for todo in todo_list_retrieves:
        load_retrieves(todo)
        
    
    print("Loading retrieves.csv.gz files finished")
    print("===========================================")
    
    print("Loading archives.csv.gz files")
    
    for todo in todo_list_archives:
        load_archives(todo)

    print("start drawing")

    retrieve_bytes_sum = Counter()
    for db, counter in retrieve_stats["bytes_sum"].items():
        for b, c in counter.items():
            retrieve_bytes_sum[b] += c

    retrieve_bytes = Counter()
    for db, counter in retrieve_stats["bytes"].items():
        for b, c in counter.items():
            retrieve_bytes[b] += c

    retrieve_bytes_online = Counter()
    for db, counter in retrieve_stats["bytes_online"].items():
        for b, c in counter.items():
            retrieve_bytes_online[b] += c
    
    retrieve_bytes_offline = Counter()
    for db, counter in retrieve_stats["bytes_offline"].items():
        for b, c in counter.items():
            retrieve_bytes_offline[b] += c

    archives_bytes_total = Counter()
    for db, counter in archives_stats["bytes"].items():
        for b, c in counter.items():
            archives_bytes_total[b] += c
    
    ##### DRAW BYTES HISTOGRAM
    
    print("PLOT!")
    cdf_over_request_size(retrieve_bytes_sum, retrieve_bytes, retrieve_bytes_online, retrieve_bytes_offline, archives_bytes_total, target_graphs_dir)