#!/usr/bin/env python

__author__ = 'meatz'

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


stats = Counter()
retrieves_files_read_cnt = 0
archives_files_read_cnt = 0


def count_retrieves(sf):
    global retrieves_files_read_cnt
    retrieves_files_read_cnt += 1
    print("%d: %s start reading retrieves_file: %d : %s" % (os.getpid(), datetime.datetime.now(), retrieves_files_read_cnt, sf))
    
    with gzip.open(sf, 'rt') as csv_file:
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

        for row in reader:

            if int(row[BYTES]) + int(row[BYTES_ONLINE]) + int(row[BYTES_OFFLINE]) > (1024 * 1024 * 1024 * 1024) :
                # print ("skipping line: %s" % row)
                pass
            else:
                stats["retr_fields_sum"] += int(row[FIELDS]) 
                stats["retr_fields_fdb"] += int(row[FIELDS]) - int(row[FIELDS_ONLINE]) - int(row[FIELDS_OFFLINE])
                stats["retr_fields_mars"] += int(row[FIELDS_ONLINE])
                stats["retr_fields_hpss"] += int(row[FIELDS_OFFLINE])
                stats["retr_bytes_sum"] += int(row[BYTES]) 
                stats["retr_bytes_fdb"] += int(row[BYTES]) - int(row[BYTES_ONLINE]) - int(row[BYTES_OFFLINE])
                stats["retr_bytes_mars"] +=  + int(row[BYTES_ONLINE])
                stats["retr_bytes_hpss"] += int(row[BYTES_OFFLINE])
            
            stats["num_retrieve_requests"] += 1
    print("%d: %s finished reading retrieves_file: %d : %s" % (os.getpid(), datetime.datetime.now(), retrieves_files_read_cnt, sf))


def count_archives(sf):
    global archives_files_read_cnt
    archives_files_read_cnt += 1
    # print ("%s reading archives_file: %d" % (datetime.datetime.now(), archives_files_read_cnt))
    print("%d: %s start reading archives_file: %d : %s" % (os.getpid(), datetime.datetime.now(), archives_files_read_cnt, sf))
        
    with gzip.open(sf, 'rt') as csv_file:
        reader = csv.reader(csv_file, delimiter=';')
        next(reader)  # skip header

        TS = 0
        FIELDS = 1
        BYTES = 2
        EXEC_TIME = 3
        DATABASE = 4

        for row in reader:
            
            if int(row[BYTES]) > (1024 * 1024 * 1024 * 1024) :
                print ("skipping line: %s" % row)
            else:
                stats["arch_bytes_sum"] += int(row[BYTES])
                stats["arch_fields_sum"] += int(row[FIELDS])

            stats["num_archive_requests"] += 1

    print("%d: %s finished reading archives_file: %d : %s" % (os.getpid(), datetime.datetime.now(), archives_files_read_cnt, sf))


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print ("usage: source_dir")
        sys.exit(1)

    print("pid: %d, version 12" % (os.getpid()))
    # LIMIT = 20
    LIMIT = None  #20

    source_dir = sys.argv[1]  # that contains the cvs
    
    todo_list_retrieves = [os.path.join(dirpath, f)
        for dirpath, dirnames, files in os.walk(source_dir)
        for f in fnmatch.filter(files, '*.retrieves.csv.gz')]

    todo_list_archives = [os.path.join(dirpath, f)
        for dirpath, dirnames, files in os.walk(source_dir)
        for f in fnmatch.filter(files, '*.archives.csv.gz')]

    print("Loading retrieves.csv.gz files")
    # results = pool.map(load_retrieves, todo_list_retrieves)
    
    for todo in todo_list_retrieves:
        count_retrieves(todo)
        
    
    print("Loading retrieves.csv.gz files finished")
    print("===========================================")
    
    print("Loading archives.csv.gz files")
    # results = pool.map(load_archives, todo_list_archives)
    
    for todo in todo_list_archives:
        count_archives(todo)

    print(json.dumps(stats, indent=2))