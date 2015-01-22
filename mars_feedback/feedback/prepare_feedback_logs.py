__author__ = 'meatz'

import bz2
import os
import sys
import glob
import re
import time
import calendar
import json
import resource
import gzip
from collections import defaultdict
import io
from multiprocessing import Pool

"""
    transform the log files into one log reader_log that is obfuscated and just contains the interesting contents.


http://old.ecmwf.int/publications/manuals/mars/guide/Identification_keywords.html

class: full list available: http://old.ecmwf.int/publications/manuals/d/gribapi/mars/att=class/index.html
    most important will be od and rd

"""

stats=defaultdict(int)

# add ecmwf_utils to python path
util_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "ecmwf_util")
print (util_path)
sys.path.append(util_path)

import Obfuscate

MONITOR_LINES = 100000

line_pattern = re.compile("([a-zA-Z_-]*)='([^']*)'")

# debug output
PRINTRET = False
PRINTARC = False
PRINTSTA = False
PRINTFLU = False
PRINTREM = False

def get_utc_timestamp(date_, time_):
    """
        return unix timestamp in utc
      "startdate": "20130813",
      "starttime": "10:53:44",
    """
    t = time.strptime("%s-%s" % (date_, time_), '%Y%m%d-%H:%M:%S')
    epoch = calendar.timegm(t)
    return epoch

def transform_line(line):
    """
        filters the line for relevant data and adds a utc timestamp.
    """

    request = dict()
    r = None
    # build the request dict from the key:value pairs in the logline $key='value';
    for t in line_pattern.findall(line):
        request[t[0]] = t[1]

    if "verb" in request:
        if request["verb"] == "retrieve":
            stats["retrieve_calls"] += 1
            r = add_retrieve(request)
        elif request["verb"] == "archive":
            stats["archive_calls"] += 1
            r = add_archive(request)
        elif request["verb"] == "stage":
            stats["stage_calls"] += 1
            r = add_stage(request)
        elif request["verb"] == "flush":
            stats["flush_calls"] += 1
            r = add_flush(request)
        elif request["verb"] == "remove":
            stats["remove_calls"] += 1
            r = add_remove(request)

    if r is not None:
        if "start" in r:
            line = str(r.pop("start"))
            line += ";" + request["verb"]
            for x, y in r.items():
                line += ";" + str(x) + "=" + str(y)
            return line + "\n"
        else:
            print("!!broken request: %s" % line)

    return None



def add_retrieve(request):
    """
        analyze a verb=retrieve request.
    """

    if PRINTRET:
        print("=========RETRIEVE=========")
        print(json.dumps(request, indent=2, sort_keys=True))

    for x in ["startdate", "starttime", "stopdate", "stoptime"]:
        if x not in request:
            return None
    

    r = dict()
    start_ts = get_utc_timestamp(request["startdate"], request["starttime"])
    r["start"] = start_ts

    stop_ts = get_utc_timestamp(request["stopdate"], request["stoptime"])
    #
    # # how many seconds did the execution of the retrieve take?
    exec_time = stop_ts - start_ts

    r["stop"] = stop_ts
    r["exec_time"] = exec_time

    if "user" in request:
        r["user"] = Obfuscate.get_md5(request["user"], hexdigest=True)
    
    for x in ["application", "bytes", "bytes_online", "bytes_offline", "class", "database", "disk_files", "environment", "fields", "fields_online", "fields_offline", "rdatabase", "status", "tapes", "tape_files", "written"]:
        if x in request:
            if PRINTRET: print(x, request[x])

            r[x] = request[x]

    if PRINTRET: print("=========/RETRIEVE=========")
    return r


def add_archive(request):

    if PRINTARC:
        print("=========ARCHIVE=========")
        print(json.dumps(request, indent=2, sort_keys=True))

    for x in ["startdate", "starttime", "stopdate", "stoptime"]:
        if x not in request:
            return None
    
    r = dict()
    start_ts = get_utc_timestamp(request["startdate"], request["starttime"])
    r["start"] = start_ts

    stop_ts = get_utc_timestamp(request["stopdate"], request["stoptime"])
    exec_time = stop_ts - start_ts

    r = dict()
    r["start"] = start_ts
    r["stop"] = stop_ts
    r["exec_time"] = exec_time

    # obfuscate the user!
    if "user" in request:
        r["user"] = Obfuscate.get_md5(request["user"], hexdigest=True)
            
    for x in ["application", "bytes", "class", "database", "environment", "fields", "rdatabase", "status"]:
        if x in request:
            r[x] = request[x]

    if PRINTARC:
        print("========/ARCHIVE=========")
    return r

def add_stage(request):
    if PRINTSTA:
        print("=========STAGE=========")
        print(json.dumps(request, indent=2, sort_keys=True))
        print("========/STAGE=========")

    return None


def add_remove(request):
    if PRINTREM:
        print("=========REMOVE=========")
        print(json.dumps(request, indent=2, sort_keys=True))
        print("========/REMOVE=========")
    return None


def add_flush(request):
    if PRINTFLU:
        print("=========FLUSH=========")
        print(json.dumps(request, indent=2, sort_keys=True))
        print("========/FLUSH=========")
    return None


def filter_file(params):
    source_file = params[0]
    target_file = params[1]
    print ("filtering %r -> %r" % (source_file, target_file))
    bz_file = bz2.BZ2File(source_file)

    with gzip.open(target_file, 'wb') as tf:
        for line in bz_file:
            try:
                tline = transform_line(line.decode())
                if tline is not None:
                    tf.write(tline.encode())
            except UnicodeDecodeError:
                pass
    print ("finished %r -> %r" % (source_file, target_file))


if __name__ == "__main__":

    if len(sys.argv) != 5:
        print("source_dir parameter missing")
        print("target_dir parameter missing")
        print("num_processes parameter missing")
        sys.exit(1)

    source_dir = sys.argv[1]
    target_dir = sys.argv[2]
    num_processes = int(sys.argv[3])
    year_filter = sys.argv[4].strip()

    if os.path.exists(source_dir):
        print ("reading files from %s" % (source_dir))
    else:
        print("dir does not exist%s" % source_dir)
        sys.exit(1)

    if os.path.exists(target_dir):
        print ("writing files to %s" % (target_dir))
    else:
        print("dir does not exist%s" % target_dir)
        sys.exit(1)

    todos = list()

    for filename in glob.glob(os.path.join(source_dir, "*.bz2")):
        source_file = os.path.join(source_dir, filename)
        fname = os.path.basename(source_file)
        if fname.startswith(year_filter):
            fname = fname[:fname.rfind('.')] + ".filtered.gz"
            target_file = os.path.join(target_dir, fname)
            todos.append((source_file, target_file))

    pool = Pool(processes=num_processes)
    pool.map(filter_file, todos)

    print("DONE")