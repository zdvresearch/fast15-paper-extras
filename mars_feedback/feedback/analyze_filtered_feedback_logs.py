__author__ = 'meatz'

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
import datetime


# add ecmwf_utils to python path
util_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
print (util_path)
sys.path.append(util_path)

from ecmwf_util import Stats

MONITOR_LINES = 10000

"""
for a given time range, check that all reader logs exist.
Then merge them all into one big file and sort them.
"""
def get_utc_ts(ts, format='%Y-%m-%d %H:%M:%S'):
    timestamp = time.strptime(ts, format)
    epoch = calendar.timegm(timestamp)
    return epoch

csv_retrieve_header= "timestamp;fields;fields_online;fields_offline;bytes;bytes_online;bytes_offline;tapes;tape_files;exec_time;database\n"
csv_archive_header = "timestamp;fields;bytes;exec_time;database\n"

def create_stats(params):
    source_file = params[0]
    stats_file = params[1]
    csv_file_retrieve = params[2]
    csv_file_archive = params[3]

    if os.path.exists(stats_file):
        print("skipping existing stats: %s" % (stats_file))
        return

    stats = Stats.Stats()

    processed_lines = 0
    t = time.time()
    
    csv_retrieve = gzip.open(csv_file_retrieve, 'wt')
    csv_retrieve.write(csv_retrieve_header)

    csv_archive = gzip.open(csv_file_archive, 'wt')
    csv_archive.write(csv_archive_header)
    
    with gzip.open(source_file, 'rt') as sf:
        for line in sf:
            processed_lines += 1
            if processed_lines % MONITOR_LINES == 0:
                print ("processed lines: %d, mem: %rMB, lines/s: %r" %
                 (processed_lines,
                  resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024 / 1024,
                  int(MONITOR_LINES / (time.time() - t))
                 )
                )
                t = time.time()

            elems = line.strip().split(";")
            ts = int(elems[0])
            verb = elems[1]

            r = dict()
            for i in range(2, len(elems)):
                v = elems[i].split("=")
                # sometime values contain 'retries' : 123|123|123 or marsod|marsod. then just take the rightmost entry
                r[v[0]] = v[1].split("|")[-1:][0]
            
            if "status" not in r or r["status"] != "ok":
                # skip failed request
                continue  # advance to next line

            for k, v in r.items():
                # if it's a digit, always put it to the totals
                if v.isdigit():
                    stats.updateStats((verb, "totals", k), ts, int(v))

            # now for some specific markers
            if "user" in r:
                for k, v in r.items():
                    if v.isdigit():
                        stats.updateStats((verb, "by_user", r["user"], k), ts, int(v))

            if "class" in r:
                for k, v in r.items():
                    if v.isdigit():
                        stats.updateStats((verb, "by_class", r["class"], k), ts, int(v))

            if "database" in r:
                for k, v in r.items():
                    if v.isdigit():
                        stats.updateStats((verb, "by_database", r["database"], k), ts, int(v))

            if verb == "retrieve":
                line = "%d;%d;%d;%d;%d;%d;%d;%d;%d;%d;%s\n" % (
                    ts,
                    int(r.get("fields", 0)),
                    int(r.get("fields_online", 0)),
                    int(r.get("fields_offline", 0)),
                    int(r.get("bytes", 0)),
                    int(r.get("bytes_online", 0)),
                    int(r.get("bytes_offline", 0)),
                    int(r.get("tapes", 0)),
                    int(r.get("tape_files", 0)),
                    int(r.get("exec_time", 0)),
                    r.get("database", "None")
                    )
                csv_retrieve.write(line)
            elif verb == "archive":
                line = "%d;%d;%d;%d;%s\n" % (
                    ts,
                    int(r.get("fields", 0)),
                    int(r.get("bytes", 0)),
                    int(r.get("exec_time", 0)),
                    r.get("database", "None")
                    )
                csv_archive.write(line)
                
        print("finished: %s" % (source_file))

    
    csv_archive.close()
    csv_retrieve.close()

    s = stats.to_dict()
    with open(stats_file, 'w') as out:
        json.dump(s, out, indent=4, sort_keys=True)

    
if __name__ == "__main__":
    print(sys.argv)
    if len(sys.argv) != 4:
        print("usage: %s source_dir num_processes year_filter" % sys.arv[0])
        sys.exit(1)

    source_dir = sys.argv[1]
    num_processes = int(sys.argv[2])
    year_filter = sys.argv[3].strip()

    if os.path.exists(source_dir):
        print ("reading files from %s" % (source_dir))
    else:
        print("sourc_edir does not exist%s" % source_dir)
        sys.exit(1)

    todos = list()

    for filename in glob.glob(os.path.join(source_dir, "*filtered.gz")):
        source_file = os.path.join(source_dir, filename)
        fname = os.path.basename(source_file)
        if fname.startswith(year_filter):
            target_stats_file = os.path.join(source_dir, fname[:fname.rfind('.')] + ".stats.json")
            target_csv_file_retrieve = os.path.join(source_dir, fname[:fname.rfind('.')] + ".retrieves.csv.gz")
            target_csv_file_archive = os.path.join(source_dir, fname[:fname.rfind('.')] + ".archives.csv.gz")
            todos.append((source_file, target_stats_file, target_csv_file_retrieve, target_csv_file_archive))

    pool = Pool(processes=num_processes)
    pool.map(create_stats, todos)

    print("DONE")