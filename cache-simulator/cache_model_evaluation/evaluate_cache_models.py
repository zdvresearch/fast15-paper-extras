#!/usr/bin/env python

import os
import sys
import gzip
import json
import hashlib
import resource
import datetime



from CacheBuckets import CacheBuckets
from StorageSystem import StorageSystem

# add ecmwf_utils to python path
util_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
print (util_path)
sys.path.append(util_path)

from ecmwf_util import Stats


START_TIME      = 0
USER_ID         = 1
HOST_ID         = 2
PROCESS_ID      = 3
REQUEST         = 4
PARAMS          = 5
FILE_SIZE       = 6
EXECUTION_TIME  = 7
ADDITIONAL_INFO = 8
NEXT_LINE       = 9

KB = 1024
MB = 1024 * KB
GB = 1024 * MB
TB = 1024 * GB

import time
class Timer():
    def __init__(self, s):
        self.s = s

    def __enter__(self):
        self.start = time.time()

    def __exit__(self, *args):
        print ("%s: %fs" % (self.s, (time.time() - self.start)))


def unix_time(dt):
    epoch = datetime.datetime.utcfromtimestamp(0)
    delta = dt - epoch
    return delta.total_seconds()


def get_md5(s, hexdigest=False):
    # return s
    m = hashlib.md5()
    m.update(s)
    if hexdigest:
        return m.hexdigest()
    else:
        return m.digest()

# import pdb; pdb.set_trace()
#import objgraph

def main(results_dir, test_config_file):

#   h = hpy()
    stats = Stats.Stats()

    with open(test_config_file, 'r') as f:
        config = json.load(f)

    cache_buckets = config["buckets"]
    # for bucket_name, bucket_config in cache_buckets.items():
    #     print (bucket_name, bucket_config)

    cache = CacheBuckets(cache_buckets)
    storage = StorageSystem(cache)

    trace_file = os.path.abspath(config["trace"])

    if not os.path.exists(trace_file):
        print ("tracefile does not exist: %s" % (trace_file))
        sys.exit(1)

    processed_lines = 0
    min_epoch = 99999999999999999999999
    max_epoch = 0

    # suspicious_elems = ["624d66b5d86684334fd0387015ddbb9a","8be1e3557e7b7f5b0599e224409b5136", "07e9134eece9be8ac6cafc0b4c0907a4", "838e6c0a20308a730f5d1d3b92147e88", "fe99c5fa4a71454d04a919c156807d35", "416e7504069f37420f3075906c02d46a", "52ef73fa630d46245ee676d6b47e9995"]

    lines_dump = 10000
    with gzip.open(trace_file, 'r') as source_file:
        with Timer("Analyzing file: %s" % (trace_file)):
            epoch = 0
            obj_id = ""
            size = 0
            t = time.time()
            dt = None
            last_day = None
            current_day = None

            next_day_epoch = 0

            for line in source_file:
                # print line
                processed_lines += 1
                if processed_lines % lines_dump == 0:
                    print ("processed: %d, cached_objects: %d, mem: %rMB, requests/s: %r" %
                     (processed_lines,
                      storage.get_cache().get_num_cached_objects(),
                      float(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss) / 1024,
                      int(lines_dump / (time.time() - t))
                     )
                    )
                    # print(json.dumps(storage.get_stats(), indent=2, sort_keys=True))
                    # print ("=================")
                    t = time.time()

                    # if not storage.check_cache_sanity(processed_lines):
                    #     print ("Sanity Check failed")
                    #     sys.exit(1)
                elems = line.split('|')
                
                if len(elems) > FILE_SIZE and elems[START_TIME].isdigit():
                    
                    epoch = int(elems[START_TIME])
                    next_line = int(elems[NEXT_LINE])

                    #check if a new day has started.
                    if next_day_epoch == 0 or epoch >= next_day_epoch:
                        # this is a quite expensive operation and does not need to be calculated more than once a day.
                        dt = datetime.datetime.utcfromtimestamp(epoch)
                        current_day = dt.strftime("%Y-%m-%d")
                        next_day_epoch = unix_time(dt + datetime.timedelta(days=1))
                        print("======================> NEW DAY!!: %s" % current_day)
                    if current_day != last_day:
                        # a new day! write some stats...
                        if last_day is not None:
                            # day has changed, write daily snapshot of storage system stats
                            day_stats = storage.get_stats_day()
                            total_stats = storage.get_stats_total()
                            stats.setDictDay(("stats",), epoch, day_stats)
                            stats.setDictDay(("ctotal",), epoch, total_stats)
                            last_day = current_day
                        last_day = current_day
                    min_epoch = min(min_epoch, epoch)
                    max_epoch = max(max_epoch, epoch)

                    obj_id = get_md5(elems[PARAMS].strip(), hexdigest=True)
                    # if obj_id in suspicious_elems:
                    #     print("%s ::::> %s" % (obj_id, line))
                    #     if not storage.check_cache_sanity(processed_lines):
                    #         print ("Sanity Check before failed")
                    #         sys.exit(1)
                    if elems[REQUEST] == 'GET':
                        if elems[FILE_SIZE].isdigit():
                            size = int(elems[FILE_SIZE])
                            uid = elems[USER_ID]
                            storage.get_object(obj_id, uid, size, epoch, next_line)
                        else:
                            print("bad line: %s" % (line))
                    elif elems[REQUEST] == 'PUT':
                        if elems[FILE_SIZE].isdigit():
                            uid = elems[USER_ID]
                            size = int(elems[FILE_SIZE])
                            storage.put_object(obj_id, uid, size, epoch, next_line)
                        else:
                            print("bad line: %s" % (line))
                    elif elems[REQUEST] == 'DEL':
                        uid = elems[USER_ID]
                        storage.del_object(obj_id, uid, epoch)
                    elif elems[REQUEST] == 'RENAME':
                        uid = elems[USER_ID]
                        from_obj_id = get_md5(elems[PARAMS].split(" ")[0].strip(), hexdigest=True)
                        to_obj_id = get_md5(elems[PARAMS].split(" ")[1].strip(), hexdigest=True)

                        # if to_obj_id in suspicious_elems or from_obj_id in suspicious_elems:
                        #     print("%s ::::> %s" % ( obj_id, line))
                        #     if not storage.check_cache_sanity(processed_lines):
                        #         print ("Sanity Check before failed")
                        #         sys.exit(1)
                        # print ("rename %s \n\t --> %s" % (from_obj_id, to_obj_id))
                        storage.rename_object(from_obj_id, to_obj_id)
                        # if to_obj_id in suspicious_elems or from_obj_id in suspicious_elems:
                        #     if not storage.check_cache_sanity(processed_lines):
                        #         print ("Sanity Check after failed")
                        #         sys.exit(1)
                    else:
                        print("bad line: %s" % (line))

                    # if obj_id in suspicious_elems:
                    #         if not storage.check_cache_sanity(processed_lines):
                    #             print ("Sanity Check after failed")
                    #             sys.exit(1)

            # write stats for ongoing day / month /year
            s = storage.get_stats_day()
            stats.setDictDay(("stats",), epoch, s)

    results = dict()
    results["totals"] = storage.get_stats_total()

    # also store the initial config
    results["config"] = config

    results["epoch_start_ts"] = min_epoch
    results["epoch_start"] = datetime.datetime.utcfromtimestamp(min_epoch).strftime("%Y-%m-%d")

    results["epoch_end_ts"] = max_epoch
    results["epoch_end"] = datetime.datetime.utcfromtimestamp(max_epoch).strftime("%Y-%m-%d")

    results["stats"] = stats.to_dict()
    print(json.dumps(results, indent=4, sort_keys=True))

    results_file = os.path.join(results_dir, "results.json")

    with open(results_file, 'w') as f:
        json.dump(results, f, indent=4, sort_keys=True)

    return 0 
    
if __name__ == "__main__":

    name = sys.argv[0]

    if len(sys.argv) == 3:

        results_dir = sys.argv[1]
        test_config_file = sys.argv[2]

        if not os.path.exists(results_dir):
            print ("results_dir %r does not exist.")
            sys.exit(1)

        if not os.path.exists(test_config_file):
            print("test_config_file %r does not exist.")
            sys.exit(1)
    else:
        print ("usage: %s trace_dir results_dir test_config_file" % name)

    sys.exit(main(results_dir, test_config_file))
