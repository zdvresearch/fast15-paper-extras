#!/usr/bin/env python

import os
import json
import datetime

KB = 1024
MB = 1024 * KB
GB = 1024 * MB
TB = 1024 * GB
PB = 1024 * TB


import sys
def main(config_dir):

    cache_buckets_defs = []

    ts = datetime.datetime.now().strftime("%Y-%m-%d_%H%M%S")

    TINY = "Tiny"
    SMALL = "Small"
    MEDIUM = "Medium"
    LARGE = "Large"
    HUGE = "Huge"
    ENORMOUS = "Enormous"


    # ECMWF cache sizes: base on september 2014:
    # tiny = 8*TB
    # small = 2560*GB
    # medium = 23039*GB 
    # large = 82*TB
    # huge = 148*TB
    # enormous = 88*TB

    #each 15 steps
    tiny_steps =     [1*GB,     2*GB,   4*GB,   8*GB,   16*GB,  32*GB,  64*GB,  128*GB,  256*GB, 512*GB,    1*TB,    2*TB,   4*TB,    8*TB, 100*PB]
    small_steps =    [16*GB,   32*GB,  64*GB,  96*GB, 128*GB,  192*GB, 256*GB,  512*GB,  768*GB,   1*TB,    2*TB, 2560*GB,   3*TB,    4*TB, 100*PB]
    medium_steps =   [512*GB, 768*GB,   1*TB,   2*TB,    3*TB,   4*TB,   5*TB,    6*TB,    8*TB,  12*TB,   16*TB,   20*TB,   24*TB,  28*TB, 100*PB]
    large_steps =    [256*GB, 512*GB,   1*TB,   2*TB,   4*TB,   8*TB,   16*TB,   24*TB,   32*TB,  48*TB,   64*TB,   80*TB,   96*TB, 108*TB, 100*PB]
    huge_steps =     [8*TB,    16*TB,  32*TB,  64*TB,  128*TB, 160*TB,  192*TB,  256*TB,  384*TB, 512*TB,  640*TB,  768*TB, 1024*TB,1280*TB, 100*PB]
    enormous_steps = [32*TB,   48*TB,  64*TB,  96*TB,  128*TB, 256*TB,  512*TB,  768*TB,    1*PB, 1536*TB,   2*PB,  2560*TB,    3*PB, 3584*TB, 100*PB]

    for cache_type in ["BeladyCache", "LRUCache", "FifoCache", "MRUCache", "ARCCache", "SplitLRUCache"]:
        for i in range(len(tiny_steps)):
            cache_buckets_defs.append(
                ("ecmwf_%r_%s" % (i, str(cache_type)), {
                              TINY: [0, 512 * KB, tiny_steps[i], cache_type],
                              SMALL: [512 * KB, 1 * MB, small_steps[i], cache_type],
                              MEDIUM: [1 * MB, 8 * MB, medium_steps[i], cache_type],
                              LARGE: [8 * MB, 48 * MB, large_steps[i], cache_type],
                              HUGE: [48 * MB, 1 * GB, huge_steps[i], cache_type],
                              ENORMOUS: [1 * GB, sys.maxint, enormous_steps[i], cache_type]
                            }
                    )
                )

    cache_type = "RandomCache"
    for r in range(5):
        for i in range(len(tiny_steps)):
            cache_buckets_defs.append(
                ("ecmwf_%r_%s-%r" % (i, str(cache_type), r), {
                              TINY: [0, 512 * KB, tiny_steps[i], cache_type],
                              SMALL: [512 * KB, 1 * MB, small_steps[i], cache_type],
                              MEDIUM: [1 * MB, 8 * MB, medium_steps[i], cache_type],
                              LARGE: [8 * MB, 48 * MB, large_steps[i], cache_type],
                              HUGE: [48 * MB, 1 * GB, huge_steps[i], cache_type],
                              ENORMOUS: [1 * GB, sys.maxint, enormous_steps[i], cache_type]
                            }
                    )
                )


    # generate some specific configs.
    # for i in [12]:
    #     for cache_type in ["BeladyCache"]:
    #     # for cache_type in ["BeladyCache", "LRU2NDCache", "LRUCache", "FifoCache", "MRUCache"]:
    #         cache_buckets_defs.append(
    #             ("ecmwf_%r_%s" % (i, str(cache_type)), {
    #                           TINY: [0, 512 * KB, tiny_steps[i], cache_type],
    #                           SMALL: [512 * KB, 1 * MB, small_steps[i], cache_type],
    #                           MEDIUM: [1 * MB, 8 * MB, medium_steps[i], cache_type],
    #                           LARGE: [8 * MB, 48 * MB, large_steps[i], cache_type],
    #                           HUGE: [48 * MB, 1 * GB, huge_steps[i], cache_type],
    #                           ENORMOUS: [1 * GB, sys.maxint, enormous_steps[i], cache_type]
    #                         }
    #                 )
    #             )

    #     cache_type = "RandomCache"
    #     for r in range(5):
    #         cache_buckets_defs.append(
    #             ("ecmwf_%r_%s-%r" % (i, str(cache_type), r), {
    #                           TINY: [0, 512 * KB, tiny_steps[i], cache_type],
    #                           SMALL: [512 * KB, 1 * MB, small_steps[i], cache_type],
    #                           MEDIUM: [1 * MB, 8 * MB, medium_steps[i], cache_type],
    #                           LARGE: [8 * MB, 48 * MB, large_steps[i], cache_type],
    #                           HUGE: [48 * MB, 1 * GB, huge_steps[i], cache_type],
    #                           ENORMOUS: [1 * GB, sys.maxint, enormous_steps[i], cache_type]
    #                         }
    #                 )
    #             )
    


    # single huge cache with the capacity of all others combined.
    for cache_type in ["ARCCache"]:
    # for cache_type in ["BeladyCache", "LRUCache", "FifoCache", "MRUCache", "ARCCache", "SplitLRUCache"]:
        for i in range(len(tiny_steps)):
            csize = tiny_steps[i] + small_steps[i] + medium_steps[i] + large_steps[i] + huge_steps[i] + enormous_steps[i]
            cache_buckets_defs.append(
                ("ecmwf_ALL-%d_%s" % (i, str(cache_type)), {
                    "ALL": [0, sys.maxint, csize, cache_type]
                    }
                )
            )

    # cache_type = "RandomCache"
    # for r in range(5):
    #     for i in range(len(tiny_steps)):
    #         csize = tiny_steps[i] + small_steps[i] + medium_steps[i] + large_steps[i] + huge_steps[i] + enormous_steps[i]
    #         cache_buckets_defs.append(
    #             ("ecmwf_ALL-%d_%s-%r" % (i, str(cache_type), r), {
    #                 "ALL": [0, sys.maxint, csize, cache_type]
    #                 }
    #             )
    #         )
                    
    tracefiles = []

    print(__file__)
    # spath = os.path.abspath(os.path.join(os.path.abspath(__file__), '../../../Traces/ECFS_Access_Traces/ecfs_access_2012.01-2014.05.gz'))
    # print (spath)
    # tracefiles.append(spath)

    for tracefile in tracefiles:
        for cache_config in cache_buckets_defs:
            config = {}
            xn = os.path.basename(tracefile[:tracefile.find('.')])
            config["name"] = "%s-%s" % (xn, cache_config[0])
            config["buckets"] = cache_config[1]
            config["cache_config"] = cache_config[0]
            config["trace"] = tracefile

            p = os.path.join(config_dir, config["name"] + ".json")
            with open(p, 'w') as f:
                json.dump(config, f, indent=2, sort_keys=True)
                print("wrote config : %s"  % p)

    
if __name__ == "__main__":
    name = sys.argv[0]
    if len(sys.argv) == 2:
        config_dir = sys.argv[1]
        if not os.path.exists(config_dir):
            print ("target config_dir %r does not exist.")
            sys.exit(1)

    else:
        print("usage: %r target_config_dir" % name)
        config_dir = "/tmp"

    sys.exit(main(config_dir))
