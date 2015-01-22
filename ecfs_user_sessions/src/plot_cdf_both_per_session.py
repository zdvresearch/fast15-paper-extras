#!/usr/bin/env python

import matplotlib
matplotlib.use('Agg')

from matplotlib import pyplot
import numpy as np

import lzma
import sys
import time
import os
import re
import glob
import json
from collections import defaultdict
import resource
import operator

from analyze_user_sessions import UserSession

START_TIME      = 0
USER_ID         = 1
HOST_ID         = 2
PROCESS_ID      = 3
REQUEST         = 4
PARAMS          = 5
FILE_SIZE       = 6
EXECUTION_TIME  = 7
ADDITIONAL_INFO = 8

def to_millions(x):
    return float(x) / 1000 / 1000

def to_petabyte(x):
    return float(x) / 1024 / 1024 / 1024 / 1024 / 1024

MONITOR_LINES = 100000

def traffic_per_user_id_cdfs(source_file, target_file):
    user_id_get_bytes = defaultdict(int)
    user_id_get_requests = defaultdict(int)
    user_id_put_bytes = defaultdict(int)
    user_id_put_requests = defaultdict(int)

    user_ids = set()

    with lzma.open(source_file, 'rt') as sf:
        plines = 0
        t = time.time()
        for line in sf:
            plines += 1
            if plines % MONITOR_LINES == 0:
                print ("processed lines: %d mem: %rMB, lines/s: %r" %
                 (plines,
                  float(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss) / 1024,
                  int(MONITOR_LINES / (time.time() - t))
                 )
                )
                t = time.time()


            # if plines == 1000000:
            #     break

            elems = line.split('|')

            if len(elems) <= EXECUTION_TIME:
                continue

            if not elems[START_TIME].isdigit():
                print ("bad line: %s" % line)
                continue

            ts_now = int(elems[START_TIME])
            user_id = elems[USER_ID]

            user_ids.add(user_id)

            if elems[REQUEST] == 'GET':
                if not elems[FILE_SIZE].isdigit():
                    continue
                user_id_get_requests[user_id] += 1
                user_id_get_bytes[user_id] += int(elems[FILE_SIZE])

            elif elems[REQUEST] == 'PUT':
                if not elems[FILE_SIZE].isdigit():
                    continue
                
                user_id_put_requests[user_id] += 1
                user_id_put_bytes[user_id] += int(elems[FILE_SIZE])

    get_users = set(user_id_put_requests.keys())
    put_users = set(user_id_get_requests.keys())

    print("total seen user_ids: %d" % (len(user_ids)))
    print("get_users: %d" % len(get_users))
    print("put_users: %d" % len(put_users))
    print("union: %d" % len(get_users & put_users))


    # print BYTES ====================================================================
    get_bytes_y_vals = list()
    tmp = 0
    for u in sorted(user_id_get_bytes.items(), key=operator.itemgetter(1), reverse=True):
        user_id = u[0]
        bytes = u[1]
        tmp += bytes
        get_bytes_y_vals.append(to_petabyte(tmp))
        
    put_bytes_y_vals = list()
    tmp = 0
    for u in sorted(user_id_put_bytes.items(), key=operator.itemgetter(1), reverse=True):
        user_id = u[0]
        bytes = u[1]
        tmp += bytes
        put_bytes_y_vals.append(to_petabyte(tmp))

    # print REQUESTS ====================================================================
    get_requests_y_vals = list()
    tmp = 0
    for u in sorted(user_id_get_requests.items(), key=operator.itemgetter(1), reverse=True):
        user_id = u[0]
        requests = u[1]
        tmp += requests
        get_requests_y_vals.append(to_millions(tmp))
        
    put_requests_y_vals = list()
    tmp = 0
    for u in sorted(user_id_put_requests.items(), key=operator.itemgetter(1), reverse=True):
        user_id = u[0]
        requests = u[1]
        tmp += requests
        put_requests_y_vals.append(to_millions(tmp))


    # print BYTES + REQUESTS ====================================================================
    fig, ax1 = pyplot.subplots()
    
    # pyplot.xticks(x_vals, [x for x in range(len(x_vals))], fontsize=20)
    # pyplot.yticks(fontsize=20)
    # pyplot.title('Count of Operations per Month', fontsize=40)
    ax1.set_xlabel('User ids', fontsize=20)
    l1 = ax1.plot(np.arange(len(get_bytes_y_vals)), get_bytes_y_vals, '-', linewidth=2, color="k", label="GET PB")
    l2 = ax1.plot(np.arange(len(put_bytes_y_vals)), put_bytes_y_vals, '-.', linewidth=2, color="k", label="PUT PB")
    
    ax2 = ax1.twinx()
    l3 = ax2.plot(np.arange(len(get_requests_y_vals)), get_requests_y_vals, '--', linewidth=2, color="b", label="#GET Requests")
    l4 = ax2.plot(np.arange(len(put_requests_y_vals)), put_requests_y_vals, ':', linewidth=2, color="b", label="#PUT Requests")
   
   


    ax1.set_ylabel('Accumulated PB per user id', fontsize=20)
    ylabela = ax2.set_ylabel('#Requests per user id in mil', fontsize=20)

    ax1.yaxis.grid(True)
    ax2.yaxis.grid(True)
    ax1.xaxis.grid(True)

    ax1.yaxis.label.set_color('k')
    ax2.yaxis.label.set_color('b')
    
    lns = l1 + l2 + l3 + l4
    labs = [l.get_label() for l in lns]
    ax2.legend(lns, labs, loc="lower right") 
    
    print (len(get_bytes_y_vals))
    print (len(put_bytes_y_vals))

    sizes = fig.get_size_inches()
    fig.set_size_inches(sizes[0]*1.3, sizes[1])
    
    # pyplot.tight_layout()
    pyplot.yticks(fontsize=24)
    pyplot.xticks(fontsize=24)

    pyplot.savefig(target_file, bbox_extra_artists=[ylabela], bbox_inches='tight')
    print("saved %s" % (target_file))
    pyplot.close()

if __name__ == "__main__":

    if len(sys.argv) == 1:
        print ("usage: /path/to//ecfs_access_2012.01-2014.05.xz /path/to/cdf_both_per_session.pdf")
        sys.exit(1)

    source_file = os.path.abspath(sys.argv[1])
    target_file = os.path.abspath(sys.argv[2])

    traffic_per_user_id_cdfs(source_file, target_file)