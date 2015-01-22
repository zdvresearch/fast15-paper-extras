#!/usr/bin/env python

import matplotlib
matplotlib.use('Agg')

from matplotlib import pyplot
import numpy as np


import gzip
import sys
import time
import os
import re
import glob
import json
from collections import defaultdict
import resource
import operator

from operation import Operation
import statistics

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

MONITOR_LINES = 100000

session_lengths = []
for i in range(1, 41):
    length = 60 * i
    session_lengths.append(length)

def prepare_sessions(data_dir):
    actions_per_sessions = {}
    for session_length in session_lengths:
        actions_per_sessions[session_length] = []

    session_files = glob.glob(data_dir+"/*.csv")
    cnt=0
    for user_session_file in session_files:
        cnt += 1
        print ("preparing %d / %d" % (cnt, len(session_files)))

        # if cnt == 200:
        #     break

        with open(user_session_file, 'rt') as sf:
            ops = list()

            for line in sf:
                op = Operation()
                op.init(line.strip())
                ops.append(op)
            ops.sort(key=operator.attrgetter('ts'))
            
            for session_length in session_lengths:
                # identify all sessions and store the actions per session
                action_counts = []
                actions = 0
                last_ts = 0
                for op in ops:
                    if actions == 0:
                        last_ts = op.ts + op.execution_time
                        actions = 1
                    else:
                        if op.ts - last_ts <= session_length:
                            # another action within the session
                            actions += 1
                        else:
                            # start of a new session
                            action_counts.append(actions)
                            actions = 1
                        last_ts = op.ts + op.execution_time

                if actions > 0:
                    action_counts.append(actions)

                actions_per_sessions[session_length] += (action_counts)

    for x in sorted(actions_per_sessions.keys()):
        print ("%d - count: %s" % (x, statistics.get_mean_string(actions_per_sessions[x])))

    return actions_per_sessions


def plot(actions_per_sessions, target_file):
    
    x_vals = list()
    y_vals = list()
    y_errs = list()
    
    for session_length in session_lengths:
        actions = actions_per_sessions[session_length]
        m, h = statistics.mean_confidence_interval_with_error(actions)
        x_vals.append(session_length)
        y_vals.append(m)
        y_errs.append(h)

    print (x_vals)
    print (y_vals)
    fig = pyplot.figure()
    # pyplot.xticks(x_axis, tags, rotation=45)
    # pyplot.xticks(x_vals, [x for x in range(len(x_vals))], fontsize=24)
    pyplot.yticks(fontsize=30)
    pyplot.xticks(fontsize=30)
    # pyplot.title('Count of Operations per Month', fontsize=40)
    pyplot.xlabel('Session window length in seconds', fontsize=30)
    pyplot.ylabel('Actions in sessions', fontsize=30)
    pyplot.grid(True)

    pyplot.errorbar(x_vals, y_vals, fmt='-*', yerr=y_errs)

    sizes = fig.get_size_inches()
    fig.set_size_inches(sizes[0]*2, sizes[1])
    
    pyplot.tight_layout()
    pyplot.savefig(target_file)
    print("saved %s" % (target_file))
    pyplot.close()

if __name__ == "__main__":

    if len(sys.argv) == 1:
        print ("usage: /path/to/USERS_SESSIONS_DATA_DIR /path/to/actions_per_session.pdf")
        sys.exit(1)

    data_dir = os.path.abspath(sys.argv[1])
    target_file = os.path.abspath(sys.argv[2])
    
    sessions = prepare_sessions(data_dir)
  
    plot(sessions, target_file)