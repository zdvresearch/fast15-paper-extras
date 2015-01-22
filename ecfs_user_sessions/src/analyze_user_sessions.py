#!/usr/bin/env python3
"""
    Categorize and analyze user sessions.
    Read in ecfs_obfuscated_filtered.gz file, output some fancy results. 
"""

from collections import defaultdict
from collections import Counter
import sys
import time
import os
import resource
import json
import fnmatch
from pipes import Pipes
import operator

from operation import Operation

KB = 1024
MB = KB * 1024
GB = MB * 1024
TB = GB * 1024
PB = TB * 1024

MONITOR_LINES = 100000


class UserSession():
    def __init__(self, user_id):
        self.user_id = user_id
        self.from_ts = 0
        self.till_ts = 0
        self.get_requests = 0
        self.reget_requests = 0
        self.put_requests = 0
        self.get_bytes = 0
        self.put_bytes = 0
        self.rename_requests = 0
        self.del_requests = 0
        self.get_dirs = 0
        self.put_dirs = 0
        self.put_files_per_dir = 0.0
        self.get_files_per_dir = 0.0
        self.window_seconds = 0

        self.file_cnt_gets = Counter()
        self.file_cnt_puts = Counter()
        self.dir_cnt_gets = Counter()
        self.dir_cnt_puts = Counter()

        self.num_ops = 0
        self.last_ts = 0

    def add_op(self, op):
        self.num_ops += 1

        if op.ts < self.last_ts:
            raise Exception("Timestamp too old")
        else:
            self.last_ts = op.ts

        if op.optype == 'g':
            self.get_requests += 1
            self.get_bytes += op.size
            self.file_cnt_gets[op.obj_id] += 1
            self.dir_cnt_gets[op.parent_dir_id] += 1
        elif op.optype == 'p':
            self.put_requests += 1
            self.put_bytes += op.size
            self.file_cnt_puts[op.obj_id] += 1
            self.dir_cnt_puts[op.parent_dir_id] += 1
        elif op.optype == 'd':
            self.del_requests += 1
        elif op.optype == 'r':
            self.rename_requests += 1

        #update last time stamp in the session
        self.till_ts = op.ts + op.execution_time

    def finish(self):
        self.get_dirs = len(self.dir_cnt_gets)
        if self.get_dirs > 0:
            self.get_files_per_dir = float(self.get_requests) / self.get_dirs

        self.put_dirs = len(self.dir_cnt_puts)
        if self.put_dirs > 0:
            self.put_files_per_dir = float(self.put_requests) / self.put_dirs

        """
        set reget_counter
        :param counter: contains [ 1, 1, 5] counts of objects. value > 1 is a re-retrieval.
        :return:
        """
        for c in self.file_cnt_gets.values():
            if c > 1:
                self.reget_requests += (c - 1)

        # self.announce()

        return ";".join([str(x) for x in [
        self.user_id,
        self.from_ts,
        self.till_ts,
        self.till_ts - self.from_ts,
        self.get_requests,
        self.reget_requests,
        self.put_requests,
        self.get_bytes,
        self.put_bytes,
        self.rename_requests,
        self.del_requests,
        self.get_dirs,
        self.put_dirs,
        self.put_files_per_dir,
        self.get_files_per_dir,
        self.window_seconds
        ]]
        )


    def announce(self):
        print("closed session. gets: %r, regets: %r, puts: %r, dels: %r, renames: %r get_dirs: %r, put_dirs: %r, get_bytes: %r put_bytes: %r window_seconds: %d" % \
              (self.get_requests, self.reget_requests, self.put_requests, self.del_requests, self.rename_requests, self.get_dirs, self.put_dirs, self.get_bytes, self.put_bytes, self.window_seconds))


def find_clusters(atimes):
    foo = Counter()
    bar = dict()
    for i in xrange(120, 3660, 10):
        clusters = get_clusters(atimes, i)
        cs = len(clusters)
        foo[cs] += 1

        # note first occurance of this cluster size.
        if cs not in bar:
            bar[cs] = i
        # print(len(atimes), i, cs)

    return bar[foo.most_common()[0][0]]

def get_clusters(data, maxgap):
    '''Arrange data into groups where successive elements
       differ by no more than *maxgap*

        >>> cluster([1, 6, 9, 100, 102, 105, 109, 134, 139], maxgap=10)
        [[1, 6, 9], [100, 102, 105, 109], [134, 139]]

        >>> cluster([1, 6, 9, 99, 100, 102, 105, 134, 139, 141], maxgap=10)
        [[1, 6, 9], [99, 100, 102, 105], [134, 139, 141]]
    '''
    data.sort()
    groups = [[data[0]]]
    for x in data[1:]:
        if abs(x - groups[-1][-1]) <= maxgap:
            groups[-1].append(x)
        else:
            groups.append([x])
    return groups


def analyze_user_session(user_session_file, out_pipeline, target_file_name):
    with open(user_session_file, 'r') as sf:
        ops = list()
        atimes = list()

        for line in sf:
            op = Operation()
            op.init(line.strip())
            ops.append(op)
            atimes.append(op.ts)

        ops.sort(key=operator.attrgetter('ts'))
        atimes.sort()
        window_seconds = find_clusters(atimes)

        session_counter = 1

        uf = os.path.basename(user_session_file)
        user_id = uf[:uf.find(".user_session.csv")]

        session = UserSession(user_id)
        session.window_seconds = window_seconds

        for op in ops:
            if session.from_ts == 0:
                    session.from_ts = op.ts
                    session.till_ts = op.ts + op.execution_time

            if (session.till_ts + window_seconds) < op.ts:
                # this session is over, so archive it.
                out_pipeline.write_to(target_file_name, session.finish())
                del session
                session = UserSession(user_id)
                session.window_seconds = window_seconds
                session_counter += 1
            
            session.add_op(op)

        if session.num_ops > 0:
            out_pipeline.write_to(target_file_name, session.finish())

        print("sessions: %d with window_seconds: %d" %(session_counter, window_seconds))


if __name__ == "__main__":
    source_dir = os.path.abspath(sys.argv[1])


    result = os.path.abspath(sys.argv[2])
    results_dir = os.path.dirname(result)
    target_file_name = os.path.basename(result)

    users_session_files = [os.path.join(dirpath, f)
        for dirpath, dirnames, files in os.walk(source_dir)
        for f in fnmatch.filter(files, '*.user_session.csv')]

    #remove the old log file, as outpipe is append only.
    if os.path.exists(os.path.join(results_dir, target_file_name)):
        os.remove(os.path.join(results_dir, target_file_name))

    out_pipe = Pipes(results_dir)
    
    csv_header = ";".join(["user_id",
                    "from_ts",
                    "till_ts",
                    "session_lifetime",
                    "get_requests",
                    "reget_requests",
                    "put_requests",
                    "get_bytes",
                    "put_bytes",
                    "rename_requests",
                    "del_requests",
                    "get_dirs",
                    "put_dirs",
                    "put_files_per_dir",
                    "get_files_per_dir",
                    "window_seconds"
                    ])

    out_pipe.write_to(target_file_name, csv_header)

    cnt = 0
    for sf in users_session_files:
        cnt += 1
        print ("working on %d/%d" % (cnt, len(users_session_files)))
        analyze_user_session(sf, out_pipe, target_file_name)

        # if cnt >=20:
        #     break

    out_pipe.close()

    print("wrote results to %s: " % (os.path.join(results_dir, target_file_name)))
    
    