#!/usr/bin/env python3
"""
    Categorize and analyze user sessions.
    Read in ecfs_access_2012.01-2014.05.xz file, output one file per user session that contains analyseable details. 
"""

import lzma
import sys
import time
import os
import resource

from hashing import get_md5

from pipes import Pipes
from operation import Operation

"""
    the lines
"""
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

def prepare_user_sessions(source_file, data_dir):
    
    pipes = Pipes(data_dir, suffix=".user_session.csv")

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

            # if int(plines) >= int(1000000):
            #     print("BREAK")
            #     break

            elems = line.split('|')

            if len(elems) <= EXECUTION_TIME:
                continue

            if not elems[START_TIME].isdigit():
                print ("bad line: %s" % line)
                continue

            ts = int(elems[START_TIME])
            user_id = elems[USER_ID]
            host_id = elems[HOST_ID]

            execution_time = 0
            if elems[EXECUTION_TIME].isdigit():
                execution_time = int(elems[EXECUTION_TIME])

            op = Operation()
            if elems[REQUEST] == 'GET':
                if not elems[FILE_SIZE].isdigit():
                    continue
                
                op.ts = ts
                op.optype = 'g'
                op.obj_id = obj_id = get_md5(elems[PARAMS].strip(), hexdigest=True)
                op.parent_dir_id = get_md5(os.path.dirname(elems[PARAMS].strip()), hexdigest=True)
                op.size = int(elems[FILE_SIZE])
                op.execution_time = execution_time
                pipes.write_to(user_id + "_" + host_id, str(op))

            elif elems[REQUEST] == 'PUT':
                if not elems[FILE_SIZE].isdigit():
                    continue
                
                op.ts = ts
                op.optype = 'p'
                op.obj_id = obj_id = get_md5(elems[PARAMS].strip(), hexdigest=True)
                op.parent_dir_id = get_md5(os.path.dirname(elems[PARAMS].strip()), hexdigest=True)
                op.size = int(elems[FILE_SIZE])
                op.execution_time = execution_time
                pipes.write_to(user_id + "_" + host_id, str(op))

            elif elems[REQUEST] == 'DEL':
                op.ts = ts
                op.optype = 'd'
                op.obj_id = obj_id = get_md5(elems[PARAMS].strip(), hexdigest=True)
                op.parent_dir_id = get_md5(os.path.dirname(elems[PARAMS].strip()), hexdigest=True)
                op.execution_time = execution_time
                pipes.write_to(user_id + "_" + host_id, str(op))
            elif elems[REQUEST] == 'RENAME':
                op.ts = ts
                op.optype = 'r'
                op.obj_id = obj_id = get_md5(elems[PARAMS].strip(), hexdigest=True)
                op.parent_dir_id = get_md5(os.path.dirname(elems[PARAMS].strip()), hexdigest=True)
                op.execution_time = execution_time
                pipes.write_to(user_id + "_" + host_id, str(op))

    pipes.close()


if __name__ == "__main__":
    source_file = os.path.abspath(sys.argv[1])
    data_dir = os.path.abspath(sys.argv[2])
    prepare_user_sessions(source_file, data_dir)
