#!/usr/bin/env python

"""obfuscate.py: Obfuscates ECFS access log files. Replaces all occurances of usernames and hostnames with their md5 hashes.
    
    version 1.1: also work on mars logs.
"""
__author__ = "Matthias Grawinkel"
__status__ = "Production"




import sys
import os
import gzip
import hashlib
import time
import resource
import shutil
import re


DEBUG = False
MONITOR_LINES=100000

def to_md5(s):
    m = hashlib.md5()
    m.update(s)
    return m.hexdigest()

def obfuscate_path(path):
    """
    in: /a/b.tgz
    return: /md5('a')/md5('b').tgz
    """
    DONT_REPLACE = ['mars', 'ecfs', 'TMP', 'tmp', 'temp', 'ec:', 'ectmp:']

    parts = [x.strip() for x in path.split("/")]

    filename = parts.pop()
    if filename.__contains__('.'):
        # assume to have an extension. split of right side of rightmost .
        basename = filename[:filename.rfind('.')]
        ext = filename[filename.rfind('.'):]
        filename = to_md5(basename) + ext
    else:
        filename = to_md5(filename)

    oe = []
    for p in parts:
        if p != '':
            if not p in DONT_REPLACE:
                p = to_md5(p)
            oe.append(p)
    oe.append(filename)

    opath = "/" + "/".join(oe)

    return opath


def obfuscate(source_file, target_file):

    path_re = re.compile("\/(ecfs|mars)\/([^ ]+)")
    start_time = time.time()
    with open(source_file, 'r') as source:
        with gzip.open(target_file, 'w') as target:
            t = time.time()
            plines = 0

            for line in source:

                # make a working copy of the current line
                oline = line

                plines += 1
                if plines % MONITOR_LINES == 0:
                    print ("processed lines: %d, mem: %rMB, lines/s: %r" %
                     (plines,
                      resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024 / 1024,
                      int(MONITOR_LINES / (time.time() - t))
                     )
                    )
                    t = time.time()


                # find all pathes in the line and replace their elements by their md5 sums
                pathes = path_re.findall(oline)

                for path in pathes:

                    # replace all occurances of the path by the hashed path
                    oline = oline.replace(path[1], obfuscate_path(path[1]))

                if DEBUG:
                    print "======================="
                    print "original: %s" % line
                    print "replaced: %s" % oline

                target.write(oline)

    print("finished. Total time: %d seconds" % (start_time - time.time()) )

if __name__ == "__main__":

    if len(sys.argv) != 3:
        print("usage: %s source_file target.gz" % sys.argv[0])
        sys.exit(1)

    source_file = os.path.abspath(sys.argv[1])
    target_file = os.path.abspath(sys.argv[2])

    if not os.path.exists(source_file):
        print("source: %s does not exist" % source_file)
        sys.exit(1)

    if os.path.exists(target_file):
        print("target_file: %s already exists" % target_file)
        sys.exit(1)

    obfuscate(source_file, target_file)