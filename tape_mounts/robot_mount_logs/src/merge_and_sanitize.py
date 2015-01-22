#!/usr/bin/env python

import os
import gzip
import sys
import time
import calendar
import collections
import re
import glob

# assume all timestamps to be UTC.


# also add the parent folder of this file to the python search path.
module_base_dir = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
sys.path.append(module_base_dir)

from TimeTools import get_epochs, Timer


##  hdre01#VLAD#20100427:001756#  DISMOUNT C25895 Home 0,3,19,4,10 Drive 0,3,10,8 Client Host Id 136.156.216.161
re_line = re.compile(".*([0-9]{8}:[0-9]{6}).* (ACSMV|ACSCR|AUDIT|EJECT|ENTER|MOUNT|DISMOUNT) .*")



def sort_in(source_dir, target_dir, broken_lines_file):
    """
    Reads in all robot_mount_log_vlad*.gz.
    For every log line, the epoch is calculated and the files are appended into monthly log files.
    Whenever the monotonicity of the log line's dates is not given, the line is written to a
    broken_lines.gz file which is later merged back into the logfiles.
    @param source_dir:
    @param target_dir:
    @param broken_lines_file:
    @return:
    """
    file_pointers = dict()
    files = sorted(glob.glob(os.path.join(source_dir, 'robot_mount_log_vlad*.gz')))

    current_file = 1
    num_files = len(files)

    for f in files:
        print os.path.abspath(f)
    print "source_dir", source_dir
    print "target_dir", target_dir

    last_epoch = 0

    broken_lines = gzip.open(broken_lines_file, 'w')

    for sf in files:
        with gzip.open(sf, 'r') as source_file:
            current_file += 1
            print ("processing %d/%d" % (current_file, num_files))
            for line in source_file:
                match = re_line.search(line)
                if match:
                    g = match.groups()
                    t = time.strptime(g[0], '%Y%m%d:%H%M%S')
                    epoch = calendar.timegm(t)

                    if epoch >= last_epoch:
                        last_epoch = epoch
                        target_file = "robot_mounts_log_%s-%s.log.gz" % (t.tm_year, t.tm_mon)

                        if target_file not in file_pointers:
                            file_pointers[target_file] = gzip.open(os.path.join(target_dir, target_file), 'w')

                        file_pointers[target_file].write(line)
                    else:
                        broken_lines.write(line)

    broken_lines.close()
    for key, fp in file_pointers.items():
        fp.close()


def merge_in_broken_lines(broken_lines_files, target_dir):
    """
    sort_in() creates a broken lines files with lines that did not fit into the monotonic order.
    Some of the lines had a delay of multiple hours, so they were thrown out.
    Now, open this file, sort the lines by their time and sort them into the target sanitized files
    to the right positions. For any updated log file, a merged_%s logfile is created that should replace
    the original. After that, the broken lines file can be deleted.
    @param broken_lines_files:
    @param target_dir:
    @return:
    """

    content = dict()
    #first, open and sort the broken lines
    with gzip.open(broken_lines_files, 'r') as sf:
        for line in sf:
            match = re_line.search(line)
            if match:
                g = match.groups()
                t = time.strptime(g[0], '%Y%m%d:%H%M%S')

                target_file = "robot_mounts_log_%s-%s.log.gz" % (t.tm_year, t.tm_mon)

                if target_file not in content:
                    content[target_file] = dict()

                epoch = calendar.timegm(t)
                if not epoch in content[target_file]:
                    content[target_file][epoch] = []
                content[target_file][epoch].append(line)

    # then, for every target file
    for target_file in content.iterkeys():

        cnt = 0
        for epoch, lines in content[target_file].items():
            cnt += len(lines)
        print ("target_file: %s - lines: %d" % (target_file, cnt))

        #sort the broken lines into an order preserving orderedDict
        broken_lines = collections.OrderedDict(sorted(content[target_file].items()))

        with gzip.open(os.path.join(target_dir, target_file), 'r') as source:
            with gzip.open(os.path.join(target_dir, "merged_%s" % target_file), 'w') as target:
                #pop the first broken line

                lcount = 0
                bcount = 0
                dupes = 0
                bline_epoch, blines_array = broken_lines.popitem(last=False)
                bline = blines_array[0]
                # throw away first entry. Results in an empty [], when last one is removed.
                blines_array = blines_array[1:]

                for line in source:
                    match = re_line.search(line)
                    if match:
                        g = match.groups()
                        t = time.strptime(g[0], '%Y%m%d:%H%M%S')
                        epoch = calendar.timegm(t)

                        if not bline:
                            # simple case, no blines left.
                            lcount += 1
                            target.write(line)
                        elif bline.strip() == line.strip():
                            dupes += 1
                            target.write(bline)
                            # and skip the line

                             # now, get a new bline or set bline to None
                            if len(blines_array) > 0:
                                bline = blines_array[0]
                                blines_array = blines_array[1:]
                            else:
                                if len(broken_lines) == 0:
                                    # no more blines available
                                    bline = None
                                else:
                                    bline_epoch, blines_array = broken_lines.popitem(last=False)
                                    bline = blines_array[0]
                                    blines_array = blines_array[1:]
                        else:
                            if bline_epoch > epoch:
                                # broken line available, but current line is older
                                lcount += 1
                                target.write(line)
                            else:
                                # write as many broken lines as possible
                                while bline and bline_epoch <= epoch:
                                    bcount += 1
                                    target.write(bline)
                                     # now, get a new bline or set bline to None
                                    if len(blines_array) > 0:
                                        bline = blines_array[0]
                                        blines_array = blines_array[1:]
                                    else:
                                        if len(broken_lines) == 0:
                                            # no more blines available
                                            bline = None
                                        else:
                                            bline_epoch, blines_array = broken_lines.popitem(last=False)
                                            bline = blines_array[0]
                                            blines_array = blines_array[1:]
                                lcount += 1
                                target.write(line)


                # are there broken lines that need to be inserted after the last "ok" line?
                while bline:
                    bcount += 1
                    target.write(bline)
                    # now, get a new bline or set bline to None
                    if len(blines_array) > 0:
                        bline = blines_array[0]
                        blines_array = blines_array[1:]
                    else:
                        if len(broken_lines) == 0:
                            # no more blines available
                            bline = None
                        else:
                            bline_epoch, blines_array = broken_lines.popitem(last=False)
                            bline = blines_array[0]
                            blines_array = blines_array[1:]


                print ("lines: %d, blines: %d, dupes: %d" % (lcount, bcount, dupes))
                        # either no blines are left, or the read line is older than bline's epoch
                        #if not bline or (bline_epoch > epoch):
                        #    lcount += 1
                        #    target.write(line)
                        #else:
                        #    bcount += 1
                        #    target.write(bline)
                        #
                        #    # now, get a new bline or set bline to None
                        #    if len(blines_array) > 0:
                        #        bline = blines_array[0]
                        #        blines_array = blines_array[1:]
                        #    else:
                        #        if len(broken_lines) == 0:
                        #            # no more blines available
                        #            bline = None
                        #        else:
                        #            bline_epoch, blines_array = broken_lines.popitem(last=False)
                        #            bline = blines_array[0]
                        #            blines_array = blines_array[1:]

    print ("now, its safe to delete file: %s" % (os.path.abspath(broken_lines_files)))


def check_sanity(sanitized_logs_dir):
    files = sorted(glob.glob(os.path.join(sanitized_logs_dir, 'robot_mounts_*.gz')))
    current_file = 1
    num_files = len(files)

    for source_file in files:
        print os.path.abspath(source_file)
        with gzip.open(source_file, 'r') as sf:
            lcount = 0
            last_epoch = 0
            for line in sf:
                match = re_line.search(line)
                lcount += 1
                if lcount % 100000 == 0:
                    print (lcount)
                if match:
                    g = match.groups()
                    t = time.strptime(g[0], '%Y%m%d:%H%M%S')
                    epoch = calendar.timegm(t)

                    if epoch >= last_epoch:
                        last_epoch = epoch
                    else:
                        print ("bad line: %s" % line)
                else:
                    print ("bad line: %s" % line)


def main(argv):
    if len(argv) < 2:
        sys.stderr.write("Usage: %s (sanitize_merge|insert_broken|check_log_file_sanity) args" % (argv[0],))
        return 1

    if argv[1] == 'sanitize_merge':
        print 'sanitize_merge'
        source_dir = os.path.abspath(argv[2])
        target_dir = os.path.abspath(argv[3])
        broken_lines_file = os.path.abspath(argv[4])
        print source_dir
        print target_dir
        print broken_lines_file
        sort_in(source_dir, target_dir, broken_lines_file)

    elif argv[1] == 'insert_broken':
        print 'insert_broken'
        broken_lines_file = os.path.abspath(argv[2])
        target_dir = os.path.abspath(argv[3])
        merge_in_broken_lines(broken_lines_file, target_dir)

    elif argv[1] == 'check_log_file_sanity':
        sanitized_logs_dir = os.path.abspath(argv[2])
        check_sanity(sanitized_logs_dir)

if __name__ == "__main__":
    sys.exit(main(sys.argv))