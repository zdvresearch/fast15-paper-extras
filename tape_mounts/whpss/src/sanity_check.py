#!/usr/bin/env python

import os
import gzip
import datetime
import time
import calendar
import sets
import re
import glob

class Timer():
    def __init__(self, s):
        self.s = s

    def __enter__(self):
        self.start = time.time()

    def __exit__(self, *args):
        print ("%s: %fs" % (self.s, (time.time() - self.start)))


# 05/26 00:15:46 ***
re_time = re.compile("([0-9]{2})/([0-9]{2}) ([0-9]{2}):([0-9]{2}):([0-9]{2}).*")
re_filename_date = re.compile("([0-9]{4})([0-9]{2})([0-9]{2})_([0-9]{2})([0-9]{2})([0-9]{2}).gz")

# epoch counter. assumed to be monotonically rising for every next entry
last_epoch = 0


# 05/25 00:19:27 -> 1370822690
def get_epoch(log_creation_date, log_entry_date):
    year = log_creation_date.year
    # when log_creation_date is 01/01, then the first entries are from the previous year
    if log_creation_date.month == 1 and log_creation_date.day == 1:
        if log_entry_date.month == 12 and log_entry_date.day == 31:
            log_entry_date = datetime.datetime(log_creation_date.year -1, log_entry_date.month, log_entry_date.day, log_entry_date.hour, log_entry_date.minute, log_entry_date.second)
    return  calendar.timegm(log_entry_date.utctimetuple())


def check(f):
    global last_epoch
    print os.path.abspath(f)

    m = re.search(re_filename_date, os.path.basename(f))
    if not m:
        print ("ERROR, cannot process invalid file name: %s" % (f))

    x = m.groups()
    log_creation_date = datetime.datetime(int(x[0]), int(x[1]),int(x[2]),int(x[3]),int(x[4]),int(x[5]))
    
    with gzip.open(f, 'r') as source:
        
        last_line = ""
        
        line_nr = 0

        try:
            for line in source.readlines():
                line_nr += 1
                match = re_time.search(line)

                if match:
                    x = match.groups()
                    # print x
                    log_entry_date = datetime.datetime(log_creation_date.year, int(x[0]), int(x[1]),int(x[2]),int(x[3]),int(x[4]))
    
                    epoch = get_epoch(log_creation_date, log_entry_date)
    
                    if epoch >= last_epoch:
                        # chronologic order
                        last_epoch = epoch
                    else:
                        print "chronologic error in line %d: \n\t%s \n\t%s" % (line_nr, last_line, line)
                # else:
                    # print ("bad line %d: %s" % (line_nr, line))

                last_line = line

                     
        except Exception,e :
            print(e)
            import traceback
            traceback.print_exc()
            raise e

    

cwd = os.path.dirname(__file__)

# natural sorting will preserve timely order.
files = sorted(glob.glob('whpss_log/whpss_log_*.gz'))

current_file = 1
num_files = len(files)
for filename in files:
    f = os.path.join(cwd, filename)
    with Timer("working: %d / %d  %s:" % (current_file, num_files, filename)):
        check(f)
    current_file += 1




