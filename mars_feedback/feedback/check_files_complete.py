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
import datetime

"""
for a given time range, check that all reader logs exist.
Then merge them all into one big file and sort them.
"""


def get_timestamps_set(start_date, end_date, time_format="%Y%m%d"):
    """
        start_date = "20120110"
        end_date = "20140520"
        time_format = "%Y-%m-%d"
    """
    dates = set()
    test_date = start_date
    while test_date != end_date:
        # print(test_date)
        current_date = datetime.datetime.strptime(test_date, time_format)
        current_date = current_date + datetime.timedelta(days=1)
        test_date = current_date.strftime(time_format)
        dates.add(test_date)
    return dates


def get_file_dates(source_dir):
    dates = set()
    for filename in glob.glob(os.path.join(source_dir, "*filtered.gz")):
        source_file = os.path.basename(filename)
        print(source_file)
        d = source_file[:8]
        print(d)
        dates.add(d)
    return dates

def check_file_dates(source_dir):
    expected_dates = get_timestamps_set("20100110", "20140227")
    found_dates = get_file_dates(source_dir)
    # print(expected_dates ^ found_dates)

    for i in sorted(found_dates ^ expected_dates):
        print ("missing: %r" % i)

    print("========================")
    print("expected: %d" % (len(expected_dates)))
    print("found: %d" % (len(found_dates)))
    print("union: %d" % (len(expected_dates & found_dates)))
    missing = expected_dates ^ found_dates
    print("missing %d" % (len(missing)))

    
if __name__ == "__main__":
    source_dir = "/home/shared/meatz/ecmwf_data/traces/mars/feedback_filtered/"
    check_file_dates(source_dir)

