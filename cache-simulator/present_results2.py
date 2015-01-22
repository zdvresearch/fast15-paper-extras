# #!/usr/bin/env python

import matplotlib
matplotlib.use('Agg')

import matplotlib.pyplot as pyplot
import numpy as np
import pylab

import os
import sys
import glob
import json
import csv
import re
from collections import defaultdict
import datetime



class TestResult():
    _cache_sizes = [
        "tiny",
        "small",
        "medium",
        "large",
        "huge",
        "enormous"]

    _cache_fields = [
         "cache_fill_level",
         "cache_hit_ratio_requests",
         "cache_hits",
         "cache_misses",
         "cache_size",
         "cache_type",
         "cache_used",
         "cached_bytes_read",
         "cached_bytes_written",
         "cached_objects_current",
         "cached_objects_total",
         "deleted_objects",
         "evicted_objects",
         "first_eviction_ts"]

    def __init__(self, base_dir):
        self.base_dir = base_dir

        try:
            with open(os.path.join(self.base_dir, 'config.json'), 'r') as c:
                self.config = json.load(c)

            with open(os.path.join(self.base_dir, 'results.json'), 'r') as r:
                self.results = json.load(r)

            self._is_valid = True
        except Exception:
            self.__is_valid = False

    def is_valid(self):
        return self._is_valid

    def get_hr(self, bucket, start_date="None", end_date="None"):

        print (bucket, self.config["cache_config"])
        
        if start_date == "None":
            sd = None
        else:
            sd = datetime.datetime.strptime(start_date, "%Y-%m-%d")

        if end_date == "None":
            ed = None
        else:
            ed = datetime.datetime.strptime(end_date, "%Y-%m-%d")


        cnt = 0
        cache_hits = 0
        cache_misses = 0
        for date, value in self.results["stats"].items():
            if date == "totals":
                continue

            count = False
            
            if sd is None and ed is None:
                count = True
            elif sd is None:
                # just an end date is set
                td = datetime.datetime.strptime(date, "%Y-%m-%d")
                if td <= ed:
                    count = True
            elif ed is None:
                td = datetime.datetime.strptime(date, "%Y-%m-%d")
                if sd <= td:
                    count = True
            else:
                # both, start and end dates are set
                td = datetime.datetime.strptime(date, "%Y-%m-%d")

                if sd <= td <= ed:
                    # if the current date is greater than or equal the start date, count it
                    count = True

            if count:
                if bucket in value["stats"]["caches"]:
                    cache_hits += value["stats"]["caches"][bucket]["cache_hits"]
                    cache_misses += value["stats"]["caches"][bucket]["cache_misses"]
                    cnt += 1

        # if start_date is None:
        #     start_date = "-"
        # print("counted: %r %r %r" % (bucket, start_date, cnt))
        return float(cache_hits) / (cache_hits + cache_misses )

    def get_cache_warm_starting_date(self, bucket):

        start_date = "2012-01-10"

        window_size = 10
        threshold = 0.1
        current_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")

        prev_window_hr = 0.0
        for i in range(window_size, len(self.results["stats"])):
            hits_in_window = 0
            misses_in_window = 0
            for w in range(window_size):
                sd = (current_date - datetime.timedelta(days=w)).strftime("%Y-%m-%d")
                if sd in self.results["stats"]:
                    hits_in_window += self.results["stats"][sd]["stats"]["caches"][bucket]["cache_hits"]
                    misses_in_window += self.results["stats"][sd]["stats"]["caches"][bucket]["cache_misses"]
                else:
                    print("missing day: %r in %s" % (sd, self.config["name"]))
            window_hr = float(hits_in_window) / (hits_in_window + misses_in_window)

            print("%r = %r : delta: %r" % (current_date, window_hr, (window_hr - prev_window_hr)))
            if window_hr < prev_window_hr + threshold:
                print ("return:", current_date.strftime("%Y-%m-%d"))
                return current_date.strftime("%Y-%m-%d")
            else:
                #advance to next window
                current_date = current_date + datetime.timedelta(days=1)
                prev_window_hr = window_hr

        #fallback:
        print ("return fallback")
        return "2013-01-01"

    def get_cache_details_headline():
        headline = []
        for cs in TestResult._cache_sizes:
            for f in TestResult._cache_fields:
                headline.append("%s-%s" % (cs, f))
        return headline

    def get_cache_details_csv(self):
        values = []
        for cs in TestResult._cache_sizes:
            for f in TestResult._cache_fields:
                if cs in self.results["totals"]["caches"]:
                    values.append("%r" % self.results["totals"]["caches"][cs][f])
                else:
                    values.append(0)
        return values

    def get_totals_headline():
        return [
           "bytes_read",
           "bytes_written",
           "bytes_read",
           "bytes_written",
           "cache_hit_ratio_bytes",
           "cache_hit_ratio_requests",
           "cache_hits",
           "cache_hits_bytes",
           "cache_misses",
           "cache_misses_bytes",
           "del_requests",
           "get_requests",
           "put_overwrites",
           "put_requests",
           "rename_requests"
        ] + TestResult.get_cache_details_headline()

    def to_totals_csv(self):
        return [
            self.results["totals"]["front"]["bytes_read"],
            self.results["totals"]["front"]["bytes_written"],
            self.results["totals"]["front"]["bytes_read"],
            self.results["totals"]["front"]["bytes_written"],
            self.results["totals"]["front"]["cache_hit_ratio_bytes"],
            self.results["totals"]["front"]["cache_hit_ratio_requests"],
            self.results["totals"]["front"]["cache_hits"],
            self.results["totals"]["front"]["cache_hits_bytes"],
            self.results["totals"]["front"]["cache_misses"],
            self.results["totals"]["front"]["cache_misses_bytes"],
            self.results["totals"]["front"]["del_requests"],
            self.results["totals"]["front"]["get_requests"],
            self.results["totals"]["front"]["put_overwrites"],
            self.results["totals"]["front"]["put_requests"],
            self.results["totals"]["front"]["rename_requests"]
        ] + self.get_cache_details_csv()


def movingaverage(interval, window_size):
    window = np.ones(int(window_size))/float(window_size)
    return np.convolve(interval, window, 'same')


def create_bucket_compare_plots(results_dir, test_results,  target_dir, ecfs_base_cache_hit_ratio_file):

    font = {'family' : 'normal',
             'size'   : 22}

    matplotlib.rc('font', **font)

    ticks = dict()
    
    #each 15 steps
    ticks["Tiny"] =     ["1GB",        "2GB",    "4GB",     "8GB",   "16GB",    "32GB",   "64GB",  "128GB",  "256GB", "512GB",    "1TB",    "2TB",   "4TB",    "8TB",    "$\infty$"]
    ticks["Small"] =    ["16GB",      "32GB",   "64GB",    "96GB", "128GB",    "192GB",  "256GB",  "512GB",  "768GB",   "1TB",    "2TB", "2560GB",   "3TB",    "4TB",    "$\infty$"]
    ticks["Medium"] =   ["512GB",    "768GB",    "1TB",     "2TB",    "3TB",     "4TB",    "5TB",    "6TB",    "8TB",  "12TB",   "16TB",   "20TB",   "24TB",   "28TB",   "$\infty$"]
    ticks["Large"] =    ["256GB",    "512GB",    "1TB",     "2TB",   "4TB",     "8TB",    "16TB",   "24TB",   "32TB",  "48TB",   "64TB",   "80TB",   "96TB",   "108TB",  "$\infty$"]
    ticks["Huge"] =     ["8TB",       "16TB",   "32TB",    "64TB",  "128TB",   "160TB",   "192TB",  "256TB",  "384TB", "512TB",  "640TB",  "768TB", "1,024TB",  "1,280TB", "$\infty$"]
    ticks["Enormous"] = ["32TB",      "48TB",   "64TB",    "96TB",  "128TB",   "256TB",   "512TB",  "768TB",    "1PB", "1.5PB",   "2PB",  "2.5PB",    "3PB", "3.5PB", "$\infty$"]
    ticks["ALL"] =      [ "40.8TB", "65.3TB", "98.1TB", "164.1TB", "263.1TB", "428.2TB", "725.3TB", "1.03PB", "1.42PB", "2.06PB", "2.7PB", "3.35PB", "4.12PB", "4.89PB", "$\infty$"]

    caches = ["MRUCache",  "FifoCache", "RandomCache", "LRUCache", "BeladyCache", "ARCCache"]
    # caches = ["MRUCache",  "FifoCache", "RandomCache", "LRUCache", "BeladyCache", "ARCCache", "SplitLRU"]

    cache_line_types = {
        "MRUCache" : "-1",
        "FifoCache" : "--x",
        "RandomCache" : ":D",
        "LRUCache" : "-8",
        "BeladyCache" : "-s",
        "ARCCache" : "--*",
        "SplitLRU" : ":o"
    }

    cache_labels = {
        "MRUCache" : "MRU",
        "FifoCache" : "Fifo",
        "RandomCache" : "Random",
        "LRUCache" : "LRU",
        "BeladyCache" : "Belady",
        "ARCCache" : "ARC",
        "SplitLRU" : "LRU|LRU"
    }

    for bucket_size in ["Tiny", "Small", "Medium", "Large", "Huge", "Enormous", "ALL"]:
        # draw one plot per bucket size and for totals
        for time_frame in [("None", "2012-12-31"), ("2013-01-01", "None")]:
            warm_up_date = time_frame[0]
            end_date = time_frame[1]

            values = defaultdict(list)

            if bucket_size == "ALL":
                for i in range(len(ticks["ALL"])):
                    for cache_type in caches:
                        active_tests = [tr for tr in test_results if tr.config["cache_config"].__contains__("ecmwf_ALL-%d_" % i) and tr.config["cache_config"].__contains__("_%s" % (cache_type))]
                        hit_ratio = 0.0
                        if len(active_tests) > 0:
                            hrs = list()
                            for test in active_tests:
                                hr = test.get_hr("ALL", start_date=warm_up_date, end_date=end_date)
                                hrs.append(hr)
                            hit_ratio = sum(hrs) / len(hrs)
                        else:
                            print("ERROR: missing test %s %d:" % ("ALL", i))
                        # print(bucket_size, i, cache_type, hit_ratio)
                        values[cache_type].append(hit_ratio)
            else:
                for i in range(len(ticks["Tiny"])):
                    for cache_type in caches:
                        active_tests = [tr for tr in test_results if tr.config["cache_config"].__contains__("ecmwf_%d_" % i) and tr.config["cache_config"].__contains__("_%s" % (cache_type))]
                        hit_ratio = 0.0
                        if len(active_tests) > 0:
                            hrs = list()
                            for test in active_tests:
                                hr = test.get_hr(bucket_size, start_date=warm_up_date, end_date=end_date)
                                hrs.append(hr)
                            hit_ratio = sum(hrs) / len(hrs)
                        else:
                            print("ERROR: missing test %s %d:" % (cache_type, i))
                        # print(bucket_size, i, cache_type, hit_ratio)
                        values[cache_type].append(hit_ratio)

            #plot it
            target_file = os.path.join(target_dir, "%s_%s_cache_hits.pdf" % (bucket_size, warm_up_date))

            fig, ax = pyplot.subplots()
    
            x_vals = np.arange(len(ticks["Tiny"]))

            for ct in caches:
                ax.plot(x_vals, values[ct], cache_line_types[ct], linewidth=1, color='k', label=cache_labels[ct])
            
            if bucket_size != "ALL":
                ecmwf_hit_ratio_total = get_ecfs_baseline_total(ecfs_base_cache_hit_ratio_file, start_date=warm_up_date, end_date=end_date, bucket=bucket_size)
                pyplot.axhline(y=ecmwf_hit_ratio_total, linewidth=1, color='r', label="ECMWF baseline")

            ax.set_ylabel('Cache Hit Ratio', fontsize=30)

            ax.set_xticks(x_vals)
            ax.set_xticklabels(ticks[bucket_size], rotation=45)
            ax.tick_params(labelsize=14)
            ax.set_ylim(0, 1)
            # ax.set_xlim(1,len(total_size_per_size_category)+1)
            ax.yaxis.grid(True)
            ax.xaxis.grid(True)
            ax.tick_params(labelsize=20)

            sizes = fig.get_size_inches()
            fig.set_size_inches(sizes[0]*1.7, sizes[1])

            handles,labels = ax.get_legend_handles_labels()

            pyplot.tight_layout()
            pyplot.savefig(target_file)
            print("saved %s" % (target_file))
            pyplot.close()

            pyplot.clf()

            fig, ax = pyplot.subplots()
            ax.legend(handles, labels, fancybox=True, shadow=False, ncol=7, fontsize=20)
            ax.xaxis.set_visible(False)
            ax.yaxis.set_visible(False)
            pyplot.axis('off')

            sizes = fig.get_size_inches()
            fig.set_size_inches(sizes[0]* 0.1, sizes[1])
            pyplot.tight_layout()

            pyplot.savefig(os.path.join(target_dir, "legend.pdf"), bbox_inches='tight', pad_inches=0.1)
            pyplot.close()



# def get_ecfs_baseline(ecfs_base_cache_hit_ratio_file, bucket=None):
#     baseline = {}
#     with open(ecfs_base_cache_hit_ratio_file, 'r') as f:
#         ratios = json.load(f)

#         for date, values in ratios.items():
#             if bucket is None:
#                 baseline[date] = values["total"]["hit_ratio_requests"]
#             else:
#                 baseline[date] = values["ecmwf"][bucket]["hit_ratio_requests"]

#     return baseline

def get_ecfs_baseline_total(ecfs_base_cache_hit_ratio_file, start_date="None", end_date="None", bucket=None):
    with open(ecfs_base_cache_hit_ratio_file, 'r') as f:
        ratios = json.load(f)

        if start_date == "None":
            sd = None
        else:
            sd = datetime.datetime.strptime(start_date, "%Y-%m-%d")

        if end_date == "None":
            ed = None
        else:
            ed = datetime.datetime.strptime(end_date, "%Y-%m-%d")

        disk_requests = 0
        tape_requests = 0

        for date, values in ratios.items():
            count = False
            
            if sd is None and ed is None:
                count = True
            elif sd is None:
                # just an end date is set
                td = datetime.datetime.strptime(date, "%Y-%m-%d")
                if td <= ed:
                    count = True
            elif ed is None:
                td = datetime.datetime.strptime(date, "%Y-%m-%d")
                if sd <= td:
                    count = True
            else:
                # both, start and end dates are set
                td = datetime.datetime.strptime(date, "%Y-%m-%d")

                if sd <= td <= ed:
                    # if the current date is greater than or equal the start date, count it
                    count = True

            if count:
                if bucket is None:
                    if "total" in values:
                        disk_requests += values["total"]["disk_requests"]
                        tape_requests += values["total"]["tape_requests"]
                else:
                    if bucket in values["ecmwf"]:
                        disk_requests += values["ecmwf"][bucket]["disk_requests"]
                        tape_requests += values["ecmwf"][bucket]["tape_requests"]
    return float(disk_requests) / (disk_requests + tape_requests)


if __name__ == "__main__":
    results_dir = sys.argv[1]
    target_dir = sys.argv[2]
    ecfs_base_cache_hit_ratio_file = sys.argv[3]

    print ("version 4")
    test_results = []

    # read all the results into one big dict
    for r in glob.glob(results_dir+"/**/results.json"):
        print (r)
        with open(r, 'r') as f:
            test_result = TestResult(os.path.join(os.path.dirname(r)))

            if test_result.is_valid():
                test_results.append(test_result)
                # test_result.plot_cache_fill_levels(baseline=ecfs_baseline)
                # test_result.plot_cache_hit_ratios(baseline=ecfs_baseline)

    print("data loaded. Now plotting.")
    # now, draw some conclusions...
    create_bucket_compare_plots(results_dir, test_results, target_dir, ecfs_base_cache_hit_ratio_file)