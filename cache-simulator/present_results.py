# #!/usr/bin/env python

import matplotlib
matplotlib.use('Agg')

import matplotlib.pyplot as plt
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

    def get_hr(self, bucket, start_date="None"):
        if start_date == "None":
            sd = None
        else:
            sd = datetime.datetime.strptime(start_date, "%Y-%m-%d")

        cnt = 0
        cache_hits = 0
        cache_misses = 0
        for date, value in self.results["stats"].items():
            if date == "totals":
                continue

            count = False
            if sd is None:
                count = True
            else:
                td = datetime.datetime.strptime(date, "%Y-%m-%d")

                if sd <= td:
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


    def plot_cache_fill_levels(self, baseline=None):
        target_file = os.path.join(self.base_dir, "cache_fill_levels.png")

        # prepare data
        dates = []
        for key in self.results["stats"].keys():
            if re.match(r'\d+-\d+-\d+', key):
                dates.append(key)

        dates = sorted(dates)

        cache_fill_levels = defaultdict(list)

        for date in dates:
            for cs in self._cache_sizes:
                if cs in self.results["stats"][date]["stats"]["caches"]:
                    cfl = self.results["stats"][date]["stats"]["caches"][cs]["cache_fill_level"]
                    cache_fill_levels[cs].append(cfl)

        #plot it
        fig, ax = plt.subplots()

        index = np.arange(len(dates))

        line_types = ["-", "--", "-.", ":", "-", "--", "-.", ":", "-", "--", "-.", "::"]
        i = 0
        for cache_size in cache_fill_levels.keys():

            plt.plot(index, cache_fill_levels[cache_size],line_types[i], label=cache_size)
            i += 1

        plt.grid(True)
        plt.ylabel('Cache fill Level in percent')
        plt.title('Cache Fill Levels over time - %s' % (self.config["cache_config"]))

        ax.legend(fancybox=True, shadow=False, title="Cache sizes")

        plt.tight_layout()

        plt.savefig(target_file)
        plt.close()

    def plot_cache_hit_ratios(self, baseline=None):
        target_file = os.path.join(self.base_dir, "cache_hit_ratios.png")

        # prepare data
        dates = []
        for key in self.results["stats"].keys():
            if re.match(r'\d+-\d+-\d+', key):
                dates.append(key)

        dates = sorted(dates)

        cache_fill_levels = defaultdict(list)
        totals = list()

        if baseline != None:
            blv = []
        else:
            blv = None


        for date in dates:
            for cs in self._cache_sizes:
                if cs in self.results["stats"][date]["stats"]["caches"]:
                    cfl = self.results["stats"][date]["stats"]["caches"][cs]["cache_hit_ratio_requests"]
                    cache_fill_levels[cs].append(cfl)

            # print front
            cfl = self.results["stats"][date]["stats"]["front"]["cache_hit_ratio_requests"]
            totals.append(cfl)

            # print the baseline
            if blv != None:
                blv.append(baseline[date])

        #plot it
        fig, ax = plt.subplots()

        index = np.arange(len(dates))

        line_types = ["-", "--", "-.", ":", "-", "--", "-.", ":", "-", "--", "-.", "::"]
        i = 0
        for cache_size in cache_fill_levels.keys():
            smoothed = movingaverage(cache_fill_levels[cache_size], 10)
            plt.plot(index, smoothed, line_types[i], label=cache_size)
            i += 1

        # print thick line for front
        smoothed = movingaverage(totals, 10)
        plt.plot(index, smoothed,"-",color='b', linewidth=2.0, label="total")

        if blv != None:
            smoothed = movingaverage(blv, 10)
            plt.plot(index, smoothed,"-",color='y', linewidth=2.0, label="baseline")

        plt.grid(True)
        plt.ylabel('Cache Hit Ratios in percent')
        plt.title('Cache Fill Ratios over time - %s' % (self.config["cache_config"]))

        ax.legend(fancybox=True, shadow=False, title="Cache sizes")

        plt.tight_layout()

        plt.savefig(target_file)
        plt.close()

def movingaverage(interval, window_size):
    window = np.ones(int(window_size))/float(window_size)
    return np.convolve(interval, window, 'same')

def to_csv(test_results, outfile_path):
    with open(outfile_path, 'w') as csvfile:
        writer = csv.writer(csvfile, delimiter=";")
        writer.writerow(TestResult.get_totals_headline())

        for test_result in test_results:
            writer.writerow(test_result.to_totals_csv())

    print ("wrote summary to: %s" % (os.path.abspath(outfile_path)))


def get_ecfs_baseline(ecfs_base_cache_hit_ratio_file, bucket=None):
    baseline = {}
    with open(ecfs_base_cache_hit_ratio_file, 'r') as f:
        ratios = json.load(f)

        for date, values in ratios.items():
            if bucket is None:
                baseline[date] = values["total"]["hit_ratio_requests"]
            else:
                baseline[date] = values["ecmwf"][bucket]["hit_ratio_requests"]

    return baseline

def get_ecfs_baseline_total(ecfs_base_cache_hit_ratio_file, start_date="None", bucket=None):
    with open(ecfs_base_cache_hit_ratio_file, 'r') as f:
        ratios = json.load(f)

        if start_date == "None":
            sd = None
        else:
            sd = datetime.datetime.strptime(start_date, "%Y-%m-%d")

        disk_requests = 0
        tape_requests = 0
        for date, values in ratios.items():
            count = False
            if sd is None:
                count = True
            else:
                td = datetime.datetime.strptime(date, "%Y-%m-%d")

                if sd <= td:
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


# def create_bucket_compare_plots(results_dir, test_results, ecfs_base_cache_hit_ratio_file):
#     ticks = dict()
#     ticks["Tiny"] =     ["1GB",     "4GB",   "8GB",  "16GB",   "32GB",  "64GB",  "128GB",  "256GB", "512GB", "$\infty$"]
#     ticks["Small"]  =   ["32GB",   "64GB",  "96GB", "128GB",  "192GB", "256GB",  "512GB",  "768GB",   "1TB", "$\infty$"]
#     ticks["Medium"] =   ["512GB", "768GB",   "1TB",   "2TB",    "3TB",   "4TB",    "5TB",    "6TB",   "8TB", "$\infty$"]
#     ticks["Large"] =    ["4TB",     "8TB",  "16TB",  "24TB",   "32TB",  "48TB",   "64TB",   "80TB",  "96TB", "$\infty$"]
#     ticks["Huge"] =     ["32TB",   "64TB", "128TB", "256TB",  "384TB", "512TB",  "768TB", "1024TB","1280TB", "$\infty$"]
#     ticks["Enormous"] = ["64TB",  "128TB", "256TB", "512TB",  "768TB",   "1PB", "1536TB",    "2PB",   "3PB", "$\infty$"]
#
#
#     buckets = ["Tiny", "Small", "Medium", "Large", "Huge", "Enormous"]
#
#     caches = ["MRUCache",  "FifoCache", "RandomCache", "LRUARCCache", "LRUCache"]
#
#     warm_up_dates = ["None", "Warm"]
#
#     warm_up_line_types = ["-", "--", ":", "-."]
#     for bucket_size in buckets:
#         # draw one plot per bucket size and for totals
#
#         values = dict()
#         for warm_up_date in warm_up_dates:
#             values[warm_up_date] = defaultdict(list)
#
#         for i in range(10):
#             for cache_type in caches:
#                 for wud in warm_up_dates:
#                     active_tests = [tr for tr in test_results if tr.config["cache_config"].__contains__("ecmwf_%d" % i) and tr.config["cache_config"].__contains__(cache_type)]
#                     hit_ratio = 0.0
#                     if len(active_tests) > 0:
#                         hrs = list()
#                         for test in active_tests:
#                             if wud != "None":
#                                 warm_up_date = test.get_cache_warm_starting_date(bucket_size)
#                                 hr = test.get_hr(bucket_size, start_date=warm_up_date)
#                             else:
#                                 hr = test.get_hr(bucket_size)
#                             hrs.append(hr)
#                         hit_ratio = sum(hrs) / len(hrs)
#                     # print(bucket_size, i, cache_type, hit_ratio)
#                     values[wud][cache_type].append(hit_ratio)
#
#         #plot it
#         target_file = os.path.join(results_dir, "%s_cache_hits_.pdf" % (bucket_size))
#
#         fig, ax = plt.subplots()
#         index = np.arange(len(values["None"]["LRUCache"]))
#         width = 0.15
#         colors = [ "0.7", "0.6", "0.5", "0.4", "0.3", "0.2", "0.1"]
#         cindex = 0
#
#
#         # #Check Values:
#         # for i in range(len(values["None"]["LRUCache"])):
#         #     for cache in caches:
#         #         # v = 0.0
#         #         # for wud in warm_up_dates:
#         #         #     v_n = values[wud][cache][i]
#         #         #     print ("%r - %r - %r: %r" % (wud, cache, i, v_n))
#         #         #     if v_n < v:
#         #         #         print ("ERROR!!! %r - %r - %r" % (wud, cache, i))
#         #         #
#         #         #         # sys.exit(1)
#         #         #     v = v_n
#         #
#         #         v = 0.0
#         #         for wud in warm_up_dates:
#         #             v_n = values[wud][cache][i]
#         #             print ("%r - %r - %r: %r" % (wud, cache, i, v_n))
#         #             if v_n < v:
#         #                 print ("ERROR!!! %r - %r - %r" % (wud, cache, i))
#         #
#         #                 # sys.exit(1)
#         #             v = v_n
#
#
#
#         for c, v in values["None"].items():
#             bar = ax.bar(index + (cindex * width), v, width, label=c, color=colors[cindex])
#             cindex += 1
#
#
#
#         #plot ecmwf_hit_ratio base line
#         # lti = 0
#         # for warm_up_date in warm_up_dates:
#         #     lti += 1
#         #     ecmwf_hit_ratio_total = get_ecfs_baseline_total(ecfs_base_cache_hit_ratio_file, start_date=warm_up_date, bucket=bucket_size)
#         #     plt.axhline(y=ecmwf_hit_ratio_total, linestyle=warm_up_line_types[lti % len(warm_up_line_types)], linewidth=1, color='r', label=warm_up_date)
#
#
#
#         ax.yaxis.grid(True)
#
#         plt.title('Cache Hit Ratio - %s' % (bucket_size))
#         plt.ylabel('Cache Hit Ratio')
#
#         plt.xticks(np.arange(len(ticks[bucket_size])), ticks[bucket_size])
#         # ax.tick_params(direction='out', pad=15)
#         # ax.get_xaxis().get_major_formatter().set_useOffset(False)
#         # ax.xaxis.set_major_locator( xmajorLocator )
#         # ax.xaxis.set_minor_locator( xminorLocator )
#
#
#         sizes = fig.get_size_inches()
#         fig.set_size_inches(sizes[0]*3, sizes[1])
#         plt.tight_layout()
#
#         handles,labels = ax.get_legend_handles_labels()
#
#         plt.savefig(target_file)
#         plt.close()
#
#         plt.clf()
#
#         fig, ax = plt.subplots()
#         ax.legend(handles, labels, fancybox=True, shadow=False, ncol=6, title="Cache Strategy")
#         ax.xaxis.set_visible(False)
#         ax.yaxis.set_visible(False)
#         plt.axis('off')
#
#         sizes = fig.get_size_inches()
#         fig.set_size_inches(sizes[0]* 0.1, sizes[1])
#         plt.tight_layout()
#
#         plt.savefig(os.path.join(results_dir, "legend.pdf"), bbox_inches='tight', pad_inches=0.1)
#         plt.close()
def create_bucket_compare_plots(results_dir, test_results, ecfs_base_cache_hit_ratio_file, target_dir):

    font = {'family' : 'normal',
             'size'   : 22}

    matplotlib.rc('font', **font)


    ticks = dict()
    ticks["Tiny"] =     ["1GB",     "4GB",   "8GB",  "16GB",   "32GB",  "64GB",  "128GB",  "256GB", "512GB", "$\infty$"]
    ticks["Small"]  =   ["32GB",   "64GB",  "96GB", "128GB",  "192GB", "256GB",  "512GB",  "768GB",   "1TB", "$\infty$"]
    ticks["Medium"] =   ["512GB", "768GB",   "1TB",   "2TB",    "3TB",   "4TB",    "5TB",    "6TB",   "8TB", "$\infty$"]
    ticks["Large"] =    ["4TB",     "8TB",  "16TB",  "24TB",   "32TB",  "48TB",   "64TB",   "80TB",  "96TB", "$\infty$"]
    ticks["Huge"] =     ["32TB",   "64TB", "128TB", "256TB",  "384TB", "512TB",  "768TB", "1024TB","1280TB", "$\infty$"]
    ticks["Enormous"] = ["64TB",  "128TB", "256TB", "512TB",  "768TB",   "1PB", "1536TB",    "2PB",   "3PB", "$\infty$"]


    buckets = ["Tiny", "Small", "Medium", "Large", "Huge", "Enormous"]

    caches = ["MRUCache",  "FifoCache", "RandomCache", "LRUARCCache", "LRUCache", "BeladyCache"]


    warm_up_dates = ["None", "2013-01-01"]

    warm_up_line_types = ["", "--", ":", "-."]
    for bucket_size in buckets:
        # draw one plot per bucket size and for totals

        values = dict()
        for warm_up_date in warm_up_dates:
            values[warm_up_date] = defaultdict(list)

        for warm_up_date in warm_up_dates:
            for i in range(10):
                for cache_type in caches:
                    active_tests = [tr for tr in test_results if tr.config["cache_config"].__contains__("ecmwf_%d" % i) and tr.config["cache_config"].__contains__(cache_type)]
                    hit_ratio = 0.0
                    if len(active_tests) > 0:
                        hrs = list()
                        for test in active_tests:
                            hr = test.get_hr(bucket_size, start_date=warm_up_date)
                            hrs.append(hr)
                        hit_ratio = sum(hrs) / len(hrs)
                    # print(bucket_size, i, cache_type, hit_ratio)
                    values[warm_up_date][cache_type].append(hit_ratio)

        #plot it
        target_file = os.path.join(target_dir, "%s_cache_hits.pdf" % (bucket_size))

        fig, ax = plt.subplots()
        index = np.arange(len(values["None"]["LRUCache"]))
        width = 0.15
        colors = ["0.9", "0.8", "0.7", "0.6", "0.5", "0.4", "0.3", "0.2", "0.1"]

        none_values = values["None"]
        warm_values = values["2013-01-01"]

        cindex = 0
        for c, v in warm_values.items():
            bar = ax.bar(index + (cindex * width), v, width, label=c, color=colors[cindex])
            cindex += 1

        ecmwf_hit_ratio_total = get_ecfs_baseline_total(ecfs_base_cache_hit_ratio_file, start_date="2013-01-01", bucket=bucket_size)
        plt.axhline(y=ecmwf_hit_ratio_total, linewidth=2, color='r', label="ECMWF baseline")


        ## Experiment with warm up dates & caches.
        # top_values = defaultdict(list)
        # bottom_values = defaultdict(list)
        # warm_is_better = defaultdict(list)
        #
        # for cache_type in caches:
        #     for i in range(len(none_values[cache_type])):
        #         nv = none_values[cache_type][i]
        #         wv = warm_values[cache_type][i]
        #
        #         if nv <= wv:
        #             # warm is faster
        #             bottom_values[cache_type].append(nv)
        #             top_values[cache_type].append(wv - nv)
        #             warm_is_better[cache_type].append(True)
        #         else:
        #             bottom_values[cache_type].append(wv)
        #             top_values[cache_type].append(nv - wv)
        #             warm_is_better[cache_type].append(False)
        #
        # for c, bvs in bottom_values.items():
        #     tvs = top_values[c]
        #     bar = ax.bar(index + (cindex * width), bvs, width, label=c, color=colors[cindex])
        #     bar = ax.bar(index + (cindex * width), tvs, width, bottom=bvs, color=colors[cindex])
        #     cindex += 1
        #
        #
        #
        # #plot ecmwf_hit_ratio base line
        # lti = 0
        # for warm_up_date in warm_up_dates:
        #     lti += 1
        #     ecmwf_hit_ratio_total = get_ecfs_baseline_total(ecfs_base_cache_hit_ratio_file, start_date=warm_up_date, bucket=bucket_size)
        #     plt.axhline(y=ecmwf_hit_ratio_total, linestyle=warm_up_line_types[lti % len(warm_up_line_types)], linewidth=1, color='r', label=warm_up_date)



        ax.yaxis.grid(True)

        plt.title('Cache Hit Ratio - %s' % (bucket_size))
        plt.ylabel('Cache Hit Ratio')

        xs = np.arange(len(ticks[bucket_size]))
        plt.xticks([x + 0.4 for x in xs], ticks[bucket_size])


        sizes = fig.get_size_inches()
        fig.set_size_inches(sizes[0]*3, sizes[1])
        plt.tight_layout()

        handles,labels = ax.get_legend_handles_labels()

        plt.savefig(target_file)
        plt.close()

        plt.clf()

        fig, ax = plt.subplots()
        ax.legend(handles, labels, fancybox=True, shadow=False, ncol=6, title="Cache Strategy")
        ax.xaxis.set_visible(False)
        ax.yaxis.set_visible(False)
        plt.axis('off')

        sizes = fig.get_size_inches()
        fig.set_size_inches(sizes[0]* 0.1, sizes[1])
        plt.tight_layout()

        plt.savefig(os.path.join(target_dir, "legend.pdf"), bbox_inches='tight', pad_inches=0.1)
        plt.close()

        # # now print the legend
        # axl = plt.subplot()  #create the axes
        # axl.set_axis_off()  #turn off the axis
        #
        # cindex = 0
        # handles = []
        # labels = []
        # for c in caches:
        #     x = plt.bar([1], 100, 0.5, label=c, color=colors[cindex])
        #     handles.append(x)
        #     labels.append(c)
        #     cindex += 1
        # ax.legend(handles, labels, fancybox=True, shadow=False, ncol=6, title="Cache Strategy")
        #
        # plt.savefig(os.path.join(results_dir, "legend.pdf"), bbox_inches='tight', pad_inches=0.1)

# "totals": {
#         "caches": {
#             "Enormous": {
#                 "cache_hit_ratio_requests"



def main(results_dir, ecfs_base_cache_hit_ratio_file, target_dir):

    test_results = []
    # print (results_dir)
    # read all the results into one big dict


    ecfs_baseline = get_ecfs_baseline(ecfs_base_cache_hit_ratio_file)
    for r in glob.glob(results_dir+"/**/results.json"):
        print (r)
        with open(r, 'r') as f:
            test_result = TestResult(os.path.join(os.path.dirname(r)))

            if test_result.is_valid():
                test_results.append(test_result)
                # test_result.plot_cache_fill_levels(baseline=ecfs_baseline)
                # test_result.plot_cache_hit_ratios(baseline=ecfs_baseline)

    # now, draw some conclusions...
    create_bucket_compare_plots(results_dir, test_results, ecfs_base_cache_hit_ratio_file, target_dir)
    to_csv(test_results, os.path.join(results_dir, "summary.csv"))

if __name__ == "__main__":
    results_dir = sys.argv[1]
    ecfs_base_cache_hit_ratio_file = sys.argv[2]
    target_dir = sys.argv[3]
    sys.exit(main(results_dir, ecfs_base_cache_hit_ratio_file, target_dir))
