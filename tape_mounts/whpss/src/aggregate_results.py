#!/usr/bin/env python

"""aggregate_results.py: read all intermediate results and aggregate results into visualizable format"""
__author__ = "Matthias Grawinkel (grawinkel@uni-mainz.de)"
__status__ = "Development"

import sys
import os
import time
import simplejson as json
import sets
import numpy
import gzip

#cwd = os.path.dirname(__file__)
cwd = sys.argv[1]

source_file = os.path.join(cwd, "results", "finished_mounts.json.gz")
results_file = os.path.join(cwd, "results", "analyzed_results.json.gz")

versions={}
versions["analysis_version"] = 1

class Timer():
    def __init__(self, s):
        self.s = s

    def __enter__(self):
        self.start = time.time()

    def __exit__(self, *args):
        print ("%s: %fs" % (self.s, (time.time() - self.start)))

stats={}
stats["versions"] = versions

stats["per_cartridge_id"] = {}

stats["num_tapes_per_group"] = {}
stats["total_mounts_per_group"] = {}

stats["tapes_per_type"] = {}
stats["total_mounts_per_type"] = {}


groups = sets.Set()
cartridge_types = sets.Set()
cartridge_prefixes = sets.Set()

def update_cartridge_id_stats(cartridge_id, num_mounts):
    group = cartridge_id[:1]
    cartridge_type = cartridge_id[1:][:1]

    cartridge_prefixes.add(cartridge_id[:2])
    groups.add(group)
    cartridge_types.add(cartridge_type)

    # stupid way to initialize all occuring groups
    if not group in stats["num_tapes_per_group"]:
        stats["num_tapes_per_group"][group] = 0
    if not group in stats["total_mounts_per_group"]:
        stats["total_mounts_per_group"][group] = 0
    if not cartridge_type in stats["tapes_per_type"]:
        stats["tapes_per_type"][cartridge_type] = 0
    if not cartridge_type in stats["total_mounts_per_type"]:
        stats["total_mounts_per_type"][cartridge_type] = 0

    stats["num_tapes_per_group"][group] += 1
    stats["total_mounts_per_group"][group] += num_mounts
    stats["tapes_per_type"][cartridge_type] += 1
    stats["total_mounts_per_type"][cartridge_type] += num_mounts

def get_time_between_mounts(mounts_list):

    if len(mounts_list) == 1:
        # only a single time mounted...
        return [], [], 0, 0, 0

    time_between_mounts = []
    initial_mount_request_epochs = []
    # print mounts_list

    initial_mount_request_epochs.append(mounts_list[0][0])
    last_dismounted_epoch = mounts_list[0][1]
    for mount in mounts_list[1:]: # for all other than the first entry
        diff = mount[0] - last_dismounted_epoch
        time_between_mounts.append(diff)
        initial_mount_request_epochs.append(mount[0])
        last_dismounted_epoch = mount[1]

    # print time_between_mounts
    min_time_between_mounts = numpy.min(time_between_mounts)
    max_time_between_mounts = numpy.max(time_between_mounts)
    mean_time_between_mounts = numpy.mean(time_between_mounts)

    return time_between_mounts,initial_mount_request_epochs, min_time_between_mounts, max_time_between_mounts, mean_time_between_mounts


if source_file.endswith(".gz"):
    with gzip.open (source_file, 'r') as f:
        print ("loading: %s" % (source_file))
        with Timer("Loading source file took:"): 
            r = json.load(f)
else:
    with open (source_file, 'r') as f:
        print ("loading: %s" % (source_file))
        with Timer("Loading source file took:"): 
            r = json.load(f)

with Timer("Processing file took:"): 
    print ("processing %d cartridge_ids" % (len(r)))
    for cartridge_id, mounts_list in r.items():
        if len(mounts_list) > 0:
            update_cartridge_id_stats(cartridge_id , len(mounts_list))

            mounted_times = []
            mount_times = []

            # stores min and max epoch of the cartridge
            first_seen = 9963176634
            last_seen = 0
            
            # contains tuples of (epoch_from,epoch_to)
            mounted_ranges = []

            for mount_details in mounts_list:
                """
                 {
                  "mounted_time": 154,
                  "CARTRIDGE_DISMOUNTED": 1363176812,
                  "mount_time": 24,
                  "MOUNT_COMPLETE": 1363176658,
                  "INITIAL_REQUEST": 1363176634
                }
                """
                first_seen = min(first_seen, mount_details["INITIAL_REQUEST"])
                last_seen = max(last_seen, mount_details["CARTRIDGE_DISMOUNTED"])

                mounted_times.append(mount_details["mounted_time"])
                mount_times.append(mount_details["mount_time"])
                mounted_ranges.append( (mount_details["INITIAL_REQUEST"], mount_details["CARTRIDGE_DISMOUNTED"]) )
                
            stats["per_cartridge_id"][cartridge_id] = {}
            stats["per_cartridge_id"][cartridge_id]["cartridge_group"] = cartridge_id[:1]
            stats["per_cartridge_id"][cartridge_id]["cartridge_type"] = cartridge_id[1:][:1]

            stats["per_cartridge_id"][cartridge_id]["num_mounts"] = len(mounts_list)
            stats["per_cartridge_id"][cartridge_id]["seen_first"] = first_seen
            stats["per_cartridge_id"][cartridge_id]["seen_last"] = last_seen
            stats["per_cartridge_id"][cartridge_id]["seen_timespan"] = last_seen - first_seen

            stats["per_cartridge_id"][cartridge_id]["mounted_times_min"] = numpy.min(mounted_times)
            stats["per_cartridge_id"][cartridge_id]["mounted_times_max"] = numpy.max(mounted_times)
            stats["per_cartridge_id"][cartridge_id]["mounted_times_mean"] = numpy.mean(mounted_times)

            stats["per_cartridge_id"][cartridge_id]["mount_times_min"] = numpy.min(mount_times)
            stats["per_cartridge_id"][cartridge_id]["mount_times_max"] = numpy.max(mount_times)
            stats["per_cartridge_id"][cartridge_id]["mount_times_mean"] = numpy.mean(mount_times)

            # sorted will sort the entries based on their first tuple element.
            time_between_mounts, initial_mount_request_epochs, min_time_between_mounts, max_time_between_mounts, mean_time_between_mounts = get_time_between_mounts(sorted(mounted_ranges))
            
            stats["per_cartridge_id"][cartridge_id]["time_between_mounts"] = time_between_mounts
            stats["per_cartridge_id"][cartridge_id]["time_between_mounts_min"] = min_time_between_mounts
            stats["per_cartridge_id"][cartridge_id]["time_between_mounts_max"] = max_time_between_mounts
            stats["per_cartridge_id"][cartridge_id]["time_between_mounts_mean"] = mean_time_between_mounts
            stats["per_cartridge_id"][cartridge_id]["initial_mount_request_epochs"] = initial_mount_request_epochs

    # print len(r)
    # print "groups", groups
    # print "cartridge_types", cartridge_types
    # print "cartridge_prefixes", cartridge_prefixes

    with gzip.open (results_file, 'w') as f:
        json.dump(stats,f, indent=2, sort_keys=True)