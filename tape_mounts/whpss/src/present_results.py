#!/usr/bin/env python

"""present_results.py: read the analyzed_results.json and present some relevant numbers create some graphs"""
__author__ = "Matthias Grawinkel (grawinkel@uni-mainz.de)"
__status__ = "Development"

import sys
import os
import time
import simplejson as json
import numpy as np
import gzip

import matplotlib.pyplot as plt



COLORS = ['SteelBlue', 'Blue', 'Gainsboro','DarkGray', 'SlateGray', 'Olive', 'LightSeaGreen', 'DarkCyan','Lime','LightGreen','Green', 'LightSalmon','IndianRed', 'Red']

#### HELPER =====================================================================

class Timer():
    def __init__(self, s):
        self.s = s

    def __enter__(self):
        self.start = time.time()

    def __exit__(self, *args):
        print ("%s: %fs" % (self.s, (time.time() - self.start)))

data = None

from datetime import date, timedelta
def get_date_index_for(timespan, epoch_from, epoch_to):
    time_from = time.gmtime(epoch_from)
    time_to = time.gmtime(epoch_to)
    date_from = date(time_from.tm_year, time_from.tm_mon, time_from.tm_mday)
    date_to = date(time_to.tm_year, time_to.tm_mon, time_to.tm_mday)

    elems = set()

    d = date_from
    while d <= date_to:
        if timespan == 'days':
            elems.add(d.strftime("%Y%m%d"))
        elif timespan == 'weeks':
            elems.add(d.strftime("%Y%W"))
        else:
            raise Exception("unknown timespan")
        
        d = d + timedelta(1)  # add one day

    date_index = sorted(list(elems))
    return date_index

def get_mount_time_clusters(mount_times, time_between_mounts, max_time_between_mounts):
    print len (mount_times)
    print len (time_between_mounts)

    clusters = []
    cc = set()

    for i in range(len(time_between_mounts)):
        if time_between_mounts[i] <= max_time_between_mounts:
            cc.add(mount_times[i])
            cc.add(mount_times[i+1])
        else:
            if len(cc) > 0:
                clusters.append(list(cc))
                cc.clear()

    return clusters
    
#### FILTER =====================================================================

def get_mounts_per_group_for(timespan, year):
    """
        timespan can be 'days' / 'weeks'
    """
    year = int(year)
    min_epoch = 9999999999999
    max_epoch = 0
    plt_data = {}
    for cartridge_id, details in data['per_cartridge_id'].items():
        group = details['cartridge_group']

        for epoch in details['initial_mount_request_epochs']:

            t = time.gmtime(epoch)
            if t.tm_year == year:
                if timespan == 'days':
                    ts = time.strftime("%Y%m%d", t)
                elif timespan == 'weeks':
                    ts = time.strftime("%Y%W", t)
                else:
                    raise Exception("unknown timespan")

                min_epoch = min(min_epoch, epoch)
                max_epoch = max(max_epoch, epoch)

                if not group in plt_data:
                    plt_data[group] = {}

                if not ts in plt_data[group]:
                    plt_data[group][ts] = 1
                else:
                    plt_data[group][ts] += 1

    return plt_data, min_epoch, max_epoch

def get_hot_cartridges(min_mounts, years=None, groups=None):
    """
        years can be a list of [2011,2012,2013]
        groups can be a list of ['C', 'X', 'B']
    """
    r = {}
    for cartridge_id, details in data['per_cartridge_id'].items():
        if not groups or details['cartridge_group'] in groups:
            if not years:
                # don't filter for year, just take all
                if details["num_mounts"] >= min_mounts:
                    r[cartridge_id] = details
            else:
                if details["num_mounts"] >= min_mounts:
                    mount_count = 0
                    for epoch in details['initial_mount_request_epochs']:
                        if time.gmtime(epoch).tm_year in years:
                            mount_count += 1
                    if mount_count >= min_mounts:
                        r[cartridge_id] = details
    return r


def get_sorted_cdf_cartridge_mount_cnts(groups):
    """
        returns a list with cartridge mounts.
        sorted by: hottness of cartridges
    """

    r = [] 
    for cartridge_id, details in data['per_cartridge_id'].items():
        if not groups or details['cartridge_group'] in groups:
            r.append(details['num_mounts'])

    # sort by highest numbers first
    
    r = sorted(r)[::-1]

    for i in range(1, len(r)):
        r[i] = r[i] + r[i-1]
    return r

#### GRAPHS =====================================================================

def draw_cartridge_mounts_per_week(date_index, plt_data, fig_path, year):
    plt.xlabel('Week')
    plt.ylabel('Number of cartridge mounts')
    plt.title("Cartridge Mounts per Group per Week - %s" % (year))
    ind = np.arange(len(date_index))

    legend_labels = []
    plotted_elements = []
    bottom = len(date_index) * [0]

    group_cnt = 0
    for group in plt_data.keys():
    
        col = COLORS[group_cnt % len(COLORS)]
        group_cnt += 1
        print "plotting group %s with color %s" % (group, col)
        values = []
        for x in range(len(date_index)):
            if date_index[x] in plt_data[group]:
                v = plt_data[group][date_index[x]]
                values.append(v)
            else:
                values.append(0)
        print values

        b = plt.bar(ind, values, bottom=bottom, color=col)

        plotted_elements.append(b)
        legend_labels.append(group)

        for i in range(len(date_index)):
            bottom[i] += values[i]

    # plt.xticks(np.arange(len(date_index)))
    
    plt.legend(
        plotted_elements[::-1],
        legend_labels[::-1],
        loc='center left',
        fancybox=True,
        shadow=True,
        bbox_to_anchor=(0.9, 0.5)
    )

    plt.savefig(fig_path)
    plt.close()

def draw_cdf(plt_data, fig_path, year, group):
    plt.xlabel('Cartdiges, sorted by mount count')
    plt.ylabel('Total cartridge mounts')
    if group:
        plt.title("CDF of cartridge mounts for group %s - %s" % (group, year))
    else:
        plt.title("CDF of cartridge mounts for all groups- %s" % (year))
    ind = np.arange(len(plt_data))

    plt.plot(plt_data)
    
    plt.savefig(fig_path)
    plt.show()
    plt.close()


##########################################################################################

draw_mnt_per_group_per_week = False
draw_cdf_of_month = True

#cwd = os.path.dirname(__file__)
cwd = sys.argv[1]

source_file = os.path.join(cwd, "results", "analyzed_results.json.gz")

if source_file.endswith(".gz"):
    with gzip.open (source_file, 'r') as f:
        print ("loading: %s" % (source_file))
        with Timer("Loading source file took:"):
            data = json.load(f)
else:
    with open (source_file, 'r') as f:
        print ("loading: %s" % (source_file))
        with Timer("Loading source file took:"):
            data = json.load(f)

#with open (source_file, 'r') as src:
#    data = json.load(src)



if draw_mnt_per_group_per_week:
    # ================== Stacked Barplot - mounts per group per week ===================================
    year = 2012
    plt_data, min_epoch, max_epoch = get_mounts_per_group_for('weeks', year)
    date_index = get_date_index_for('weeks', min_epoch, max_epoch)
    print plt_data
    print date_index
    draw_cartridge_mounts_per_week(date_index, plt_data, "results/graphs/dcartridge_mounts_per_week-%d.pdf" % (year), year)
    # ===========================================================


# ==================== Find number of tapes at least beeing mounted X times in year ===========================
# limits = [2,4,8,16,32,64,128,256,512,1024,2048,4096,8192]
# groups = [None, ['C'], ['E'], ['M'], ['Q'], ['P'], ['S'], ['R'], ['U'], ['W'], ['V'], ['Y'], ['X'], ['Z']]
# for group in groups:
#     print "======="
#     if group:
#         gs = group[0]
#     else:
#         gs = "All"
#     print "Group: %s, known cartridges: %d" % (gs, len(get_hot_cartridges(0, groups=group)))
#     for limit in limits:
#         mounts = get_hot_cartridges(limit, groups=group)
#         mounts_len = len(mounts)
#         if limit >= 4096 and mounts_len > 0:
#             print ">= %d: %d - %s" % (limit, len(mounts), str(mounts.keys()))
#         else:
#             print ">= %d: %d" % (limit, len(mounts))
# ===========================================================


if draw_cdf_of_month:
#==================== Draw CDF of mounts =========================================
    year = 2012
    groups = [None, ['C'], ['E'], ['M'], ['Q'], ['P'], ['S'], ['R'], ['U'], ['W'], ['V'], ['Y'], ['X'], ['Z']]
    for group in groups:
        if group:
             group_name = group[0]
        else:
             group_name = "All"
        print "plotting CDF - %s" % (group_name)
        plt_data = get_sorted_cdf_cartridge_mount_cnts(group)
        draw_cdf(plt_data, "results/graphs/cdf-%s.pdf" % (group_name), year, group)
#======================================================================



# ============================= Clustering ==================
details = data['per_cartridge_id']['XB0161']
# put into cluster when mounted again within 4 hours after dismount.
max_time_between_mounts =  5 * 60 
clusters = get_mount_time_clusters(details['initial_mount_request_epochs'], details['time_between_mounts'], max_time_between_mounts)
print clusters
clusters = clusters

cluster_lengths  = []
for c in clusters:
    cluster_lengths.append(len(c))

cluster_lengths = sorted(cluster_lengths)[::-1]    
print cluster_lengths
# ===========================================================
