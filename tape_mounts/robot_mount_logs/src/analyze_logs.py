#!/usr/bin/env python

"""filter_log.py: read the analyzed_results.json and present some relevant numbers create some graphs"""

__author__ = ["Matthias Grawinkel (grawinkel@uni-mainz.de)", "Markus Maesker (maesker@uni-mainz.de)"]
__status__ = "Development"



import StringIO
import os
import gzip
import sys
import math
import time # assume all timestamps to be UTC.
import calendar
import datetime
import re
import json
import glob
import socket

from robot_components import *


if socket.gethostname() == 'deb7':
    module_base_dir = os.path.abspath(os.path.abspath(os.getcwd()))
# also add the parent folder of this file to the python search path.
else:
    module_base_dir = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
sys.path.append(module_base_dir)

from TimeTools import get_epochs, Timer


#VLAD#20130610:050528#     ACSCR UB1698 Home 1,-1,0,0,0 STATUS_VOLUME_NOT_FOUND Client Host Id Local
#VLAD#20130424:110940#     AUDIT RC1335 Home 1,-1,0,0,0 STATUS_VOLUME_NOT_FOUND Client Host Id Local

##  hdre01#VLAD#20100427:001756#  DISMOUNT C25895 Home 0,3,19,4,10 Drive 0,3,10,8 Client Host Id 136.156.216.161
re_line = re.compile(".*([0-9]{8}:[0-9]{6}).* (ACSMV|ACSCR|AUDIT|EJECT|ENTER|MOUNT|DISMOUNT) ([0-9a-zA-Z]{6}) Home ([0-9,]*) ([a-zA-Z\s]+) ([0-9,]*) .*")
re_line_not_found = re.compile(".*([0-9]{8}:[0-9]{6}).* (ACSCR|AUDIT) ([0-9a-zA-Z]{6}).* (STATUS_VOLUME_NOT_FOUND) .*")


god = None


def get_epoch(ts):
    t = time.strptime(ts, '%Y%m%d:%H%M%S')
    epoch = calendar.timegm(t)
    return epoch

def percentile(N, P):
    """
    Find the percentile of a list of values

    @parameter N - A list of values.  N must be sorted.
    @parameter P - A float value from 0.0 to 1.0

    @return - The percentile of the values.
    """
    n = int(round(P * len(N) + 0.5))
    return N[n-1]

def analyze_mounts(mounts_list):

    if len(mounts_list) <= 1:
        return None, None, None, None, None

    # mounts_list contains tuples (epoch_mounted, epoch_dismounted, drive_id)
    time_between_mounts = []

    last_dismount = mounts_list[0][1]
    last_drive = mounts_list[0][2]

    # calculate the drive affinity.
    same_60 = 0
    other_60 = 0
    same_300 = 0
    other_300 = 0

    drives = set()

    for mount in mounts_list[1:]: # for all other than the first entry
        if mount[0] < mount[1]:
            delta = mount[0] - last_dismount
            time_between_mounts.append(delta)
            last_dismount = mount[1]

            # now, the da
            if delta <= 60:
                if mount[2] == last_drive:
                    same_60 += 1
                else:
                    other_60 += 1

            if delta <= 300:
                if mount[2] == last_drive:
                    same_300 += 1
                else:
                    other_300 += 1

            drives.add(mount[2])
            last_drive = mount[2]
        else:
            print ("found broken mount: %s" % str(mount))

    da = dict()
    da["different_drives"] = len(drives)
    if same_60 > 0:
        da["same_60"] = same_60
    if other_60 > 0:
        da["other_60"] = other_60
    if same_300 > 0:
        da["same_300"] = same_300
    if other_300 > 0:
        da["other_300"] = other_300

    tbm = dict()

    time_between_mounts = sorted(time_between_mounts)
    size = len(time_between_mounts)
    tbm["p5"] = percentile(time_between_mounts, P=0.05)
    tbm["p25"] = percentile(time_between_mounts, P=0.25)
    tbm["p50"] = percentile(time_between_mounts, P=0.5)
    tbm["p75"] = percentile(time_between_mounts, P=0.75)
    tbm["p95"] = percentile(time_between_mounts, P=0.95)

    tbm["min"] = time_between_mounts[0]
    tbm["max"] = time_between_mounts[-1]
    tbm["mean"] = float(sum(time_between_mounts) / len(time_between_mounts))
    tbm["sum"] = sum(time_between_mounts)

    time_mounts = []
    min_epoch = 99999999999999
    max_epoch = 0
    for mount in mounts_list:
        t = mount[1] - mount[0]
        time_mounts.append(t)
        min_epoch = min(min_epoch, mount[0])
        max_epoch = max(max_epoch, mount[1])

    tm = dict()
    time_mounts = sorted(time_mounts)
    size = len(time_mounts)
    tm["p5"] = percentile(time_mounts, P=0.05)
    tm["p25"] = percentile(time_mounts, P=0.25)
    tm["p50"] = percentile(time_mounts, P=0.5)
    tm["p75"] = percentile(time_mounts, P=0.75)
    tm["p95"] = percentile(time_mounts, P=0.95)

    tm["min"] = time_mounts[0]
    tm["max"] = time_mounts[-1]
    tm["mean"] = float(sum(time_mounts) / len(time_mounts))
    tm["sum"] = sum(time_mounts)
    return tbm, da, tm, min_epoch, max_epoch


def get_mount_stats(cartridge_id, mounts_list):
    """
        mounts list is a list of sublists [mounted_epoch, dismounted_epoch, drive_id]
    @param mounts_list:
    @return:
    """

    # the mounts_list needs to sorted by the mounted_epochs.
    # sorts entries by their first element
    mounts_list = sorted(mounts_list)

    broken_mounts = []
    for m in xrange (1, len(mounts_list)):
        if not (mounts_list[m-1][0] <= mounts_list[m-1][1]):
            broken_mounts.append(mounts_list[m])
        assert(mounts_list[m-1][0] <= mounts_list[m][0])
        if not (mounts_list[m-1][1] <= mounts_list[m][1]):
            #print mounts_list[m-1]
            #print mounts_list[m]
            broken_mounts.append(mounts_list[m])

    for x in broken_mounts:
        print "found overlapping mount. removing: ", x
        mounts_list.remove(x)

    first_mount_epoch = mounts_list[0][0]
    last_dismounted_epoch = mounts_list[-1][1]

    s = dict()
    s["first_mount_epoch"] = first_mount_epoch
    s["last_dismounted_epoch"] = last_dismounted_epoch

    #for epoch_filter in ["all", "daily", "monthly"]:
    for epoch_filter in ["all", "yearly", "monthly"]:
        if epoch_filter == "all":
            epoch_ranges = [(first_mount_epoch, last_dismounted_epoch)]
        else:
            epoch_ranges = get_epochs(first_mount_epoch, last_dismounted_epoch, epoch_filter)

        for epoch_range in epoch_ranges:
            relevant_mounts = []
            for mount in mounts_list:
                # filter all mounts that have been started in that range / day / month
                if (mount[0] >= epoch_range[0]) and (mount[0] <= epoch_range[1]):
                    relevant_mounts.append(mount)

            rs = dict()
            rs["total_mounts"] = len(relevant_mounts)

            # get stats for "time between mounts" and "drive affinity"
            tbm, da, tm, min_epoch, max_epoch = analyze_mounts(relevant_mounts)
            if min_epoch:
                rs["min_epoch_in_range"] = min_epoch
            if max_epoch:
                rs["max_epoch_in_range"] = max_epoch
            if tbm:
                rs["time_between_mounts"] = tbm
            if da:
                rs["drive_affinity"] = da
            if tm:
                rs["time_mounted"] = tm

            #cartridge_id/all/stats
            #            /daily/"2012-05-23/stats
            #            /monthly/"2012-03"/stats
            if len(relevant_mounts) > 0:
                if not epoch_filter in s:
                    s[epoch_filter] = dict()
                if epoch_filter == "all":
                    s[epoch_filter] = rs
                elif epoch_filter == "daily":
                    ts = time.strftime("%Y-%m-%d", time.gmtime(epoch_range[0]))
                    s[epoch_filter][ts] = rs
                elif epoch_filter == "monthly":
                    ts = time.strftime("%Y-%m", time.gmtime(epoch_range[0]))
                    s[epoch_filter][ts] = rs
                elif epoch_filter == "yearly":
                    ts = time.strftime("%Y", time.gmtime(epoch_range[0]))
                    s[epoch_filter][ts] = rs

    return s


def analyze_drive_mounts(relevant_mounts):
    """
    relevant_mounts contains tuples (epoch_mounted, epoch_dismounted, drive_id)
    @param relevant_mounts:
    @return:
    """

    if len(relevant_mounts) == 0:
        return None, None, None, None

    # Time between mounts
    tbm = None

    if len(relevant_mounts) > 1:
        if len(relevant_mounts) <= 1:
            return None, None, relevant_mounts[0][0],relevant_mounts[0][1]

        time_between_mounts = []

        last_dismount = relevant_mounts[0][1]

        for mount in relevant_mounts[1:]:  # for all other than the first entry
            delta = mount[0] - last_dismount
            time_between_mounts.append(delta)
            last_dismount = mount[1]

        tbm = dict()

        time_between_mounts = sorted(time_between_mounts)
        tbm["p5"] = percentile(time_between_mounts, P=0.05)
        tbm["p25"] = percentile(time_between_mounts, P=0.25)
        tbm["p50"] = percentile(time_between_mounts, P=0.5)
        tbm["p75"] = percentile(time_between_mounts, P=0.75)
        tbm["p95"] = percentile(time_between_mounts, P=0.95)

        tbm["min"] = time_between_mounts[0]
        tbm["max"] = time_between_mounts[-1]
        tbm["mean"] = float(sum(time_between_mounts) / len(time_between_mounts))
        tbm["sum"] = sum(time_between_mounts)
    time_mounts = []

    min_epoch = 99999999999999
    max_epoch = 0
    for mount in relevant_mounts:
        if mount[0] < mount[1]:
            t = mount[1] - mount[0]
            time_mounts.append(t)
            min_epoch = min(min_epoch, mount[0])
            max_epoch = max(max_epoch, mount[1])
        else:
            print ("found broken mount: %s" % str(mount))

    tm = dict()
    time_mounts = sorted(time_mounts)
    if len(time_mounts) > 1:
        tm["p5"] = percentile(time_mounts, P=0.05)
        tm["p25"] = percentile(time_mounts, P=0.25)
        tm["p50"] = percentile(time_mounts, P=0.5)
        tm["p75"] = percentile(time_mounts, P=0.75)
        tm["p95"] = percentile(time_mounts, P=0.95)

        tm["min"] = time_mounts[0]
        tm["max"] = time_mounts[-1]
        tm["mean"] = float(sum(time_mounts) / len(time_mounts))
        tm["sum"] = sum(time_mounts)
    else:
        tm["p5"] = time_mounts[0]
        tm["p25"] = time_mounts[0]
        tm["p50"] = time_mounts[0]
        tm["p75"] = time_mounts[0]
        tm["p95"] = time_mounts[0]

        tm["min"] = time_mounts[0]
        tm["max"] = time_mounts[0]
        tm["mean"] = time_mounts[0]
        tm["sum"] = time_mounts[0]
    return tbm, tm, min_epoch, max_epoch

def get_drive_stats(drive_id, drive_mounts_list):
    """
        mounts list is a list of sublists [mounted_epoch, dismounted_epoch, cartridge_id]
    @param drive_mounts_list:
    @return:
    """

    # the drive_mounts_list is sorted


    first_mount_epoch = drive_mounts_list[0][0]
    last_dismounted_epoch = drive_mounts_list[-1][1]

    s = dict()
    s["first_mount_epoch"] = first_mount_epoch
    s["last_dismounted_epoch"] = last_dismounted_epoch

    #for epoch_filter in ["all", "daily", "monthly"]:
    for epoch_filter in ["all", "yearly", "monthly"]:
        if epoch_filter == "all":
            epoch_ranges = [(first_mount_epoch, last_dismounted_epoch)]
        else:
            epoch_ranges = get_epochs(first_mount_epoch, last_dismounted_epoch, epoch_filter)

        for epoch_range in epoch_ranges:
            relevant_mounts = []
            for mount in drive_mounts_list:
                # filter all mounts that have been started in that range / day / month
                if (mount[0] >= epoch_range[0]) and (mount[0] <= epoch_range[1]):
                    relevant_mounts.append(mount)

            rs = dict()
            rs["total_mounts_on_drive"] = len(relevant_mounts)

            #epoch start to epoch end == total seconds
            #calculate the time that this drive_id was active

            tbm, tm, min_epoch, max_epoch = analyze_drive_mounts(relevant_mounts)
            if min_epoch:
                rs["min_epoch_in_range"] = min_epoch
            if max_epoch:
                rs["max_epoch_in_range"] = max_epoch
            if tbm:
                rs["time_between_mounts"] = tbm
            if tm:
                rs["time_mounted"] = tm

            if tm:
                seconds_in_range = max_epoch - min_epoch
                rs["percent_time_in_use"] = (1.0 * rs["time_mounted"]["sum"]) / seconds_in_range * 100

            unique_cartridge_ids = set()
            for mount in relevant_mounts:
                unique_cartridge_ids.add(mount[2])
            rs["unique_cartridge_ids"] = len(unique_cartridge_ids)

            if len(relevant_mounts) > 0:
                if not epoch_filter in s:
                    s[epoch_filter] = dict()
                if epoch_filter == "all":
                    s[epoch_filter] = rs
                elif epoch_filter == "daily":
                    ts = time.strftime("%Y-%m-%d", time.gmtime(epoch_range[0]))
                    s[epoch_filter][ts] = rs
                elif epoch_filter == "monthly":
                    ts = time.strftime("%Y-%m", time.gmtime(epoch_range[0]))
                    s[epoch_filter][ts] = rs
                elif epoch_filter == "yearly":
                    ts = time.strftime("%Y", time.gmtime(epoch_range[0]))
                    s[epoch_filter][ts] = rs

    return s


def analyze_totals(available_months_list, per_cartridge_id, per_drive_id):
    summary = dict()

    yset = set()
    for m in available_months_list:
        yset.add(m.split("-")[0])
    years = sorted(list(yset))

    summary["all"] = dict()
    summary["all"]["total_cartridge_mounts"] = 0

    summary["yearly"] = dict()

    mounts_per_cartridge_tmp = dict()
    mounts_per_cartridge_tmp["monthly"] = dict()
    mounts_per_cartridge_tmp["yearly"] = dict()

    for y in years:
        summary["yearly"][y] = dict()
        summary["yearly"][y]["same_60"] = 0
        summary["yearly"][y]["other_60"] = 0
        summary["yearly"][y]["same_300"] = 0
        summary["yearly"][y]["other_300"] = 0
        summary["yearly"][y]["total_mounts"] = 0
        mounts_per_cartridge_tmp["yearly"][y] = []

    summary["monthly"] = dict()
    for m in available_months_list:
        summary["monthly"][m] = dict()
        summary["monthly"][m]["same_60"] = 0
        summary["monthly"][m]["other_60"] = 0
        summary["monthly"][m]["same_300"] = 0
        summary["monthly"][m]["other_300"] = 0
        summary["monthly"][m]["total_mounts"] = 0

        mounts_per_cartridge_tmp["monthly"][m] = []
    # ============================================

    for cartridge_id, details in per_cartridge_id.items():
        summary["all"]["total_cartridge_mounts"] += details["mount_details"]["all"]["total_mounts"]

        for m in available_months_list:
            if m in details["mount_details"]["monthly"]:
                if "drive_affinity" in details["mount_details"]["monthly"][m] and details["mount_details"]["monthly"][m]["drive_affinity"] != None:
                    same_60 = details["mount_details"]["monthly"][m]["drive_affinity"].get("same_60", 0)
                    other_60 = details["mount_details"]["monthly"][m]["drive_affinity"].get("other_60", 0)
                    same_300 = details["mount_details"]["monthly"][m]["drive_affinity"].get("same_300", 0)
                    other_300 = details["mount_details"]["monthly"][m]["drive_affinity"].get("other_300", 0)

                    summary["monthly"][m]["same_60"] += same_60
                    summary["monthly"][m]["other_60"] += other_60
                    summary["monthly"][m]["same_300"] += same_300
                    summary["monthly"][m]["other_300"] += other_300

                total_mounts = details["mount_details"]["monthly"][m]["total_mounts"]
                summary["monthly"][m]["total_mounts"] += total_mounts

                mounts_per_cartridge_tmp["monthly"][m].append(details["mount_details"]["monthly"][m]["total_mounts"])

        for y in years:
            if y in details["mount_details"]["yearly"]:
                if "drive_affinity" in details["mount_details"]["yearly"][y] and details["mount_details"]["yearly"][y]["drive_affinity"] != None:
                    same_60 = details["mount_details"]["yearly"][y]["drive_affinity"].get("same_60", 0)
                    other_60 = details["mount_details"]["yearly"][y]["drive_affinity"].get("other_60", 0)
                    same_300 = details["mount_details"]["yearly"][y]["drive_affinity"].get("same_300", 0)
                    other_300 = details["mount_details"]["yearly"][y]["drive_affinity"].get("other_300", 0)

                    summary["yearly"][y]["same_60"] += same_60
                    summary["yearly"][y]["other_60"] += other_60
                    summary["yearly"][y]["same_300"] += same_300
                    summary["yearly"][y]["other_300"] += other_300

                total_mounts = details["mount_details"]["yearly"][y]["total_mounts"]
                summary["yearly"][y]["total_mounts"] += total_mounts

                mounts_per_cartridge_tmp["yearly"][y].append(details["mount_details"]["yearly"][y]["total_mounts"])

    for month, mounts_per_catrdige_list in mounts_per_cartridge_tmp["monthly"].items():
        mounts_per_catrdige_list = sorted(mounts_per_catrdige_list)
        summary["monthly"][month]["mounts_per_cartridge"] = dict()
        summary["monthly"][month]["mounts_per_cartridge"]["min"] = mounts_per_catrdige_list[0]
        summary["monthly"][month]["mounts_per_cartridge"]["max"] = mounts_per_catrdige_list[-1]
        summary["monthly"][month]["mounts_per_cartridge"]["mean"] = float(sum(mounts_per_catrdige_list) / len(mounts_per_catrdige_list))
        summary["monthly"][month]["mounts_per_cartridge"]["p5"] = percentile(mounts_per_catrdige_list, P=0.05)
        summary["monthly"][month]["mounts_per_cartridge"]["p25"] = percentile(mounts_per_catrdige_list, P=0.25)
        summary["monthly"][month]["mounts_per_cartridge"]["p50"] = percentile(mounts_per_catrdige_list, P=0.5)
        summary["monthly"][month]["mounts_per_cartridge"]["p75"] = percentile(mounts_per_catrdige_list, P=0.75)
        summary["monthly"][month]["mounts_per_cartridge"]["p95"] = percentile(mounts_per_catrdige_list, P=0.95)

    for year, mounts_per_catrdige_list in mounts_per_cartridge_tmp["yearly"].items():
        mounts_per_catrdige_list = sorted(mounts_per_catrdige_list)
        summary["yearly"][year]["mounts_per_cartridge"] = dict()
        summary["yearly"][year]["mounts_per_cartridge"]["min"] = mounts_per_catrdige_list[0]
        summary["yearly"][year]["mounts_per_cartridge"]["max"] = mounts_per_catrdige_list[-1]
        summary["yearly"][year]["mounts_per_cartridge"]["mean"] = float(sum(mounts_per_catrdige_list) / len(mounts_per_catrdige_list))
        summary["yearly"][year]["mounts_per_cartridge"]["p5"] = percentile(mounts_per_catrdige_list, P=0.05)
        summary["yearly"][year]["mounts_per_cartridge"]["p25"] = percentile(mounts_per_catrdige_list, P=0.25)
        summary["yearly"][year]["mounts_per_cartridge"]["p50"] = percentile(mounts_per_catrdige_list, P=0.5)
        summary["yearly"][year]["mounts_per_cartridge"]["p75"] = percentile(mounts_per_catrdige_list, P=0.75)
        summary["yearly"][year]["mounts_per_cartridge"]["p95"] = percentile(mounts_per_catrdige_list, P=0.95)
    #==============================
    # Calculate how "busy" the drives have been
    for m in available_months_list:
        #print json.dumps(details, indent=2,sort_keys=True)
        drive_usages = []
        min_epoch = 99999999999999
        max_epoch = 0

        for drive_id, details in per_drive_id.items():
            if m in details["monthly"]:
                drive_usages.append(details["monthly"][m]["percent_time_in_use"])
                min_epoch = min(min_epoch, details["monthly"][m]["min_epoch_in_range"])
                max_epoch = max(max_epoch, details["monthly"][m]["max_epoch_in_range"])
        drive_usages = sorted(drive_usages)
        summary["monthly"][m]["drive_busy"] = dict()
        summary["monthly"][m]["drive_busy"]["min"] = drive_usages[0]
        summary["monthly"][m]["drive_busy"]["max"] = drive_usages[-1]
        summary["monthly"][m]["drive_busy"]["mean"] = float(sum(drive_usages) / len(drive_usages))
        summary["monthly"][m]["drive_busy"]["p5"] = percentile(drive_usages, P=0.05)
        summary["monthly"][m]["drive_busy"]["p25"] = percentile(drive_usages, P=0.25)
        summary["monthly"][m]["drive_busy"]["p50"] = percentile(drive_usages, P=0.5)
        summary["monthly"][m]["drive_busy"]["p75"] = percentile(drive_usages, P=0.75)
        summary["monthly"][m]["drive_busy"]["p95"] = percentile(drive_usages, P=0.95)

        summary["monthly"][m]["from_ts"] = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(min_epoch))
        summary["monthly"][m]["to_ts"] = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(max_epoch))

    for y in years:
        drive_usages = []
        min_epoch = 99999999999999
        max_epoch = 0
        for drive_id, details in per_drive_id.items():
            if y in details["yearly"]:
                drive_usages.append(details["yearly"][y]["percent_time_in_use"])
                min_epoch = min(min_epoch, details["yearly"][y]["min_epoch_in_range"])
                max_epoch = max(max_epoch, details["yearly"][y]["max_epoch_in_range"])
        drive_usages = sorted(drive_usages)
        summary["yearly"][y]["drive_busy"] = dict()
        summary["yearly"][y]["drive_busy"]["min"] = drive_usages[0]
        summary["yearly"][y]["drive_busy"]["max"] = drive_usages[-1]
        summary["yearly"][y]["drive_busy"]["mean"] = float(sum(drive_usages) / len(drive_usages))
        summary["yearly"][y]["drive_busy"]["p5"] = percentile(drive_usages, P=0.05)
        summary["yearly"][y]["drive_busy"]["p25"] = percentile(drive_usages, P=0.25)
        summary["yearly"][y]["drive_busy"]["p50"] = percentile(drive_usages, P=0.5)
        summary["yearly"][y]["drive_busy"]["p75"] = percentile(drive_usages, P=0.75)
        summary["yearly"][y]["drive_busy"]["p95"] = percentile(drive_usages, P=0.95)

        summary["yearly"][y]["from_ts"] = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(min_epoch))
        summary["yearly"][y]["to_ts"] = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(max_epoch))

    return summary


def aggregate_logs(sanitized_logs_dir, analysis_results_dir):

    god = God(analysis_results_dir)
    # natural sorting will preserve timely order.
    all_log_files = sorted(glob.glob(os.path.join(sanitized_logs_dir, 'robot_mounts*.gz')))

    current_file_cnt = 1
    num_files = len(all_log_files)

    # key = cartridge id, value = [(mount_epoch,dismount_epoch,drive_id),(..),...]
    all_mounts = dict()

    cartridge_state = dict()
    eject_state = dict()

    print("Analysing %d files" % (num_files))
    # Read all Mounts/Dismounts into one giant Hashmap that is analyzed in the next step.
    for filename in all_log_files:
        logfile = os.path.join(sanitized_logs_dir, filename)
        with Timer("Finished: %d / %d  %s:" % (current_file_cnt, num_files, filename)):
            with gzip.open(logfile, 'r') as source_file:
                for line in source_file:
                    processed = False
                    match = re_line.search(line)
                    if match:
                        g = match.groups()
                        epoch = get_epoch(g[0])
                        action = g[1]
                        cartridge_id = g[2]
                        library_pos = g[3]  # not used right now.
                        rawdrive = string.split(g[5], ',')
                        if len(rawdrive)>3:
                            drive = "%i%02i%i%02i"%(int(rawdrive[0]),int(rawdrive[1]),int(rawdrive[2]),int(rawdrive[3]))
                        else:
                            drive = rawdrive

                        if action == "MOUNT":
                            cartridge_state[cartridge_id] = ("Mount", epoch)
                            god.handle_mount(cartridge_id, epoch, drive, library_pos)

                        elif action == "DISMOUNT":  # be aware of cleaning cartridges
                                t = cartridge_state.get(cartridge_id)
                            #if t and t[1] < epoch: # make sure that dismount is AFTER mount epoch.
                                if not cartridge_id in all_mounts:
                                    all_mounts[cartridge_id] = []
                                # store epochs of mounted and dismounted time stamps
                                #all_mounts[cartridge_id].append((t[1], epoch, drive))  !!!!!!!!!!!!!!!!!!!!!!!!!
                                #cartridge_state.pop(cartridge_id)
                                god.handle_dismount(cartridge_id, epoch, drive, library_pos)
                            #else:
                                #print "dismount on unmounted id:%s %s"%(cartridge_id,t)
                                #pass

                        elif action == "ENTER":
                            eject_state[cartridge_id] = ('Enter', epoch,drive)
                            god.handle_enter(cartridge_id, epoch)

                        elif action == "EJECT":
                            t = eject_state.get(cartridge_id)
                            if t and t[1] < epoch:
                                if not cartridge_id in all_mounts:
                                    all_mounts[cartridge_id] = []
                                #all_mounts[cartridge_id].append((t[1], epoch, drive))   !!!!!!!!!!!!!!!!!!!!!!!!!
                                god.handle_eject(cartridge_id, epoch)
                            else:
                            #    print "eject on unentered id:%s %s"%(cartridge_id,t)
                                pass

                        elif action == 'ACSMV':
                            god.handle_move(epoch, library_pos, drive)
                        processed = True

                    else:
                        match = re_line_not_found.search(line)
                        if match:
                            g = match.groups()
                            epoch = get_epoch(g[0])
                            action = g[1]
                            cartridge_id = g[2]
                            processed = True

                    if not processed:
                        print ("Bad line %s" % line)

                current_file_cnt += 1

    with Timer("Persisting all_mounts.json.gz: "):
        with gzip.open(os.path.join(analysis_results_dir, "all_mounts.json.gz"), 'w') as f:
            json.dump(all_mounts, f, indent=2)
    god.jsondump()

def analyze(analysis_results_dir, all_mounts_file):
    with Timer("Parsing all_mounts.json.gz: "):
        with gzip.open(all_mounts_file, 'r') as f:
            all_mounts = json.load(f)

    per_cartridge_id = dict()

    total_cartridge_mounts = 0

    num_tapes_per_group = dict()
    total_mounts_per_group = dict()

    tapes_per_type = dict()
    total_mounts_per_type = dict()

    global_min_epoch = 9999999999
    global_max_epoch = 0

    cartridge_groups = set()
    cartridge_types = set()
    cartridge_prefixes = set()

    available_months = set()

    num_items = len(all_mounts)

    with Timer("Cartridge Analysis: "):
        cnt = 0
        for cartridge_id, mounts_list in all_mounts.items():
            cnt += 1
            num_mounts = len(mounts_list)
            with Timer("processing cartridge stats %s: %d/%d - num elements: %d" % (cartridge_id, cnt, num_items, num_mounts)):

                total_cartridge_mounts += num_mounts

                cartridge_group = cartridge_id[:1]
                cartridge_type = cartridge_id[1:][:1]

                cartridge_prefixes.add(cartridge_id[:2])
                cartridge_groups.add(cartridge_group)
                cartridge_types.add(cartridge_type)

                 # stupid way to initialize all occuring groups
                if not cartridge_group in num_tapes_per_group:
                    num_tapes_per_group[cartridge_group] = 0
                if not cartridge_group in total_mounts_per_group:
                    total_mounts_per_group[cartridge_group] = 0
                if not cartridge_type in tapes_per_type:
                    tapes_per_type[cartridge_type] = 0
                if not cartridge_type in total_mounts_per_type:
                    total_mounts_per_type[cartridge_type] = 0

                # per cartridge
                details = dict()
                details["cartridge_group"] = cartridge_group
                details["cartridge_type"] = cartridge_type
                details["num_mounts"] = num_mounts
                md = get_mount_stats(cartridge_id, mounts_list)

                if not "all" in md:
                    print ("ERROR! md == None: %s:%s" % (cartridge_id, mounts_list))

                if "monthly" in md:
                    for month_id in md["monthly"].keys():
                        available_months.add(month_id)
                details["mount_details"] = md

                # extend globally known earliest and latest epochs.
                global_min_epoch = min(global_min_epoch, details["mount_details"]["first_mount_epoch"])
                global_max_epoch = max(global_max_epoch, details["mount_details"]["last_dismounted_epoch"])

                per_cartridge_id[cartridge_id] = details

                # global summaries
                num_tapes_per_group[cartridge_group] += 1
                total_mounts_per_group[cartridge_group] += num_mounts
                tapes_per_type[cartridge_type] += 1
                total_mounts_per_type[cartridge_type] += num_mounts

    per_drive_id = dict()
    drive_mount_details = dict()
    all_drive_ids = set()
    with Timer("Drive Analysis"):
        cnt = 0

        for cartridge_id, mounts_list in all_mounts.items():
            for mount in mounts_list:
                mount_from = mount[0]
                mount_to = mount[1]
                drive_id = mount[2]
                all_drive_ids.add(drive_id)
                if drive_id not in drive_mount_details:
                    drive_mount_details[drive_id] = []
                drive_mount_details[drive_id].append((mount_from, mount_to, cartridge_id))

        for drive_id, mount_details in drive_mount_details.items():
            mount_details = sorted(mount_details)
            per_drive_id[drive_id] = dict()
            per_drive_id[drive_id]["total_mounts"] = len(mount_details)

            per_drive_id[drive_id] = get_drive_stats(drive_id, mount_details)

    with Timer("Persisting Results"):
        # now aggregate results into one stats object
        stats = dict()
        stats["per_cartridge_id"] = per_cartridge_id
        stats["total_cartridge_mounts"] = total_cartridge_mounts

        stats["total_drives"] = len(all_drive_ids)
        stats["drive_ids"] = list(all_drive_ids)
        stats["per_drive_id"] = per_drive_id

        stats["num_tapes_per_group"] = num_tapes_per_group
        stats["total_mounts_per_group"] = total_mounts_per_group

        stats["tapes_per_type"] = tapes_per_type
        stats["total_mounts_per_type"] = total_mounts_per_type

        stats["global_min_epoch"] = global_min_epoch
        stats["global_max_epoch"] = global_max_epoch

        available_months_list = sorted(list(available_months))
        stats["available_months"] = available_months_list
        stats["total_summary"] = analyze_totals(available_months_list, per_cartridge_id, per_drive_id)

        with gzip.open(os.path.join(analysis_results_dir, "stats.json.gz"), 'w') as f:
            json.dump(stats, f, indent=2, sort_keys=True)


def cartridge_mount_stats_to_csv(stats_dict, target_csv_file, verbose=False):
    with Timer("Generating: %s" % target_csv_file):
        with open("/tmp/present_errors.log", 'w') as errors:
            with open(target_csv_file, 'w') as csv_file:

                months = sorted(stats_dict["available_months"])
                years = []

                from_time = time.gmtime(stats_dict["global_min_epoch"])
                to_time = time.gmtime(stats_dict["global_max_epoch"])

                for y in range(from_time.tm_year, to_time.tm_year + 1):
                    years.append(str(y))

                print months
                print years

                lineBuf = StringIO.StringIO()
                lineBuf.write(
                        "cartridge_id;"
                        +"num_mounts;"
                        +"cartridge_type;"
                        +"cartridge_group;"
                        +"first_mount_epoch;"
                        +"last_dismounted_epoch;"
                        +"total_tbm_min;"
                        +"total_tbm_max;"
                        +"total_tbm_mean;"
                        +"total_tbm_sum;"
                        +"total_tbm_p5;"
                        +"total_tbm_p25;"
                        +"total_tbm_p50;"
                        +"total_tbm_p75;"
                        +"total_tbm_p95;"
                        +"total_tm_min;"
                        +"total_tm_max;"
                        +"total_tm_mean;"
                        +"total_tm_sum;"
                        +"total_tm_p5;"
                        +"total_tm_p25;"
                        +"total_tm_p50;"
                        +"total_tm_p75;"
                        +"total_tm_p95;"
                        +"drive_affinity_different_drives;"
                        +"drive_affinity_same_60;"
                        +"drive_affinity_other_60;"
                        +"drive_affinity_same_300;"
                        +"drive_affinity_other_300;"
                )
                for year in years:
                    lineBuf.write(str(year) + "_mounts;")
                    lineBuf.write(str(year) + "_tbm_min;")
                    lineBuf.write(str(year) + "_tbm_max;")
                    lineBuf.write(str(year) + "_tbm_mean;")
                    lineBuf.write(str(year) + "_tbm_sum;")
                    lineBuf.write(str(year) + "_tbm_p5;")
                    lineBuf.write(str(year) + "_tbm_p25;")
                    lineBuf.write(str(year) + "_tbm_p50;")
                    lineBuf.write(str(year) + "_tbm_p75;")
                    lineBuf.write(str(year) + "_tbm_p95;")
                    lineBuf.write(str(year) + "_tm_min;")
                    lineBuf.write(str(year) + "_tm_max;")
                    lineBuf.write(str(year) + "_tm_mean;")
                    lineBuf.write(str(year) + "_tm_sum;")
                    lineBuf.write(str(year) + "_tm_p5;")
                    lineBuf.write(str(year) + "_tm_p25;")
                    lineBuf.write(str(year) + "_tm_p50;")
                    lineBuf.write(str(year) + "_tm_p75;")
                    lineBuf.write(str(year) + "_tm_p95;")

                    lineBuf.write(str(year) + "_drive_affinity_different_drives;")
                    lineBuf.write(str(year) + "_drive_affinity_same_60;")
                    lineBuf.write(str(year) + "_drive_affinity_other_60;")
                    lineBuf.write(str(year) + "_drive_affinity_same_300;")
                    lineBuf.write(str(year) + "_drive_affinity_other_300;")

                if verbose:
                    for month in months:
                        lineBuf.write(month + "_mounts;")
                        lineBuf.write(month + "_tbm_min;")
                        lineBuf.write(month + "_tbm_max;")
                        lineBuf.write(month + "_tbm_mean;")
                        lineBuf.write(month + "_tbm_sum;")
                        lineBuf.write(month + "_tbm_p5;")
                        lineBuf.write(month + "_tbm_p25;")
                        lineBuf.write(month + "_tbm_p50;")
                        lineBuf.write(month + "_tbm_p75;")
                        lineBuf.write(month + "_tbm_p95;")
                        lineBuf.write(month + "_tm_min;")
                        lineBuf.write(month + "_tm_max;")
                        lineBuf.write(month + "_tm_mean;")
                        lineBuf.write(month + "_tm_sum;")
                        lineBuf.write(month + "_tm_p5;")
                        lineBuf.write(month + "_tm_p25;")
                        lineBuf.write(month + "_tm_p50;")
                        lineBuf.write(month + "_tm_p75;")
                        lineBuf.write(month + "_tm_p95;")
                        lineBuf.write(month + "_drive_affinity_different_drives;")
                        lineBuf.write(month + "_drive_affinity_same_60;")
                        lineBuf.write(month + "_drive_affinity_other_60;")
                        lineBuf.write(month + "_drive_affinity_same_300;")
                        lineBuf.write(month + "_drive_affinity_other_300;")

                lineBuf.write("\n")
                csv_file.write(lineBuf.getvalue())




                for cartridge_id, details in stats_dict["per_cartridge_id"].items():
                    # reset linebuf.
                    lineBuf.truncate(0)
                    #print cartridge_id
                    if details["num_mounts"] > 1:
                        lineBuf.write("%s;%d;%s;%s;%d;%d;%.3f;%.3f;%.3f;%.3f;%.3f;%.3f;%.3f;%.3f;%.3f;%.3f;%.3f;%.3f;%.3f;%.3f;%.3f;%.3f;%.3f;%.3f;%d;%d;%d;%d;%d;" % (
                            cartridge_id,
                            details["num_mounts"],
                            details["cartridge_type"],
                            details["cartridge_group"],
                            details["mount_details"].get("first_mount_epoch", 0),
                            details["mount_details"].get("last_dismounted_epoch", 0),
                            details["mount_details"]["all"]["time_between_mounts"].get("min", 0.0),
                            details["mount_details"]["all"]["time_between_mounts"].get("max", 0.0),
                            details["mount_details"]["all"]["time_between_mounts"].get("mean", 0.0),
                            details["mount_details"]["all"]["time_between_mounts"].get("sum", 0.0),
                            details["mount_details"]["all"]["time_between_mounts"].get("p5", 0.0),
                            details["mount_details"]["all"]["time_between_mounts"].get("p25", 0.0),
                            details["mount_details"]["all"]["time_between_mounts"].get("p50", 0.0),
                            details["mount_details"]["all"]["time_between_mounts"].get("p75", 0.0),
                            details["mount_details"]["all"]["time_between_mounts"].get("p95", 0.0),
                            details["mount_details"]["all"]["time_mounted"].get("min", 0.0),
                            details["mount_details"]["all"]["time_mounted"].get("max", 0.0),
                            details["mount_details"]["all"]["time_mounted"].get("mean", 0.0),
                            details["mount_details"]["all"]["time_mounted"].get("sum", 0.0),
                            details["mount_details"]["all"]["time_mounted"].get("p5", 0.0),
                            details["mount_details"]["all"]["time_mounted"].get("p25", 0.0),
                            details["mount_details"]["all"]["time_mounted"].get("p50", 0.0),
                            details["mount_details"]["all"]["time_mounted"].get("p75", 0.0),
                            details["mount_details"]["all"]["time_mounted"].get("p95", 0.0),
                            details["mount_details"]["all"]["drive_affinity"].get("different_drives", 0),
                            details["mount_details"]["all"]["drive_affinity"].get("same_60", 0),
                            details["mount_details"]["all"]["drive_affinity"].get("other_60", 0),
                            details["mount_details"]["all"]["drive_affinity"].get("same_300", 0),
                            details["mount_details"]["all"]["drive_affinity"].get("other_300", 0)
                            )
                        )

                        for year in years:
                            if year in details["mount_details"]["yearly"]:
                                ydetail = details["mount_details"]["yearly"][year]
                                if ydetail["total_mounts"] > 1:
                                    lineBuf.write("%d;%.3f;%.3f;%.3f;%.3f;%.3f;%.3f;%.3f;%.3f;%.3f;%.3f;%.3f;%.3f;%.3f;%.3f;%.3f;%.3f;%.3f;%.3f;%d;%d;%d;%d;%d;" % (
                                        ydetail["total_mounts"],
                                        ydetail["time_between_mounts"].get("min", 0.0),
                                        ydetail["time_between_mounts"].get("max", 0.0),
                                        ydetail["time_between_mounts"].get("mean", 0.0),
                                        ydetail["time_between_mounts"].get("sum", 0.0),
                                        ydetail["time_between_mounts"].get("p5", 0.0),
                                        ydetail["time_between_mounts"].get("p25", 0.0),
                                        ydetail["time_between_mounts"].get("p50", 0.0),
                                        ydetail["time_between_mounts"].get("p75", 0.0),
                                        ydetail["time_between_mounts"].get("p95", 0.0),
                                        ydetail["time_mounted"].get("min", 0.0),
                                        ydetail["time_mounted"].get("max", 0.0),
                                        ydetail["time_mounted"].get("mean", 0.0),
                                        ydetail["time_mounted"].get("sum", 0.0),
                                        ydetail["time_mounted"].get("p5", 0.0),
                                        ydetail["time_mounted"].get("p25", 0.0),
                                        ydetail["time_mounted"].get("p50", 0.0),
                                        ydetail["time_mounted"].get("p75", 0.0),
                                        ydetail["time_mounted"].get("p95", 0.0),
                                        ydetail["drive_affinity"].get("different_drives", 0),
                                        ydetail["drive_affinity"].get("same_60", 0),
                                        ydetail["drive_affinity"].get("other_60", 0),
                                        ydetail["drive_affinity"].get("same_300", 0),
                                        ydetail["drive_affinity"].get("other_300", 0)
                                        )
                                    )
                                else:
                                    lineBuf.write("%d;" % ydetail["total_mounts"])
                                    lineBuf.write("0;" * 23)

                            else:
                                # write an empty year..
                                lineBuf.write("0;" * 24)
                        if verbose:
                            for month in months:
                                if month in details["mount_details"]["monthly"]:
                                    mdetail = details["mount_details"]["monthly"][month]
                                    if mdetail["total_mounts"] > 1:
                                        lineBuf.write("%d;%.3f;%.3f;%.3f;%.3f;%.3f;%.3f;%.3f;%.3f;%.3f;%.3f;%.3f;%.3f;%.3f;%.3f;%.3f;%.3f;%.3f;%.3f;%d;%d;%d;%d;%d;" % (
                                            mdetail["total_mounts"],
                                            mdetail["time_between_mounts"].get("min", 0.0),
                                            mdetail["time_between_mounts"].get("max", 0.0),
                                            mdetail["time_between_mounts"].get("mean", 0.0),
                                            mdetail["time_between_mounts"].get("sum", 0.0),
                                            mdetail["time_between_mounts"].get("p5", 0.0),
                                            mdetail["time_between_mounts"].get("p25", 0.0),
                                            mdetail["time_between_mounts"].get("p50", 0.0),
                                            mdetail["time_between_mounts"].get("p75", 0.0),
                                            mdetail["time_between_mounts"].get("p95", 0.0),
                                            mdetail["time_mounted"].get("min", 0.0),
                                            mdetail["time_mounted"].get("max", 0.0),
                                            mdetail["time_mounted"].get("mean", 0.0),
                                            mdetail["time_mounted"].get("sum", 0.0),
                                            mdetail["time_mounted"].get("p5", 0.0),
                                            mdetail["time_mounted"].get("p25", 0.0),
                                            mdetail["time_mounted"].get("p50", 0.0),
                                            mdetail["time_mounted"].get("p75", 0.0),
                                            mdetail["time_mounted"].get("p95", 0.0),
                                            mdetail["drive_affinity"].get("different_drives", 0),
                                            mdetail["drive_affinity"].get("same_60", 0),
                                            mdetail["drive_affinity"].get("other_60", 0),
                                            mdetail["drive_affinity"].get("same_300", 0),
                                            mdetail["drive_affinity"].get("other_300", 0)
                                            )
                                        )
                                    else:
                                        lineBuf.write("%d;" % mdetail["total_mounts"])
                                        lineBuf.write("0;" * 23)
                                else:
                                    # write an empty month..
                                    lineBuf.write("0;" * 24)
                    else:
                        lineBuf.write("%s;%d;%s;%s;%d;%d;%.3f;%.3f;%.3f;%.3f;%.3f;%.3f;%.3f;%.3f;%.3f;%.3f;%.3f;%.3f;%.3f;%.3f;%.3f;%.3f;%.3f;%.3f;%d;%d;%d;%d;%d;" % (
                            cartridge_id,
                            details["num_mounts"],
                            details["cartridge_type"],
                            details["cartridge_group"],
                            details["mount_details"].get("first_mount_epoch", 0),
                            details["mount_details"].get("last_dismounted_epoch", 0),
                            0,
                            0,
                            0,
                            0,
                            0,
                            0,
                            0,
                            0,
                            0,
                            0,
                            0,
                            0,
                            0,
                            0,
                            0,
                            0,
                            0,
                            0,
                            1,
                            0,
                            0,
                            0,
                            0
                            )
                        )

                        for year in years:
                            if year in details["mount_details"]["yearly"]:
                                ydetail = details["mount_details"]["yearly"][year]
                                lineBuf.write("%d;" % ydetail["total_mounts"])
                                lineBuf.write("0;" * 23)
                            else:
                                # write an empty month..
                                lineBuf.write("0;" * 24)

                        if verbose:
                            for month in months:
                                if month in details["mount_details"]["monthly"]:
                                    mdetail = details["mount_details"]["monthly"][month]
                                    lineBuf.write("%d;" % mdetail["total_mounts"])
                                    lineBuf.write("0;" * 23)
                                else:
                                    # write an empty month..
                                    lineBuf.write("0;" * 24)

                    lineBuf.write("\n")
                    csv_file.write(lineBuf.getvalue())

def monthly_summary_to_csv(stats_dict, target_csv_file):
    with Timer("Generating: %s" % target_csv_file):
        with open("/tmp/present_errors.log", 'w') as errors:
            with open(target_csv_file, 'w') as csv_file:

                months = sorted(stats_dict["available_months"])
                years = []

                from_time = time.gmtime(stats_dict["global_min_epoch"])
                to_time = time.gmtime(stats_dict["global_max_epoch"])

                for y in range(from_time.tm_year, to_time.tm_year + 1):
                    years.append(str(y))

                lineBuf = StringIO.StringIO()
                lineBuf.write(
                        "month;"
                        +"total_mounts;"
                        +"drives_busy_min;"
                        +"drives_busy_max;"
                        +"drives_busy_mean;"
                        +"drives_busy_p5;"
                        +"drives_busy_p25;"
                        +"drives_busy_p50;"
                        +"drives_busy_p75;"
                        +"drives_busy_p95;"
                        +"mounts_per_cartridge_min;"
                        +"mounts_per_cartridge_max;"
                        +"mounts_per_cartridge_mean;"
                        +"mounts_per_cartridge_p5;"
                        +"mounts_per_cartridge_p25;"
                        +"mounts_per_cartridge_p50;"
                        +"mounts_per_cartridge_p75;"
                        +"mounts_per_cartridge_p95;"
                        +"other_300;"
                        +"other_60;"
                        +"same_300;"
                        +"same_60;"

                )

                lineBuf.write("\n")

                csv_file.write(lineBuf.getvalue())

                for month, details in stats_dict["total_summary"]["monthly"].items():
                    # reset linebuf.
                    lineBuf.truncate(0)
                    lineBuf.write("%s;%d;%.3f;%.3f;%.3f;%.3f;%.3f;%.3f;%.3f;%.3f;%.3f;%.3f;%.3f;%.3f;%.3f;%.3f;%.3f;%.3f;%d;%d;%d;%d;" % (
                            month,
                            details["total_mounts"],
                            details["drive_busy"]["min"],
                            details["drive_busy"]["max"],
                            details["drive_busy"]["mean"],
                            details["drive_busy"]["p5"],
                            details["drive_busy"]["p25"],
                            details["drive_busy"]["p50"],
                            details["drive_busy"]["p75"],
                            details["drive_busy"]["p95"],

                            details["mounts_per_cartridge"]["min"],
                            details["mounts_per_cartridge"]["max"],
                            details["mounts_per_cartridge"]["mean"],
                            details["mounts_per_cartridge"]["p5"],
                            details["mounts_per_cartridge"]["p25"],
                            details["mounts_per_cartridge"]["p50"],
                            details["mounts_per_cartridge"]["p75"],
                            details["mounts_per_cartridge"]["p95"],

                            details["other_300"],
                            details["other_60"],
                            details["same_300"],
                            details["same_60"]
                        )
                    )

                    lineBuf.write("\n")
                    csv_file.write(lineBuf.getvalue())

def present(analysis_results_dir, stats_file_name):
    with Timer("Parsing stats.json.gz:"):
        stats_path = os.path.join(analysis_results_dir, stats_file_name)
        with gzip.open(stats_path, 'r') as f:
            stats = json.load(f)

    for key in stats.keys():
        print ("stats: %s" % key)

    cartridge_mount_stats_to_csv(stats, os.path.join(analysis_results_dir, "cartridge_mounts.csv"))
    cartridge_mount_stats_to_csv(stats, os.path.join(analysis_results_dir, "cartridge_mounts_verbose.csv"), verbose=True)
    monthly_summary_to_csv(stats, os.path.join(analysis_results_dir, "monthly_summary.csv"))





def main(argv):
    if len(argv) < 2:
        sys.stderr.write("Usage: %s (aggregate|analyze|present) args" % (argv[0],))
        return 1

    if argv[1] == 'aggregate':
        print 'aggregate'
        sanitized_logs_dir = os.path.abspath(argv[2])
        analysis_results_dir = os.path.abspath(argv[3])
        print sanitized_logs_dir
        print analysis_results_dir
        aggregate_logs(sanitized_logs_dir, analysis_results_dir)

    elif argv[1] == 'analyze':
        analysis_results_dir = os.path.abspath(argv[2])
        all_mounts_files = os.path.join(analysis_results_dir, "all_mounts.json.gz")
        analyze(analysis_results_dir, all_mounts_files)

    elif argv[1] == 'present':
        print 'present'
        analysis_results_dir = os.path.abspath(argv[2])
        print analysis_results_dir
        present(analysis_results_dir, "stats.json.gz")

if __name__ == "__main__":
    sys.exit(main(sys.argv))