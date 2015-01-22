#!/usr/bin/env python

import os
import gzip
import sys

# assume all timestamps to be UTC.
import time
import calendar
import datetime

import sets
import re
import simplejson as json
import glob

versions={}
versions["analysis_version"] = 1

class Timer():
    def __init__(self, s):
        self.s = s

    def __enter__(self):
        self.start = time.time()

    def __exit__(self, *args):
        print ("%s: %fs" % (self.s, (time.time() - self.start)))



# assume result to be erroneous if mount_time is larger than x seconds
ERROR_TRESHOLD_MAX_MOUNT_TIME = 10 * 60  # 10 minutes

# assume result to be erroneous if mount_time is larger than x seconds
ERROR_TRESHOLD_MAX_MOUNTED_TIME = 5 * 24 * 60 * 60   # 5 days

# if a requested mount was not finished after 5 days of initial request, remove that.
THRESHOLD_REMOVE_ONGOING_AFTER_TIME = 5 * 24 * 60 * 60   # 5 days

# key is cartridge_id, value is the not-yet finished mount object
ongoing_mounts = {}

injected_cartridges = {}
ejected_cartridges = {}

# list of all seen cartridge_ids
cartridge_ids = sets.Set()

# the 'results'. map of all cartridge mounts and their timings.
finished_mounts = {}

# should be empty...
errors = {} 

stats = {}
stats["versions"] = versions
stats["total_cartridge_loads"] = 0
stats["min_epoch"] = 99362183288
stats["max_epoch"] = 0

stats["min_mount_time"] = 999999999
stats["max_mount_time"] = 0
stats["min_mounted_time"] = 999999999
stats["max_mounted_time"] = 0

# sanity checker to guarantee monotonic log reading.
last_epoch = 0

# 05/26 00:15:46 ***
re_time = re.compile("([0-9]{2})/([0-9]{2}) ([0-9]{2}):([0-9]{2}):([0-9]{2}) (RQST|DBUG) .*")
re_filename_date = re.compile("([0-9]{4})([0-9]{2})([0-9]{2})_([0-9]{2})([0-9]{2})([0-9]{2}).gz")


# 03/12 20:53:36 RQST PVRS0004 Entering pvr_Mount, cartridge = "CC0968", drive = "0"
re_pvr_Mount = re.compile(".* cartridge = \"([0-9A-Z]+)\".*")

# 03/07 17:51:11 DBUG PVRS0379 STK Request:   acs_mount: seq= 23557, cart= WB2125
re_acs_mount = re.compile(".* cart= ([0-9A-Z]+)")

# 05/26 00:15:46 RQST PVLS0002 Exiting, function = "pvl_MountCompleted", jobid = "11644979", drive = "101101", arg = "WB3134"
re_pvl_MountCompleted = re.compile(".* jobid = \"([0-9]+)\".* arg = \"([0-9A-Z]+)\"")

re_pvr_DismountCart = re.compile(".cartridge = \"([0-9A-Z]+)\".")

# 03/01 08:28:11 RQST PVRS0012 Entering pvr_Inject, cartridge = "RC2773", drive = "0"
re_pvr_Inject = re.compile(". cartridge = \"([0-9A-Z]+)\"")

# 03/01 08:23:37 EVNT PVRS0043 Ejecting cartridge="P54892", manufacturer="IBM LTO3-1", lot="Jul09", began service Tue Aug 25 12:44:27 2009, last maintained Thu Jan  1 00:00:00 1970, last mounted Tue Feb 26 22:10:33 2013, total mounts=12, mounts since last maintained=12
re_pvr_Eject = re.compile(".* cartridge=\"([0-9A-Z]+)\".*")


def epoch_to_timestamp(epoch):
    return time.strftime("%Y %m/%d %H:%M:%S", time.gmtime(epoch))

def get_epoch(log_creation_date, log_entry_date):
    """
        2012 05/25 00:19:27 -> 1370822690
        automatically fixes epochs on log files going from 12/31 -> 01/01
    """
    year = log_creation_date.year
    # when log_creation_date is 01/01, then the first entries are from the previous year
    if log_creation_date.month == 1 and log_creation_date.day == 1:
        if log_entry_date.month == 12 and log_entry_date.day == 31:
            log_entry_date = datetime.datetime(log_creation_date.year -1, log_entry_date.month, log_entry_date.day, log_entry_date.hour, log_entry_date.minute, log_entry_date.second)
    epoch = calendar.timegm(log_entry_date.utctimetuple())

    stats["min_epoch"] = min(stats["min_epoch"], epoch)
    stats["max_epoch"] = max(stats["max_epoch"], epoch)

    return epoch

def add_error(cartridge_id, error):
    if not cartridge_id in errors:
        errors[cartridge_id] = []
    errors[cartridge_id].append(error)

def move_to_finished(cartridge_id, result):
    if "INITIAL_REQUEST" in result and "MOUNT_COMPLETE" in result and "CARTRIDGE_DISMOUNTED" in result:
        result["mount_time"] = result["MOUNT_COMPLETE"] - result["INITIAL_REQUEST"]
        result["mounted_time"] = result["CARTRIDGE_DISMOUNTED"] - result["MOUNT_COMPLETE"]
    
        if result["mount_time"] < 0 or result["mount_time"] > ERROR_TRESHOLD_MAX_MOUNT_TIME:
            error = {}
            error["result"] = result
            error["timestamp"] = epoch_to_timestamp(result["INITIAL_REQUEST"])
            error["message"] = "result[mount_time] > ERROR_TRESHOLD_MAX_MOUNT_TIME"
            add_error(cartridge_id, error)
        elif result["mounted_time"] < 0 or result["mounted_time"] > ERROR_TRESHOLD_MAX_MOUNTED_TIME:
            error = {}
            error["timestamp"] = epoch_to_timestamp(result["INITIAL_REQUEST"])
            error["result"] = result
            error["message"] = "result[mounted_time] > ERROR_TRESHOLD_MAX_MOUNTED_TIME"
            add_error(cartridge_id, error)
        else:
            # results are fine
            # make sure that cartridge_id exists
            if not cartridge_id in finished_mounts:
                finished_mounts[cartridge_id] = []

            finished_mounts[cartridge_id].append(result)
            stats["total_cartridge_loads"] += 1

            stats["min_mount_time"] = min(stats["min_mount_time"], result["mount_time"] )
            stats["max_mount_time"] = max(stats["max_mount_time"], result["mount_time"])
            stats["min_mounted_time"] = min(stats["min_mounted_time"], result["mounted_time"])
            stats["max_mounted_time"] = max(stats["max_mounted_time"], result["mounted_time"])

    else:
        error = {}
        error["result"] = result
        error["message"] = "incomplete result"
        add_error(cartridge_id, error)

def clean_ongoing_mounts():
    for cartridge_id, details in ongoing_mounts.items():
        if last_epoch - details["INITIAL_REQUEST"] >= THRESHOLD_REMOVE_ONGOING_AFTER_TIME:
            r = ongoing_mounts.pop(cartridge_id)
            error = {}
            error["epoch"] = details["INITIAL_REQUEST"]
            error["timestamp"] = epoch_to_timestamp(details["INITIAL_REQUEST"])
            error["message"] = "No pvr_DismountCart after THRESHOLD_REMOVE_ONGOING_AFTER_TIME"
            error["details"] =  r
            add_error(cartridge_id, error)

def convert(f, god):
    global last_epoch

    clean_ongoing_mounts()

    print os.path.abspath(f)
    # expect year to be always at the same position of the filename
    
    m = re.search(re_filename_date, os.path.basename(f))
    if not m:
        print ("ERROR, cannot process invalid file name: %s" % (f))
        return
    x = m.groups()
    log_creation_date = datetime.datetime(int(x[0]), int(x[1]),int(x[2]),int(x[3]),int(x[4]),int(x[5]))
    

    with gzip.open(f, 'r') as source:
        for line in source.readlines():
            try:
                time_match = re_time.search(line)
                if time_match:
                    x = time_match.groups()
                    log_entry_date = datetime.datetime(log_creation_date.year, int(x[0]), int(x[1]),int(x[2]),int(x[3]),int(x[4]))
                    epoch = get_epoch(log_creation_date, log_entry_date)

                    if epoch >= last_epoch:
                        # chronologic order
                        last_epoch = epoch
                    else:
                        print "Hard abort due to chronologic error in line: \n\t%s" % (line)
                        sys.exit(1)

                    #elements = line.split(",") # never used!?

                    # once per 'cartridge_mount_process'
                    if line.__contains__("Entering") and line.__contains__("pvr_Mount"):
                        # cartridge is going to be mounted. overwrite ongoing processes for this cartridge. singlethreaded...
                        match = re_pvr_Mount.search(line)
                        if match:
                            cartridge_id = match.groups()[0][:6]
                            
                            cartridge_ids.add(cartridge_id)

                            if cartridge_id in ongoing_mounts:
                                # overwriting a previous process, so move the unfinished to the errors
                                error = {}
                                error["epoch"] = epoch
                                error["timestamp"] = epoch_to_timestamp(epoch)
                                error["unfinished"] = ongoing_mounts[cartridge_id]
                                error["message"] = "pvr_Mount overwriting unfinished mount process"
                                add_error(cartridge_id, error)
                            ongoing_mounts[cartridge_id] = {}
                            ongoing_mounts[cartridge_id]["INITIAL_REQUEST"] = epoch

                        else: 
                            print "bad line", line

                    elif line.__contains__("acs_mount"):
                    # sometimes, there is an acs_mount request without a preceeding pvr_mount request.
                    # if there was a pvr_mount for the same cartridge before, ignore this.
                        match = re_acs_mount.search(line)
                        if match:
                            cartridge_id = match.groups()[0][:6]
                            
                            if not cartridge_id in ongoing_mounts:
                                cartridge_ids.add(cartridge_id)
                                ongoing_mounts[cartridge_id] = {}
                                ongoing_mounts[cartridge_id]["INITIAL_REQUEST"] = epoch
                        else: 
                            print "bad line", line
                    # can happen multiple times within 'cartridge_mount_process' last seen pvl_MountCompleted is assumed to be successfull.
                    elif line.__contains__("Exiting") and line.__contains__("pvl_MountCompleted"):
                        match = re_pvl_MountCompleted.search(line)
                        if match:
                            job_id = match.groups()[0]
                            cartridge_id = match.groups()[1][:6]

                            if cartridge_id in ongoing_mounts:
                                ongoing_mounts[cartridge_id]["MOUNT_COMPLETE"] = epoch
                            else:
                                error = {}
                                error["epoch"] = epoch
                                error["timestamp"] = epoch_to_timestamp(epoch)
                                error["job_id"] = job_id
                                error["message"] = "pvl_MountCompleted for unknown cartridge_id"
                                add_error(cartridge_id, error)
                        else: 
                            print "bad line", line

                    elif line.__contains__("Exiting") and line.__contains__("pvr_DismountCart"):
                        match = re_pvr_DismountCart.search(line)
                        if match:
                            cartridge_id = match.groups()[0][:6]

                            if cartridge_id in ongoing_mounts:
                                ongoing_mounts[cartridge_id]["CARTRIDGE_DISMOUNTED"] = epoch
                                move_to_finished(cartridge_id, ongoing_mounts.pop(cartridge_id))
                            else:
                                error = {}
                                error["epoch"] = epoch
                                error["timestamp"] = epoch_to_timestamp(epoch)
                                error["message"] = "pvr_DismountCart for unknown cartridge_id"
                                add_error(cartridge_id, error)
                        else: 
                            print "bad line", line

                    # NEW CARTRIDGES IN, OLD CARTRIDGES OUT of the system                
                    elif line.__contains__("Entering") and line.__contains__("pvr_Inject"):
                        # a new cartridge is added, add it to list of injected_cartridges
                        match = re_pvr_Inject.search(line)
                        if match:
                            cartridge_id = match.groups()[0][:6]

                            if not cartridge_id in injected_cartridges:
                                injected_cartridges[cartridge_id] = []
                            
                            cartridge_ids.add(cartridge_id)

                            result = {}
                            result["epoch"] = epoch
                            result["timestamp"] = epoch_to_timestamp(epoch)
                            result["INITIAL_REQUEST"] = epoch
                            injected_cartridges[cartridge_id].append(result)
                        else: 
                            print "bad line", line

                    elif line.__contains__("Ejecting cartridge"):
                        match = re_pvr_Eject.search(line)
                        if match:
                            cartridge_id = match.groups()[0][:6]

                            if not cartridge_id in ejected_cartridges:
                                ejected_cartridges[cartridge_id] = []
                            
                            result = {}
                            result["epoch"] = epoch
                            result["timestamp"] = epoch_to_timestamp(epoch)
                            result["infos"] = line

                            ejected_cartridges[cartridge_id].append(result)
                        else:
                            print "bad line", line                        

            except Exception,e :
                print(e)
                import traceback
                traceback.print_exc()
                raise e

    print ("ongoing_mounts", len(ongoing_mounts))
    print ("finished_mounts", len(finished_mounts))
    print ("cartridge_ids", len(cartridge_ids))
    print ("errors", len(errors))
    print ("injected_cartridges.json", len(injected_cartridges))
    print ("ejected_cartridges.json", len(ejected_cartridges))
    print ("stats", stats)

    # for x,y in ongoing_mounts.items():
        # print x, len(y), y
    # print "-----------------------------------"
    # for x,y in erroneous_cartridge_ids.items():
        # print x, y




#cwd = os.path.dirname(__file__)
cwd = sys.argv[1]

# natural sorting will preserve timely order.
files = sorted(glob.glob(os.path.join(cwd,'whpss/whpss_log_*.gz')))
print cwd


current_file = 1
num_files = len(files)
for filename in files:
    f = os.path.join(cwd, filename)
    with Timer("working: %d / %d  %s:" % (current_file, num_files, filename)):
        convert(f)
    current_file += 1

with Timer("Writing out results:"):
    with open (os.path.join(cwd, "results", "ongoing_mounts.json"), 'w') as f:
        json.dump(ongoing_mounts, f, indent=2)

    with gzip.open (os.path.join(cwd, "results", "finished_mounts.json.gz"), 'w') as f:
        json.dump(finished_mounts, f, indent=2)

    with open (os.path.join(cwd, "results", "injected_cartridges.json"), 'w') as f: 
        json.dump(injected_cartridges, f, indent=2)

    with open (os.path.join(cwd, "results", "ejected_cartridges.json"), 'w') as f: 
        json.dump(ejected_cartridges, f, indent=2)

    with open (os.path.join(cwd, "results", "stats.json"), 'w') as f: 
        json.dump(stats, f, indent=2)
        
    with gzip.open (os.path.join(cwd, "results", "errors.json.gz"), 'w') as f:
        for error in errors:
            f.write(json.dumps(error, indent=2))
        
    with open (os.path.join(cwd, "results", "cartridge_ids.txt"), 'w') as f:
        for c in cartridge_ids:
            f.write("%s\n" % (c))
