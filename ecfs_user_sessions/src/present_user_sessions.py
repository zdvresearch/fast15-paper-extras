#!/usr/bin/env python
"""
    works on the filtered/merged/obfuscated files.
    for every day, get stats for number of files put/get/del, amount of bytes written / read / deleted.
"""

from collections import defaultdict
from collections import Counter
import sys
import os
import json

import statistics

stats = defaultdict(int)

def present(source_file, results_file):
    results = defaultdict(dict)

    time_between_all_sessions_list = list()
    
    sessions_per_user_id_counter = Counter()
    sessions_per_user_id_per_host_counter = Counter()
    
    put_files_per_dir_list = list()
    get_files_per_dir_list = list()
    get_requests_per_session_list = list()
    reget_requests_per_session_list = list()
    get_mbytes_per_session_list = list()
    put_requests_per_session_list = list()
    put_mbytes_per_session_list = list()
    get_dirs_per_session_list = list()
    put_dirs_per_session_list = list()
    total_actions_per_session_list = list()

    # times
    session_lifetimes_list = list()

    user_id_windows_seconds_dict = dict()

    total_sessions_cnt = 0

    with open (source_file, 'r') as sf:
        skipped_first = False
        for line in sf:
            if not skipped_first:
                skipped_first = True
                continue
            s = line.split(';')

            # print(line)
            user_id_host = s[0]
            from_ts = float(s[1])
            till_ts = float(s[2])
            session_lifetime = float(s[3])
            get_requests = float(s[4])
            reget_requests = float(s[5])
            put_requests = float(s[6])
            get_bytes = float(s[7])
            put_bytes = float(s[8])
            rename_requests = float(s[9])
            del_requests = float(s[10])
            get_dirs = float(s[11])
            put_dirs = float(s[12])
            put_files_per_dir = float(s[13])
            get_files_per_dir = float(s[14])
            window_second = float(s[15])
        
            total_sessions_cnt += 1

            sessions_per_user_id_counter[user_id_host.split("_")[0]] += 1
            sessions_per_user_id_per_host_counter[user_id_host] += 1

            if from_ts > 0:
                session_lifetimes_list.append(session_lifetime)
            else:
                session_lifetimes_list.append(0)

            if put_files_per_dir > 0:
                put_files_per_dir_list.append(put_files_per_dir)

            if get_files_per_dir > 0:
                get_files_per_dir_list.append(get_files_per_dir)

            if get_requests > 0:
                get_requests_per_session_list.append(get_requests)
            
            if get_requests > 0 or get_bytes > 0:
                get_mbytes_per_session_list.append(float(get_bytes) / 1024 / 1024)
            
            if reget_requests > 0:
                reget_requests_per_session_list.append(reget_requests)

            if put_requests > 0:
                put_requests_per_session_list.append(put_requests)
            
            if put_bytes > 0:
                put_mbytes_per_session_list.append(float(put_bytes) / 1024 / 1024)

            if get_dirs > 0:
                get_dirs_per_session_list.append(get_dirs)
            
            if put_dirs > 0:
                put_dirs_per_session_list.append(put_dirs)

            user_id_windows_seconds_dict[user_id_host] = window_second

            total_actions = put_requests + get_requests + rename_requests + del_requests
            total_actions_per_session_list.append(total_actions)

    print "read results, now, interpret them."

    sessions_per_user_id_list = sessions_per_user_id_counter.values()
    sessions_per_user_id_per_host_list = sessions_per_user_id_per_host_counter.values()

    # sort them for the percentiles
    sessions_per_user_id_list = sorted(sessions_per_user_id_list)
    sessions_per_user_id_per_host_list = sorted(sessions_per_user_id_per_host_list)
    put_files_per_dir_list = sorted(put_files_per_dir_list)
    get_files_per_dir_list = sorted(get_files_per_dir_list)
    get_requests_per_session_list = sorted(get_requests_per_session_list)
    reget_requests_per_session_list = sorted(reget_requests_per_session_list)
    get_mbytes_per_session_list = sorted(get_mbytes_per_session_list)
    put_requests_per_session_list = sorted(put_requests_per_session_list)
    put_mbytes_per_session_list = sorted(put_mbytes_per_session_list)
    get_dirs_per_session_list = sorted(get_dirs_per_session_list)
    put_dirs_per_session_list = sorted(put_dirs_per_session_list)
    total_actions_per_session_list = sorted(total_actions_per_session_list)
    session_lifetimes_list = sorted(session_lifetimes_list)

    window_seconds_list = sorted(list(user_id_windows_seconds_dict.values()))

    precision = 5

    results["sessions_per_user_id"]["counted_sessions"] = round(len(sessions_per_user_id_list), precision)
    results["sessions_per_user_id_per_host"]["counted_sessions"] = round(len(sessions_per_user_id_per_host_list), precision)
    results["put_files_per_dir"]["counted_sessions"] = round(len(put_files_per_dir_list), precision)
    results["get_files_per_dir"]["counted_sessions"] = round(len(get_files_per_dir_list), precision)
    results["get_requests_per_session"]["counted_sessions"] = round(len(get_requests_per_session_list), precision)
    results["reget_requests_per_session"]["counted_sessions"] = round(len(reget_requests_per_session_list), precision)
    results["get_mbytes_per_session"]["counted_sessions"] = round(len(get_mbytes_per_session_list), precision)
    results["put_requests_per_session"]["counted_sessions"] = round(len(put_requests_per_session_list), precision)
    results["put_mbytes_per_session"]["counted_sessions"] = round(len(put_mbytes_per_session_list), precision)
    results["get_dirs_per_session"]["counted_sessions"] = round(len(get_dirs_per_session_list), precision)
    results["put_dirs_per_session"]["counted_sessions"] = round(len(put_dirs_per_session_list), precision)
    results["total_actions_per_session"]["counted_sessions"] = round(len(total_actions_per_session_list), precision)
    results["session_life_time_per_session"]["counted_sessions"] = round(len(session_lifetimes_list), precision)
    results["window_seconds"]["counted_sessions"] = round(len(window_seconds_list), precision)


    results["sessions_per_user_id"]["mean"] = round(statistics.mean(sessions_per_user_id_list), precision)
    results["sessions_per_user_id_per_host"]["mean"] = round(statistics.mean(sessions_per_user_id_per_host_list), precision)
    results["put_files_per_dir"]["mean"] = round(statistics.mean(put_files_per_dir_list), precision)
    results["get_files_per_dir"]["mean"] = round(statistics.mean(get_files_per_dir_list), precision)
    results["get_requests_per_session"]["mean"] = round(statistics.mean(get_requests_per_session_list), precision)
    results["reget_requests_per_session"]["mean"] = round(statistics.mean(reget_requests_per_session_list), precision)
    results["get_mbytes_per_session"]["mean"] = round(statistics.mean(get_mbytes_per_session_list), precision)
    results["put_requests_per_session"]["mean"] = round(statistics.mean(put_requests_per_session_list), precision)
    results["put_mbytes_per_session"]["mean"] = round(statistics.mean(put_mbytes_per_session_list), precision)
    results["get_dirs_per_session"]["mean"] = round(statistics.mean(get_dirs_per_session_list), precision)
    results["put_dirs_per_session"]["mean"] = round(statistics.mean(put_dirs_per_session_list), precision)
    results["total_actions_per_session"]["mean"] = round(statistics.mean(total_actions_per_session_list), precision)
    results["session_life_time_per_session"]["mean"] = round(statistics.mean(session_lifetimes_list), precision)
    results["window_seconds"]["mean"] = round(statistics.mean(window_seconds_list), precision)


    results["sessions_per_user_id"]["min"] = round(min(sessions_per_user_id_list), precision)
    results["sessions_per_user_id_per_host"]["min"] = round(min(sessions_per_user_id_per_host_list), precision)
    results["put_files_per_dir"]["min"] = round(min(put_files_per_dir_list), precision)
    results["get_files_per_dir"]["min"] = round(min(get_files_per_dir_list), precision)
    results["get_requests_per_session"]["min"] = round(min(get_requests_per_session_list), precision)
    results["reget_requests_per_session"]["min"] = round(min(reget_requests_per_session_list), precision)
    results["get_mbytes_per_session"]["min"] = round(min(get_mbytes_per_session_list), precision)
    results["put_requests_per_session"]["min"] = round(min(put_requests_per_session_list), precision)
    results["put_mbytes_per_session"]["min"] = round(min(put_mbytes_per_session_list), precision)
    results["get_dirs_per_session"]["min"] = round(min(get_dirs_per_session_list), precision)
    results["put_dirs_per_session"]["min"] = round(min(put_dirs_per_session_list), precision)
    results["total_actions_per_session"]["min"] = round(min(total_actions_per_session_list), precision)
    results["session_life_time_per_session"]["min"] = round(min(session_lifetimes_list), precision)
    results["window_seconds"]["min"] = round(min(window_seconds_list), precision)
    

    results["sessions_per_user_id"]["max"] = round(max(sessions_per_user_id_list), precision)
    results["sessions_per_user_id_per_host"]["max"] = round(max(sessions_per_user_id_per_host_list), precision)
    results["put_files_per_dir"]["max"] = round(max(put_files_per_dir_list), precision)
    results["get_files_per_dir"]["max"] = round(max(get_files_per_dir_list), precision)
    results["get_requests_per_session"]["max"] = round(max(get_requests_per_session_list), precision)
    results["reget_requests_per_session"]["max"] = round(max(reget_requests_per_session_list), precision)
    results["get_mbytes_per_session"]["max"] = round(max(get_mbytes_per_session_list), precision)
    results["put_requests_per_session"]["max"] = round(max(put_requests_per_session_list), precision)
    results["put_mbytes_per_session"]["max"] = round(max(put_mbytes_per_session_list), precision)
    results["get_dirs_per_session"]["max"] = round(max(get_dirs_per_session_list), precision)
    results["put_dirs_per_session"]["max"] = round(max(put_dirs_per_session_list), precision)
    results["total_actions_per_session"]["max"] = round(max(total_actions_per_session_list), precision)
    results["session_life_time_per_session"]["max"] = round(max(session_lifetimes_list), precision)
    results["window_seconds"]["max"] = round(max(window_seconds_list), precision)


    for p in [("p05", 0.05), ("p25", 0.25), ("p50", 0.5), ("p75", 0.75), ("p95", 0.95), ("p99", 0.99)]:
        results["sessions_per_user_id"]["%s" % (p[0])] = round(statistics.percentile(sessions_per_user_id_list, P=p[1]), precision)
        results["sessions_per_user_id_per_host"]["%s" % (p[0])] = round(statistics.percentile(sessions_per_user_id_per_host_list, P=p[1]), precision)
        results["put_files_per_dir"]["%s" % (p[0])] = round(statistics.percentile(put_files_per_dir_list, P=p[1]), precision)
        results["get_files_per_dir"]["%s" % (p[0])] = round(statistics.percentile(get_files_per_dir_list, P=p[1]), precision)
        results["get_requests_per_session"]["%s" % (p[0])] = round(statistics.percentile(get_requests_per_session_list, P=p[1]), precision)
        results["reget_requests_per_session"]["%s" % (p[0])] = round(statistics.percentile(reget_requests_per_session_list, P=p[1]), precision)
        results["get_mbytes_per_session"]["%s" % (p[0])] = round(statistics.percentile(get_mbytes_per_session_list, P=p[1]), precision)
        results["put_requests_per_session"]["%s" % (p[0])] = round(statistics.percentile(put_requests_per_session_list, P=p[1]), precision)
        results["put_mbytes_per_session"]["%s" % (p[0])] = round(statistics.percentile(put_mbytes_per_session_list, P=p[1]), precision)
        results["get_dirs_per_session"]["%s" % (p[0])] = round(statistics.percentile(get_dirs_per_session_list, P=p[1]), precision)
        results["put_dirs_per_session"]["%s" % (p[0])] = round(statistics.percentile(put_dirs_per_session_list, P=p[1]), precision)
        results["total_actions_per_session"]["%s" % (p[0])] = round(statistics.percentile(total_actions_per_session_list, P=p[1]), precision)
        results["session_life_time_per_session"]["%s" % (p[0])] = round(statistics.percentile(session_lifetimes_list, P=p[1]), precision)
        results["window_seconds"]["%s" % (p[0])] = round(statistics.percentile(window_seconds_list, P=p[1]), precision)

    results["sessions_per_user_id"]["mean_95conf"] = statistics.get_meanconf_string(sessions_per_user_id_list) 
    results["sessions_per_user_id_per_host"]["mean_95conf"] = statistics.get_meanconf_string(sessions_per_user_id_per_host_list) 
    results["put_files_per_dir"]["mean_95conf"] = statistics.get_meanconf_string(put_files_per_dir_list) 
    results["get_files_per_dir"]["mean_95conf"] = statistics.get_meanconf_string(get_files_per_dir_list) 
    results["get_requests_per_session"]["mean_95conf"] = statistics.get_meanconf_string(get_requests_per_session_list) 
    results["reget_requests_per_session"]["mean_95conf"] = statistics.get_meanconf_string(reget_requests_per_session_list) 
    results["get_mbytes_per_session"]["mean_95conf"] = statistics.get_meanconf_string(get_mbytes_per_session_list) 
    results["put_requests_per_session"]["mean_95conf"] = statistics.get_meanconf_string(put_requests_per_session_list) 
    results["put_mbytes_per_session"]["mean_95conf"] = statistics.get_meanconf_string(put_mbytes_per_session_list) 
    results["get_dirs_per_session"]["mean_95conf"] = statistics.get_meanconf_string(get_dirs_per_session_list) 
    results["put_dirs_per_session"]["mean_95conf"] = statistics.get_meanconf_string(put_dirs_per_session_list) 
    results["total_actions_per_session"]["mean_95conf"] = statistics.get_meanconf_string(total_actions_per_session_list) 
    results["session_life_time_per_session"]["mean_95conf"] = statistics.get_meanconf_string(session_lifetimes_list) 
    results["window_seconds"]["mean_95conf"] = statistics.get_meanconf_string(window_seconds_list) 



    results["sessions_per_user_id"]["error_95conf"] = statistics.mean_confidence_interval_h(sessions_per_user_id_list) 
    results["sessions_per_user_id_per_host"]["error_95conf"] = statistics.mean_confidence_interval_h(sessions_per_user_id_per_host_list) 
    results["put_files_per_dir"]["error_95conf"] = statistics.mean_confidence_interval_h(put_files_per_dir_list) 
    results["get_files_per_dir"]["error_95conf"] = statistics.mean_confidence_interval_h(get_files_per_dir_list) 
    results["get_requests_per_session"]["error_95conf"] = statistics.mean_confidence_interval_h(get_requests_per_session_list) 
    results["reget_requests_per_session"]["error_95conf"] = statistics.mean_confidence_interval_h(reget_requests_per_session_list) 
    results["get_mbytes_per_session"]["error_95conf"] = statistics.mean_confidence_interval_h(get_mbytes_per_session_list) 
    results["put_requests_per_session"]["error_95conf"] = statistics.mean_confidence_interval_h(put_requests_per_session_list) 
    results["put_mbytes_per_session"]["error_95conf"] = statistics.mean_confidence_interval_h(put_mbytes_per_session_list) 
    results["get_dirs_per_session"]["error_95conf"] = statistics.mean_confidence_interval_h(get_dirs_per_session_list) 
    results["put_dirs_per_session"]["error_95conf"] = statistics.mean_confidence_interval_h(put_dirs_per_session_list) 
    results["total_actions_per_session"]["error_95conf"] = statistics.mean_confidence_interval_h(total_actions_per_session_list) 
    results["session_life_time_per_session"]["error_95conf"] = statistics.mean_confidence_interval_h(session_lifetimes_list) 
    results["window_seconds"]["error_95conf"] = statistics.mean_confidence_interval_h(window_seconds_list) 

    results["total_get_requests"] = sum(get_requests_per_session_list)
    results["total_reget_requests"] = sum(reget_requests_per_session_list)
    results["fraction_of_regets"] = float(results["total_reget_requests"]) / float(results["total_get_requests"])
    results["total_sessions_cnt"] = total_sessions_cnt



    with open(results_file, 'w') as rf:
        json.dump(results, rf, indent=2, sort_keys=True)

    print(json.dumps(results, indent=2, sort_keys=True))

    print("wrote results to: %s" % (results_file))

    print ("total_get_requests", results["total_get_requests"])
    print ("total_reget_requests", results["total_reget_requests"])
    print ("fraction_of_regets", results["fraction_of_regets"])

def to_latex(results):
    pass

    
if __name__ == "__main__":
    source_file = os.path.abspath(sys.argv[1])
    results_json = os.path.abspath(sys.argv[2])

    present(source_file, results_json)
    