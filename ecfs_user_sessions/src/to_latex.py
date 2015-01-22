#!/usr/bin/env python3
import json
import sys


def prettyfy(number):
    d = float(number)
    if d - int(d) > 0:
        return '{:,.2f}'.format(d)
    return '{:,d}'.format(int(d))


def to_latex(source_file):
    with open(source_file, 'r') as sf:
        r = json.load(sf)

    print("\n\n\n\n\n\n")


    print("\\begin{table*}[ht!]")
    print("\\scriptsize")
    print("\\centering")
    print("{")
    print("\\begin{tabular}{|r|r|r|r|r|r||r|}")
    print("\\hline")
    print("\\textbf{Key} & \\textbf{P05} & \\textbf{P50} & \\textbf{mean (+-95\\%)} & \\textbf{P95} & \\textbf{P99} & \\textbf{Count} \\\\\\hline")

    print("\#Sessions per user\_id & %s & %s & %s & %s & %s & %s \\\\\\hline" % (
        prettyfy(r["sessions_per_user_id"]["p05"]),
        prettyfy(r["sessions_per_user_id"]["p50"]),
        r["sessions_per_user_id"]["mean_95conf"],
        prettyfy(r["sessions_per_user_id"]["p95"]),
        prettyfy(r["sessions_per_user_id"]["p99"]),
        prettyfy(int(r["sessions_per_user_id"]["counted_sessions"]))
        ))

    print("\#Sessions per user\_id@host & %s & %s & %s & %s & %s & %s \\\\\\hline" % (
        prettyfy(r["sessions_per_user_id_per_host"]["p05"]),
        prettyfy(r["sessions_per_user_id_per_host"]["p50"]),
        r["sessions_per_user_id_per_host"]["mean_95conf"],
        prettyfy(r["sessions_per_user_id_per_host"]["p95"]),
        prettyfy(r["sessions_per_user_id_per_host"]["p99"]),
        prettyfy(int(r["sessions_per_user_id_per_host"]["counted_sessions"]))
        ))

    print("Total \# Actions & %s & %s & %s & %s & %s & %s \\\\\\hline" % (
        prettyfy(r["total_actions_per_session"]["p05"]),
        prettyfy(r["total_actions_per_session"]["p50"]),
        r["total_actions_per_session"]["mean_95conf"],
        prettyfy(r["total_actions_per_session"]["p95"]),
        prettyfy(r["total_actions_per_session"]["p99"]),
        prettyfy(int(r["total_actions_per_session"]["counted_sessions"]))
        ))

    print("GET Requests per session & %s & %s & %s & %s & %s & %s \\\\\\hline" % (
        prettyfy(r["get_requests_per_session"]["p05"]),
        prettyfy(r["get_requests_per_session"]["p50"]),
        r["get_requests_per_session"]["mean_95conf"],
        prettyfy(r["get_requests_per_session"]["p95"]),
        prettyfy(r["get_requests_per_session"]["p99"]),
        prettyfy(int(r["get_requests_per_session"]["counted_sessions"]))
        ))

    print("ReGET requests per session & %s & %s & %s & %s & %s & %s \\\\\\hline" % (
        prettyfy(r["reget_requests_per_session"]["p05"]),
        prettyfy(r["reget_requests_per_session"]["p50"]),
        r["reget_requests_per_session"]["mean_95conf"],
        prettyfy(r["reget_requests_per_session"]["p95"]),
        prettyfy(r["reget_requests_per_session"]["p99"]),
        prettyfy(int(r["reget_requests_per_session"]["counted_sessions"]))
        ))
    
    print("PUT Requests per session & %s & %s & %s & %s & %s & %s \\\\\\hline" % (
        prettyfy(r["put_requests_per_session"]["p05"]),
        prettyfy(r["put_requests_per_session"]["p50"]),
        r["put_requests_per_session"]["mean_95conf"],
        prettyfy(r["put_requests_per_session"]["p95"]),
        prettyfy(r["put_requests_per_session"]["p99"]),
        prettyfy(int(r["put_requests_per_session"]["counted_sessions"]))
        ))

    print("Dirs with GETs & %s & %s & %s & %s & %s & %s \\\\\\hline" % (
        prettyfy(r["get_dirs_per_session"]["p05"]),
        prettyfy(r["get_dirs_per_session"]["p50"]),
        r["get_dirs_per_session"]["mean_95conf"],
        prettyfy(r["get_dirs_per_session"]["p95"]),
        prettyfy(r["get_dirs_per_session"]["p99"]),
        prettyfy(int(r["get_dirs_per_session"]["counted_sessions"]))
        ))

    print("Dirs with PUTs & %s & %s & %s & %s & %s & %s\\\\\\hline" % (
        prettyfy(r["put_dirs_per_session"]["p05"]),
        prettyfy(r["put_dirs_per_session"]["p50"]),
        r["put_dirs_per_session"]["mean_95conf"],
        prettyfy(r["put_dirs_per_session"]["p95"]),
        prettyfy(r["put_dirs_per_session"]["p99"]),
        prettyfy(int(r["put_dirs_per_session"]["counted_sessions"]))
        ))
    
    print("Retrieved files per directory & %s & %s & %s & %s & %s & %s \\\\\\hline" % (
        prettyfy(r["get_files_per_dir"]["p05"]),
        prettyfy(r["get_files_per_dir"]["p50"]),
        r["get_files_per_dir"]["mean_95conf"],
        prettyfy(r["get_files_per_dir"]["p95"]),
        prettyfy(r["get_files_per_dir"]["p99"]),
        prettyfy(int(r["get_files_per_dir"]["counted_sessions"]))
        ))

    print("Archived files per directory & %s & %s & %s & %s & %s & %s \\\\\\hline" % (
        prettyfy(r["put_files_per_dir"]["p05"]),
        prettyfy(r["put_files_per_dir"]["p50"]),
        r["put_files_per_dir"]["mean_95conf"],
        prettyfy(r["put_files_per_dir"]["p95"]),
        prettyfy(r["put_files_per_dir"]["p99"]),
        prettyfy(int(r["put_files_per_dir"]["counted_sessions"]))
        ))

    print("Retrieved MBytes & %s & %s & %s & %s & %s & %s \\\\\\hline" % (
        prettyfy(r["get_mbytes_per_session"]["p05"]),
        prettyfy(r["get_mbytes_per_session"]["p50"]),
        r["get_mbytes_per_session"]["mean_95conf"],
        prettyfy(r["get_mbytes_per_session"]["p95"]),
        prettyfy(r["get_mbytes_per_session"]["p99"]),
        prettyfy(int(r["get_mbytes_per_session"]["counted_sessions"]))
        ))

    print("Archived MBytes & %s & %s & %s & %s & %s & %s \\\\\\hline" % (
        prettyfy(r["put_mbytes_per_session"]["p05"]),
        prettyfy(r["put_mbytes_per_session"]["p50"]),
        r["put_mbytes_per_session"]["mean_95conf"],
        prettyfy(r["put_mbytes_per_session"]["p95"]),
        prettyfy(r["put_mbytes_per_session"]["p99"]),
        prettyfy(int(r["put_mbytes_per_session"]["counted_sessions"]))
        ))

    print("Session lifetime in s & %s & %s & %s & %s & %s & %s \\\\\\hline" % (
        prettyfy(r["session_life_time_per_session"]["p05"]),
        prettyfy(r["session_life_time_per_session"]["p50"]),
        r["session_life_time_per_session"]["mean_95conf"],
        prettyfy(r["session_life_time_per_session"]["p95"]),
        prettyfy(r["session_life_time_per_session"]["p99"]),
        prettyfy(int(r["session_life_time_per_session"]["counted_sessions"]))
        ))


    print("Gap between sessions & %s & %s & %s & %s & %s & %s \\\\\\hline" % (
        prettyfy(r["window_seconds"]["p05"]),
        prettyfy(r["window_seconds"]["p50"]),
        r["window_seconds"]["mean_95conf"],
        prettyfy(r["window_seconds"]["p95"]),
        prettyfy(r["window_seconds"]["p99"]),
        prettyfy(int(r["window_seconds"]["counted_sessions"]))
        ))
 



    print("\\end{tabular}")
    print("}")
    print("\\caption{ECFS user session analysis. A total of %s sessions were identified.}" % ('{:,d}'.format(r["total_sessions_cnt"])))
    print("\\label{table:ecfs_user_sessions}")
    print("\\end{table*}")

    print("\n\n\n\n\n\n")



if __name__ == "__main__":
    source_file = sys.argv[1]
    #source_file = "user_session_statistics.json"
    to_latex(source_file)

