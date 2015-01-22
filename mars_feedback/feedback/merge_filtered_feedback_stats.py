__author__ = 'meatz'

import os
import sys
import glob
import json
import re
import traceback

from collections import defaultdict

"""
    For every filtered reader log file, a .stat.json file is created.
    This class aggregates all these logs into one big stat file.
"""

stats = dict()

stats["totals"] = dict()
stats["totals"]["archive"] = dict()
stats["totals"]["retrieve"] = dict()
stats["by_database"] = dict()
stats["by_user"] = dict()

user_names = set()
db_names = set()

def read_stats(source_file):
    with open(source_file, 'r') as sf:
        s = json.load(sf)
        
        for timeframe in s.keys():
            # manually filter out some outliers
            print (timeframe)
            if timeframe in ["2009-12-31", "2014-05-17", "2014-05-18"]:
                print("skipping", timeframe)
                continue

            if re.match("(\d){4}-(\d){2}-(\d){2}", timeframe):
                # print (source_file, timeframe)

                if "archive" in s[timeframe]:

                    # TOTALS
                    archive_totals = s[timeframe]["archive"]["totals"]
                    
                    relevant_fields = ["bytes", "fields"]
                    
                    if timeframe not in stats["totals"]["archive"]:
                        stats["totals"]["archive"][timeframe] = dict()
                        for rv in relevant_fields:
                            stats["totals"]["archive"][timeframe][rv] = defaultdict(int)
                        
                    for rv in relevant_fields:
                        if rv in archive_totals:    
                            stats["totals"]["archive"][timeframe][rv]["sum"] += archive_totals[rv]["sum"]
                            stats["totals"]["archive"][timeframe][rv]["count"] += archive_totals[rv]["count"]

                    
                    # BY DATABASE
                    relevant_fields = ["bytes", "fields"]

                    if "by_database" in s[timeframe]["archive"]:
                        for db_name in s[timeframe]["archive"]["by_database"].keys():
                            db_names.add(db_name)
                            if db_name not in stats["by_database"]:
                                stats["by_database"][db_name] = dict()
                                stats["by_database"][db_name]["archive"] = dict()
                                stats["by_database"][db_name]["retrieve"] = dict()

                            if timeframe not in stats["by_database"][db_name]["archive"]:
                                stats["by_database"][db_name]["archive"][timeframe] = dict()
                                for rv in relevant_fields:
                                    stats["by_database"][db_name]["archive"][timeframe][rv] = defaultdict(int)

                        for db_name, values in s[timeframe]["archive"]["by_database"].items():
                            for rv in relevant_fields:
                                if rv in values:
                                    stats["by_database"][db_name]["archive"][timeframe][rv]["sum"] += values[rv]["sum"]
                                    stats["by_database"][db_name]["archive"][timeframe][rv]["count"] += values[rv]["count"]

                    # BY USER
                    relevant_fields = ["bytes", "fields"]

                    if "by_user" in s[timeframe]["archive"]:
                        for user_name in s[timeframe]["archive"]["by_user"].keys():
                            user_names.add(user_name)
                            if user_name not in stats["by_user"]:
                                stats["by_user"][user_name] = dict()
                                stats["by_user"][user_name]["archive"] = dict()
                                stats["by_user"][user_name]["retrieve"] = dict()

                            if timeframe not in stats["by_user"][user_name]["archive"]:
                                stats["by_user"][user_name]["archive"][timeframe] = dict()
                                for rv in relevant_fields:
                                    stats["by_user"][user_name]["archive"][timeframe][rv] = defaultdict(int)

                        for user_name, values in s[timeframe]["archive"]["by_user"].items():
                            for rv in relevant_fields:
                                if rv in values:
                                    stats["by_user"][user_name]["archive"][timeframe][rv]["sum"] += values[rv]["sum"]
                                    stats["by_user"][user_name]["archive"][timeframe][rv]["count"] += values[rv]["count"]



                    
                if "retrieve" in s[timeframe]:
                    retrieve_totals = s[timeframe]["retrieve"]["totals"]
                    
                    relevant_fields = ["bytes", "bytes_offline", "bytes_online", "disk_files", "fields", "fields_offline", "fields_online", "tape_files", "written"]
                    if timeframe not in stats["totals"]["retrieve"]:
                        stats["totals"]["retrieve"][timeframe] = dict()
                        for rv in relevant_fields:
                            stats["totals"]["retrieve"][timeframe][rv] = defaultdict(int)

                    for rv in relevant_fields:
                        if rv in retrieve_totals:
                            stats["totals"]["retrieve"][timeframe][rv]["sum"] += retrieve_totals[rv]["sum"]
                            stats["totals"]["retrieve"][timeframe][rv]["count"] += retrieve_totals[rv]["count"]

                    # BY DATABASE
                    relevant_fields = ["bytes", "fields"]

                    if "by_database" in s[timeframe]["retrieve"]:
                        for db_name in s[timeframe]["retrieve"]["by_database"].keys():
                            db_names.add(db_name)
                            if db_name not in stats["by_database"]:
                                stats["by_database"][db_name] = dict()
                                stats["by_database"][db_name]["archive"] = dict()
                                stats["by_database"][db_name]["retrieve"] = dict()

                            if timeframe not in stats["by_database"][db_name]["retrieve"]:
                                stats["by_database"][db_name]["retrieve"][timeframe] = dict()
                                for rv in relevant_fields:
                                    stats["by_database"][db_name]["retrieve"][timeframe][rv] = defaultdict(int)

                        for db_name, values in s[timeframe]["retrieve"]["by_database"].items():
                            for rv in relevant_fields:
                                if rv in values:
                                    stats["by_database"][db_name]["retrieve"][timeframe][rv]["sum"] += values[rv]["sum"]
                                    stats["by_database"][db_name]["retrieve"][timeframe][rv]["count"] += values[rv]["count"]

                    # BY USER
                    relevant_fields = ["bytes", "fields"]

                    if "by_user" in s[timeframe]["retrieve"]:
                        for user_name in s[timeframe]["retrieve"]["by_user"].keys():
                            user_names.add(user_name)
                            if user_name not in stats["by_user"]:
                                stats["by_user"][user_name] = dict()
                                stats["by_user"][user_name]["archive"] = dict()
                                stats["by_user"][user_name]["retrieve"] = dict()

                            if timeframe not in stats["by_user"][user_name]["retrieve"]:
                                stats["by_user"][user_name]["retrieve"][timeframe] = dict()
                                for rv in relevant_fields:
                                    stats["by_user"][user_name]["retrieve"][timeframe][rv] = defaultdict(int)

                        for user_name, values in s[timeframe]["retrieve"]["by_user"].items():
                            for rv in relevant_fields:
                                if rv in values:
                                    stats["by_user"][user_name]["retrieve"][timeframe][rv]["sum"] += values[rv]["sum"]
                                    stats["by_user"][user_name]["retrieve"][timeframe][rv]["count"] += values[rv]["count"]


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("usage: %s source_dir target.stats.json" % sys.argv[0])
        sys.exit(1)


    source_dir = sys.argv[1]
    target_file = sys.argv[2]
    
    source_files = glob.glob(os.path.join(source_dir, "*stats.json"))
    cnt = 1
    for filename in source_files:
        source_file = os.path.join(source_dir, filename)
        print("%d / %d" % (cnt, len(source_files)))
        try:
            read_stats(source_file)
        except Exception as e:
            print("Error in %s" % source_file)
            print (e)
            traceback.print_exc()

        cnt += 1

    with open(target_file, 'w') as out:
        json.dump(stats, out, indent=4, sort_keys=True)
    print("user_names: %d" % (len(user_names)))
    print("db_names: %d" % (len(db_names)))
    print ("wrote summary to %s" % target_file)

    for tf in sorted(stats["totals"]["archive"].keys()):
        print(tf)