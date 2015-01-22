#!/usr/bin/env python2

"""analyze.py: Reads and analyzes ecfs metadata db dump."""
__author__ = "Matthias Grawinkel"
__status__ = "Production"


import matplotlib
matplotlib.use('Agg')

from matplotlib import pyplot
from matplotlib import dates

import numpy as np

import sys
import os
import gzip
import time
import calendar
import datetime
import resource
import re
import math
import json

from collections import defaultdict
from collections import Counter

import hashlib

from file_size_groups import *

MONITOR_LINES=100000

# used for cdf plot
space_per_uid = Counter()
files_per_uid = Counter()


# used for file size histogram
num_files_per_size_category = Counter()
total_size_per_size_category = Counter()

# file_size_counter = Counter()

files_per_dir_counter = Counter()

stats = defaultdict(int)

# how many .grib files are there?
file_extension_cnt = Counter()
file_extension_size = Counter()

created_capacity_cnt = defaultdict(int)
created_capacity_size = defaultdict(int)

modified_capacity_cnt = defaultdict(int)
modified_capacity_size = defaultdict(int)


# this one will get HUUUUUGE
file_map = dict()

def prettyfy(number):
    d = float(number)
    if d - int(d) > 0:
        return '{:,.3f}'.format(d)
    return '{:,d}'.format(int(d))

def to_gigabyte(val):
    return (float(val) / 1024 / 1024 / 1024)

def to_terabyte(val):
    return (float(val) / 1024 / 1024 / 1024 / 1024)

def to_petabyte(val):
    return (float(val) / 1024 / 1024 / 1024 / 1024 / 1024)

def to_millions(val):
    return (float(val) / 1000 / 1000)

def to_billions(val):
    return (float(val) / 1000 / 1000 / 1000)

class Timer():
    def __init__(self, s):
        self.s = s

    def __enter__(self):
        self.start = time.time()

    def __exit__(self, *args):
        print ("%s: %fs" % (self.s, (time.time() - self.start)))


def get_md5(s, hexdigest=False):
    # return s
    m = hashlib.md5()
    m.update(s.encode())
    if hexdigest:
        return m.hexdigest()
    else:
        return m.digest()



def days_between(ts1, ts2):
    """
     '20050620-004900',
     '19700101-000000',
     '20120615-075612',
    """
    if ts1 == '19700101-000000' or ts2 == '19700101-000000':
        return None

    t1 = datetime.datetime.strptime(ts1, "%Y%m%d-%H%M%S")
    t2 = datetime.datetime.strptime(ts2, "%Y%m%d-%H%M%S")
    return (t1 - t2).days


def load(source_file, max_lines=None,log_start="20140905-000000"):

    object_re = re.compile("^\s*([\d]+)\s*([\d]+)\s*([\d]+)\s*([\d]+)\s*([\d]+-[\d]+)\s*([\d]+-[\d]+)\s*([\d]+-[\d]+)\s*([a-zA-Z0-9/.\-]+)\s*x'([[a-zA-Z0-9/.\-]+)'\s*$")


    with gzip.open(source_file, 'rt') as source:
        t = time.time()
        plines = 0
        for line in source:
            plines += 1
            if plines % MONITOR_LINES == 0:
                print ("processed lines: %d, mem: %rMB, lines/s: %r: found files: %d" %
                 (plines,
                  float(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss) / 1024 / 1024,
                  int(MONITOR_LINES / (time.time() - t)),
                  len(file_map)
                 )
                )
                t = time.time()

            if max_lines:
                if plines >= max_lines:
                    break

            m = object_re.match(line)

            if m:
                r = m.groups()

                size = int(r[0])
                uid = int(r[1])
                gid = int(r[2])
                cos = int(r[3])  # cosid is Class-of-Service in HPSS
                creation_time = str(r[4])
                read_time = str(r[5])
                mod_time = str(r[6])
                path = str(r[7])
                magic = str(r[8])   # $bitfileid,$objectid;?

                stats["total_files"] += 1
                stats["total_size"] += size
                stats["max_file_size"] = max(stats["max_file_size"], size)

                # for cdf
                files_per_uid[uid] += 1
                space_per_uid[uid] += size

                # for file size histogram
                g = get_file_size_group(size)
                num_files_per_size_category[g] += 1
                total_size_per_size_category[g] += size

                # can be used for average file sizes et al.
                # file_size_counter[size] += 1

                #file name
                file_name = os.path.basename(path)

                if file_name.__contains__('.'):
                    extension = file_name[file_name.rfind('.'):]
                else:
                    extension = "unknown"
                
                file_extension_cnt[extension] += 1
                file_extension_size[extension] += size

                # files per dir
                dir_id = get_md5(os.path.dirname(path))
                files_per_dir_counter[dir_id] += 1

                # access times
                file_age_days = days_between(log_start, creation_time)

                created_capacity_cnt[file_age_days] += 1
                created_capacity_size[file_age_days] += size
                
                last_read_days = days_between(log_start, read_time)
                if not last_read_days:
                    stats["unread_files_cnt"] += 1
                    stats["unread_files_size"] += size
                
                last_modified_days = days_between(log_start, mod_time)
                modified_capacity_cnt[file_age_days] += 1
                modified_capacity_size[file_age_days] += size

                file_db_entry = (size, file_age_days, last_read_days, last_modified_days)

                # this will get large, but is required for the more interesting plots.
                file_map[get_md5(path)] = file_db_entry

                # how many days ago did a directory see its last upload / download / change?
                # how many files exist that have not been accessed since X days / never been read at all?
                # file_age to capacity?

def generate_x_labels(log_start):
    
    x_vals = list()
    deltas = list()
    day_0 = datetime.datetime.strptime(log_start, "%Y%m%d-%H%M%S")

    deltas.append(0)
    x_vals.append(day_0)
    
    d = day_0.replace(day=1)
    for i in range(80):
        delta = (day_0 - d).days
        deltas.append(delta)
        # print (d, delta)
        d -= datetime.timedelta(days=10)
        d = d.replace(day=1)
        x_vals.append(d)

    return x_vals, deltas

def unaccessed_files_plot(target_graph_dir, log_start, source_type):

    print(str(datetime.datetime.now()), "start to prepare unaccessed_files_plot")
    
    # these metrics will be plotted.
    unaccessed_files_cnt = Counter()
    unmodified_files_cnt = Counter()
    unread_files_cnt = Counter()
    total_created_files = Counter()
    existing_never_read_files = Counter()

    # index to filemap contents
    CREATION_DAYS = 1
    READ_DAYS = 2
    MODIFIED_DAYS = 3

    x_val_dates, deltas = generate_x_labels(log_start)
    x_val_dates = dates.date2num(x_val_dates)

    for days in deltas:
        # print (days)

        for f in file_map.itervalues():
            # print (f)
            if f[CREATION_DAYS] > days:
                total_created_files[days] += 1
                # make sure the file actually exists

                if f[READ_DAYS] and f[MODIFIED_DAYS]:
                    if days < min(f[READ_DAYS], f[MODIFIED_DAYS]):
                        unaccessed_files_cnt[days] += 1

                    if days < f[MODIFIED_DAYS]:
                        unmodified_files_cnt[days] += 1

                    if days < f[READ_DAYS]:
                        unread_files_cnt[days] += 1

                else:
                    existing_never_read_files[days] += 1
                    # some files have no f[2] (read_time) value
                    if days < f[MODIFIED_DAYS]:
                        unaccessed_files_cnt[days] += 1
                        unmodified_files_cnt[days] += 1
    # print(json.dumps(unaccessed_files_cnt, sort_keys=True, indent=2))
    print(str(datetime.datetime.now()), "plotting unaccessed_files_plot")
    
    y_vals = defaultdict(list)
    
    ax_files = [(total_created_files, "Existing files", 'k', ':o'),
                     (unaccessed_files_cnt, "Unaccessed files", 'r', '-s'),
                     (unmodified_files_cnt,"Unmodified files", 'g', '--*'), 
                     (unread_files_cnt, "Unread files", 'b', '-.+'),
                     (existing_never_read_files, "Existing never read files", 'k', ':x')
                    ]

    for p in ax_files:
        for key in deltas:
            y_vals[p[1]].append(to_millions(p[0][key]))
  

    fig, ax = pyplot.subplots()
    pyplot.xticks(rotation=90)
    pyplot.tick_params(labelsize=16)
    
    for p in ax_files:
        key_name = p[1]
        col = p[2]
        linestyle = p[3]

        ax.plot(x_val_dates, y_vals[key_name], linestyle, linewidth=2, color=col, label=key_name)

    if source_type == "ECFS":
        ax.legend(loc="upper left")
    else:
        ax.legend(loc="center left")


    ylabela = ax.set_ylabel('Existing files in mil', fontsize=24)
    
    #============ DATE FORMATTING =================
    ax.xaxis.set_major_locator(dates.MonthLocator(bymonth=[1,4,7,10]))
    ax.xaxis.set_minor_locator(dates.MonthLocator(interval=1))
    ax.xaxis.set_major_formatter(dates.DateFormatter('%Y/%m'))
    
    #============ /DATE FORMATTING =================

    ax.yaxis.grid(True)
    ax.xaxis.grid(True)
  
    sizes = fig.get_size_inches()
    fig.set_size_inches(sizes[0]*1.5, sizes[1])

    pyplot.tight_layout()
    outfile = os.path.join(target_graph_dir, "unaccessed_files_plot.pdf")
    pyplot.savefig(outfile, bbox_extra_artists=[ylabela], bbox_inches='tight')
    print("saved %s" % (outfile))
    pyplot.close()


 ###########################################
   # print(json.dumps(unaccessed_files_cnt, sort_keys=True, indent=2))
    print(str(datetime.datetime.now()), "plotting unaccessed_files_plot_2")
    
    # x_vals = defaultdict(list)
    y_vals = defaultdict(list)
    # right_y_vals = defaultdict(list)

    ax_files = [(unaccessed_files_cnt, "Unaccessed files", 'r', '-s'),
                        (unmodified_files_cnt,"Unmodified files", 'g', '--*'), 
                        (unread_files_cnt, "Unread files", 'b', '-.+'),
                        (existing_never_read_files, "Existing never read files", 'k', ':x')
                    ]

    for p in ax_files:
        # relative fraction to created files
        # y_temp = 0
        for key in deltas:
            # print ("x:%d, y:%d" % (key.p))
            # x_vals[p[1]].append(key)
            frac = float(p[0][key]) / float(total_created_files[key]) * 100
            y_vals[p[1]].append(frac)

    fig, ax = pyplot.subplots()
    pyplot.xticks(rotation=90)
    pyplot.tick_params(labelsize=16)
    
    for p in ax_files:
        key_name = p[1]
        col = p[2]
        linestyle = p[3]

        ax.plot(x_val_dates, y_vals[key_name], linestyle, linewidth=2, color=col, label=key_name)

    if source_type == "ECFS":
        ax.legend(loc="lower right") 
    else:
        #MARS
        ax.legend(loc="center left") 

    ylabela = ax.set_ylabel('Percent of existing files', fontsize=24)
        
    #============ DATE FORMATTING =================
    ax.xaxis.set_major_locator(dates.MonthLocator(bymonth=[1,4,7,10]))
    ax.xaxis.set_minor_locator(dates.MonthLocator(interval=1))
    ax.xaxis.set_major_formatter(dates.DateFormatter('%Y/%m'))
    
    ax.yaxis.grid(True)
    ax.xaxis.grid(True)
  
    sizes = fig.get_size_inches()
    fig.set_size_inches(sizes[0]*1.5, sizes[1])

    pyplot.tight_layout()
    outfile = os.path.join(target_graph_dir, "unaccessed_files_plot_2.pdf")
    pyplot.savefig(outfile, bbox_extra_artists=[ylabela], bbox_inches='tight')
    print("saved %s" % (outfile))
    pyplot.close()


def cdf_file_sizes_capacity(outfile):

    x_vals = np.arange(1, len(total_size_per_size_category) + 1)

    y_temp = 0
    y_vals = list()
    for v in [to_petabyte(total_size_per_size_category[x]) for x in sorted(total_size_per_size_category.iterkeys())]:
        y_temp += v
        y_vals.append(y_temp)
    
    fig, ax = pyplot.subplots()
    
    
    ax.plot(x_vals, y_vals, "-", linewidth=2, color='b')

    ax.set_ylabel('Total Capcity in PB', fontsize=20)
    
    labels = [get_group_name(x) for x in sorted(total_size_per_size_category.iterkeys())]
    ax.set_xticks(x_vals)
    ax.set_xticklabels(labels, rotation=90)
    ax.tick_params(labelsize=14)
    ax.set_xlim(1,len(total_size_per_size_category)+1)
    ax.yaxis.grid(True)
    ax.xaxis.grid(True)
  
    sizes = fig.get_size_inches()
    fig.set_size_inches(sizes[0]*1.3, sizes[1])

    pyplot.tight_layout()
    pyplot.savefig(outfile)
    print("saved %s" % (outfile))
    pyplot.close()

def histrogram_of_file_sizes(target_file):
    fig, ax = pyplot.subplots()
    # print (data)
    # num_files_per_size_category
    
    x_pos = np.arange(1, len(num_files_per_size_category) + 1)

    y_vals = [to_millions(num_files_per_size_category[x]) for x in sorted(num_files_per_size_category.iterkeys())]

    pyplot.bar(x_pos, y_vals, width=0.8, align='center', color="0.5")

    labels = [get_group_name(x) for x in sorted(num_files_per_size_category.iterkeys())]

    # a = ax.get_xticks().tolist()
    # print(a)

    ax.set_xticks(x_pos)
    ax.set_xticklabels(labels, rotation=90)
    ax.tick_params(labelsize=14)
    ax.set_ylabel('Number of files in mil', fontsize=24)
    # ax.set_xlabel('File size group', fontsize=24)

   # We change the fontsize of minor ticks label 
    ax.yaxis.grid(True)
    # ax2.yaxis.grid(True)
    # ax.xaxis.grid(True)

    sizes = fig.get_size_inches()
    fig.set_size_inches(sizes[0]*1.2, sizes[1])

    pyplot.tight_layout()
    
    pyplot.savefig(target_file)
    print("saved %s" % (target_file))
    pyplot.close()




def stats_to_latex_table(target_file, source_type="ECFS"):
    with open(target_file, 'wt') as tf:
        tf.write("\\begin{table}[ht!]\n")
        tf.write("\\scriptsize\n")
        tf.write("\\centering\n")
        tf.write("{\n")
        tf.write("\\begin{tabular}{|r|r|}\n")
        tf.write("\\hline\n")
        tf.write("\\multicolumn{2}{|c|}{\\textbf{File system stats}}\\\\\\hline\n")
        tf.write("Total \#files & %s\\\\\\hline\n" % (prettyfy(stats["total_files"])))
        tf.write("Total used capacity & %s\\,PB\\\\\\hline\n" % (prettyfy(to_petabyte(stats["total_size"]))))
        tf.write("Max file size & %s\\,GB\\\\\\hline\n" % (prettyfy(to_gigabyte(stats["max_file_size"]))))
        tf.write("\#Directories& %s\\\\\\hline\n" % (prettyfy(len(files_per_dir_counter))))
        
        max_files_per_dir = files_per_dir_counter.most_common(1)[0][1]
        tf.write("Max files per directory & %s\\\\\\hline\n" % (prettyfy(max_files_per_dir)))

        tf.write("\#Files never read & %s\\\\\\hline\n" % (prettyfy(stats["unread_files_cnt"])))
        tf.write("Capacity of never read files & %s\\,PB\\\\\\hline\n" % (prettyfy(to_petabyte(stats["unread_files_size"]))))
        tf.write("\\hline\n")
        
        if source_type == "ECFS":
        # most common file extensions, and how much space they take
        
            tf.write("\\multicolumn{2}{|c|}{\\textbf{Most common file types}}\\\\\\hline\n")
            tf.write("\\textbf{by file count} & \\textbf{by used capacity}\\\\\\hline\n")
            
            by_count = file_extension_cnt.most_common(10)
            by_size = file_extension_size.most_common(10)

            for i in range(min(len(by_count), len(by_size))):
                tf.write("%s (%.4f%s) & %s (%.4f%s)\\\\\\hline\n" % (
                    by_count[i][0],
                    float(by_count[i][1]) / stats["total_files"] * 100,
                    "\%",
                    by_size[i][0],
                    float(by_size[i][1]) / stats["total_size"] * 100,
                    "\%"
                    ))
            tf.write("\\end{tabular}\n")
            tf.write("}\n")

       
            tf.write("\\caption{ECFS file system statistics}\n")
            tf.write("\\label{table:ecfs_db_file_types_statistics}\n")
        else:
            tf.write("\\caption{MARS file system statistics}\n")
            tf.write("\\label{table:mars_db_file_types_statistics}\n")
        tf.write("\\end{table}\n")

    print("wrote: %s" % (target_file))

def store_stats(stats_file):

    # print(json.dumps(stats, indent=2, sort_keys=True))
    # print(json.dumps(num_files_per_size_category, indent=2, sort_keys=True))
    # print(json.dumps(file_extension_cnt, indent=2, sort_keys=True))
    # print(json.dumps(file_extension_size, indent=2, sort_keys=True))
 
    s = dict()

    s["stats"] = stats
    s["num_files_per_size_category"] = num_files_per_size_category
    s["file_extension_cnt"] = file_extension_cnt
    s["file_extension_size"] = file_extension_size

    with open(stats_file, 'wt') as sf:
        json.dump(s, sf, indent=4, sort_keys=True)


if __name__ == "__main__":

    if len(sys.argv) != 4:
        print("usage: %s source_file target.gz" % sys.argv[0])
        sys.exit(1)

    source_file = os.path.abspath(sys.argv[1])
    target_graph_dir = os.path.abspath(sys.argv[2])
    source_type = sys.argv[3]


    print("source_file == %s" % (source_file))
    print("target_graph_dir == %s" % (target_graph_dir))
    print("source_type == %s" % (source_type))

    if not os.path.exists(source_file):
        print("source: %s does not exist" % source_file)
        sys.exit(1)

    if not os.path.exists(target_graph_dir):
        print("target_graph_dir: does not exist: %s" % target_graph_dir)
        sys.exit(1)

    with Timer("Loading data"):
        if source_type == "ECFS":
            print("loading ECFS")
            log_start="20140905-000000"
        else:
            print("loading MARS")
            log_start="20140904-000000"

        # load(source_file, max_lines=1000000, log_start=log_start)
        load(source_file, log_start=log_start)

    with Timer("Plot unaccessed Files"):
        unaccessed_files_plot(target_graph_dir, log_start, source_type)

    # with Timer("Plot unaccessed Files"):
    #     unaccessed_files_plot_2(os.path.join(target_graph_dir, "unaccessed_files_plot_2.pdf"), log_start, source_type)

    with Timer("Plot histrogram of File Sizes"):
        histrogram_of_file_sizes(os.path.join(target_graph_dir, "histogram_file_sizes.pdf"))
    
    with Timer("Plot CDF over File Sizes"):
        cdf_file_sizes_capacity(os.path.join(target_graph_dir, "cdf_over_file_sizes.pdf"))
    
    stats_to_latex_table(os.path.join(target_graph_dir, "db_stats.tex"), source_type)

    with Timer("Store stats"):
        store_stats(os.path.join(target_graph_dir, "stats.json"))
