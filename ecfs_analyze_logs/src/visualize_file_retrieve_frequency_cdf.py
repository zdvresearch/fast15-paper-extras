#!/usr/bin/env python


import matplotlib
matplotlib.use('Agg')

import matplotlib.pyplot as plt

import numpy as np

import gzip
import sys
import time
import os
import resource
from collections import defaultdict



MONITOR_LINES = 100000

class Timer():
    def __init__(self, s):
        self.s = s

    def __enter__(self):
        self.start = time.time()

    def __exit__(self, *args):
        print("%s: %fs" % (self.s, (time.time() - self.start)))

line_style_per_group = dict()
line_style_per_group["Tiny"] = "-.o"
line_style_per_group["Small"] = "--D"
line_style_per_group["Medium"] = ":v"
line_style_per_group["Large"] = "-s"
line_style_per_group["Huge"] = "-.p"
line_style_per_group["Enormous"] = ":+"
line_style_per_group["Total"] = "-x"

legend_name = dict()
legend_name["Tiny"] = "Tiny (0 - 512KB)"
legend_name["Small"] = "Small (512KB - 1MB)"
legend_name["Medium"] = "Medium  (1MB - 8MB)"
legend_name["Large"] = "Large (8MB - 48MB)"
legend_name["Huge"] = "Huge (48MB - 1GB)"
legend_name["Enormous"] = "Enormous (1GB - $\infty$)"
legend_name["Total"] = "All groups combined"

group_names = ["Tiny", "Small", "Medium", "Large", "Huge", "Enormous", "Total"]

def visualize(source_file, target_dir):

    # preparation
    foo = dict()
    for group_name in group_names:
        foo[group_name] = dict()
        for filter in ["cdf_all", "cdf_no_tmp", "cdf_no_put", "cdf_no_put_no_tmp"]:
            foo[group_name][filter] = defaultdict(int)

    with Timer("reading file"):
        with gzip.open(source_file, 'rb') as sf:
            plines = 0
            t = time.time()
            for line in sf:
                plines += 1
                if plines % MONITOR_LINES == 0:
                    print ("processed lines: %d  mem: %rMB, lines/s: %r" %
                     (plines,
                      float(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss) / 1024,
                      int(MONITOR_LINES / (time.time() - t))
                     )
                    )
                    t = time.time()

                # if plines == 500000:
                #     break

                elems = line.decode().split('|')
                tags = elems[0].split(';')
                group_name = tags[0]

                if group_name == "Unknown":
                    continue

                if 'tmp' in tags:
                    is_tmp = True
                if 'no_put' in tags:
                    no_put = True

                access_times = [int(x) for x in elems[1].split(';')]
                gets = len(access_times)

                for g in [group_name, "Total"]:
                    foo[g]["cdf_all"][gets] += 1
                    if not 'tmp' in tags:
                        foo[g]["cdf_no_tmp"][gets] += 1

                    if not 'no_puts' in tags:
                        foo[g]["cdf_no_put"][gets] += 1

                    if not 'tmp' in tags and not 'no_puts' in tags:
                        foo[g]["cdf_no_put_no_tmp"][gets] += 1

                # print(group_name, access_times)
                # access_times = [int(x) for x in line.split(';')]
                # foo[len(access_times)].append(access_times)

    # draw_cdfs_A(foo, target_dir)
    plot_cdf_over_access_frequencies(foo, target_dir)


def plot_cdf_over_access_frequencies(foo, target_dir):
    with Timer("visualize2"):
        for zoomed in [True, False]:
            for filter in ["cdf_all", "cdf_no_tmp", "cdf_no_put", "cdf_no_put_no_tmp"]:
                fig, ax = plt.subplots()

                for group_name in group_names:
                    cdf_dict = foo[group_name]
                    x_vals = []
                    y_vals = []

                    x_tmp = 0
                    y_tmp = 0

                    total_accesses = 0
                    for k,v in cdf_dict[filter].items():
                        total_accesses += k * v
                    total_files = sum(cdf_dict[filter].values())

                    for k in sorted(cdf_dict[filter].keys(), reverse=True):
                    # k == number of gets, x[k] = number of files that have k gets

                        x_tmp += float(100) / total_files * cdf_dict[filter][k]
                        x_vals.append(x_tmp)

                        y_tmp += float(100) / total_accesses * (k * cdf_dict[filter][k])
                        y_vals.append(y_tmp)

                    plt.plot(x_vals, y_vals, line_style_per_group[group_name], label=legend_name[group_name])

                
                plt.grid(True)
            
                ax.set_ylabel('Summed up requests per group in %', fontsize=18)
                ax.set_xlabel('Fraction of retrieved files per group in %. Sorted by access frequency.', fontsize=18)

                if zoomed:
                    ax.set_xlim(0, 3)
                    ax.set_ylim(0, 55)
                else:
                    ax.set_xlim(0, 100)
                    ax.set_ylim(0, 100)
                ax.tick_params(labelsize=18)
                ax.legend(fancybox=True, shadow=False, loc="lower right")
                
                sizes = fig.get_size_inches()
                fig.set_size_inches(sizes[0]*1.5, sizes[1])


                plt.tight_layout()
                if zoomed:
                    pltname = "cdf_B_%s_zoomed.pdf" % (filter)
                else:
                    pltname = "cdf_B_%s.pdf" % (filter)
                tf = os.path.join(target_dir, pltname)
                plt.savefig(tf)
                print("saved %s" % (tf))
                plt.close()

if __name__ == "__main__":

    print("version 003")

    if len(sys.argv) == 1:
        print ("usage: ecfs_access_times_since_create.lines.txt.gz /target/graphs/dir")
        sys.exit(1)

    source_file = os.path.abspath(sys.argv[1])

    if not os.path.exists(source_file):
        print("target file: %s does not exist" % source_file)
        sys.exit(1)

    target_dir = os.path.abspath(sys.argv[2])
    visualize(source_file, target_dir)

