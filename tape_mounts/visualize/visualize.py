#!/usr/bin/env python2

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


import numpy
import scipy
import scipy.stats

def percentile(N, P):
    """
    Find the percentile of a list of values

    @parameter N - A list of values.  N must be sorted.
    @parameter P - A float value from 0.0 to 1.0

    @return - The percentile of the values.
    """
    n = int(round(P * len(N) + 0.5))
    return N[n-1]

def mean_confidence_interval(data, confidence=0.95):
    a = 1.0 * numpy.array(data)
    n = len(a)
    m, se = numpy.mean(a), scipy.stats.sem(a)
    # calls the inverse CDF of the Student's t distribution
    h = se * scipy.stats.t._ppf((1 + confidence) / 2., n - 1)
    return m - h, m + h

def mean_confidence_interval_h(data, confidence=0.95):
    a = 1.0 * numpy.array(data)
    n = len(a)
    m, se = numpy.mean(a), scipy.stats.sem(a)
    # calls the inverse CDF of the Student's t distribution
    h = se * scipy.stats.t._ppf((1 + confidence) / 2., n - 1)
    return h


def mean(data, places=2):
    a = 1.0 * numpy.array(data)
    return round(numpy.mean(a), places)

def get_mean_string(data):
    m = mean(data)
    mm, mp = mean_confidence_interval(data)
    return "%.1f" % (round(m, 1))

def get_meanconf_string(data):
    m = mean(data)
    mm, mp = mean_confidence_interval(data)
    return "%s ($\pm$ %s)" % ('{:,.2f}'.format(m), '{:,.2f}'.format(m - mm, 1))


def print_stats(name, r):
    latencies = sorted([x for x in r["mount_request_latency"] if x > 0 and x < 10000])

    print("=--------------------")
    print(name)
    print("percentile(0.05)", percentile(latencies, 0.05))
    print("percentile(0.25)", percentile(latencies, 0.25))
    print("percentile(0.50)", percentile(latencies, 0.50))
    print("percentile(0.75)", percentile(latencies, 0.75))
    print("percentile(0.95)", percentile(latencies, 0.95))
    print("percentile(0.99)", percentile(latencies, 0.99))
    print("mean(latencies)", mean(latencies))
    print("mean_err(latencies)", get_meanconf_string(latencies))
    print("min", min(latencies))
    print("max", max(latencies))
    print("len", len(latencies))
    # , get_meanconf_string(latencies), )

def to_million(x):
    return float(x) / 1000 / 1000

def prettyfy(number):
    d = float(number)
    if d - int(d) > 0:
        return '{:,.2f}'.format(d)
    return '{:,d}'.format(int(d))

def dump(ecfs_r, mars_r):
    # print ("Number of ECFS tapes: ", len(ecfs_r))
    # print ("Number of MARS tapes: ", len(mars_r))

    # fraction: total tape loads to total reloads 60

    tape_reloads_60sek_otherdrive = list()
    volume_remounts_300sek = list()
    tape_reloads_300sek_samedrive = list()
    volume_remounts_60sek = list()
    volume_mounts = list()
    tape_reloads_300sek_otherdrive = list()
    tape_reloads_60sek = list()
    tape_loads = list()
    tape_reloads_300sek = list()
    tape_reloads_60sek_samedrive = list()

    x_vals = sorted(ecfs_r["perweekres"].keys())
    
    for r in [ecfs_r, mars_r]:
        for x in x_vals:        
            tape_reloads_60sek_otherdrive.append(r["perweekres"][x]["tape_reloads_60sek_otherdrive"])
            volume_remounts_300sek.append(r["perweekres"][x]["volume_remounts_300sek"])
            tape_reloads_300sek_samedrive.append(r["perweekres"][x]["tape_reloads_300sek_samedrive"])
            volume_remounts_60sek.append(r["perweekres"][x]["volume_remounts_60sek"])
            volume_mounts.append(r["perweekres"][x]["volume_mounts"])
            tape_reloads_300sek_otherdrive.append(r["perweekres"][x]["tape_reloads_300sek_otherdrive"])
            tape_reloads_60sek.append(r["perweekres"][x]["tape_reloads_60sek"])
            tape_loads.append(r["perweekres"][x]["tape_loads"])
            tape_reloads_300sek.append(r["perweekres"][x]["tape_reloads_300sek"])
            tape_reloads_60sek_samedrive.append(r["perweekres"][x]["tape_reloads_60sek_samedrive"])
            
        print ("tape_reloads_60sek", float(sum(tape_reloads_60sek)) / float(sum(tape_loads)) * 100)
        print ("tape_reloads_60sek_otherdrive", float(sum(tape_reloads_60sek_otherdrive)) / float(sum(tape_loads)) * 100)
        print ("tape_reloads_300sek", float(sum(tape_reloads_300sek)) / float(sum(tape_loads)) * 100)
        print ("tape_reloads_300sek_otherdrive", float(sum(tape_reloads_300sek_otherdrive)) / float(sum(tape_loads)) * 100)
        


    mounts_per_tape = sorted(mars_r["mounts_per_tape"])
    print("MARS & %s & %s & %s & %s & %s & %s & %s \\\\\\hline" % (
        prettyfy(len(mounts_per_tape)),
        prettyfy(percentile(mounts_per_tape, 0.05)),
        prettyfy(percentile(mounts_per_tape, 0.25)),
        prettyfy(percentile(mounts_per_tape, 0.50)),
        get_meanconf_string(mounts_per_tape),
        prettyfy(percentile(mounts_per_tape, 0.95)),
        prettyfy(percentile(mounts_per_tape, 0.99))
    ))

    mounts_per_tape = sorted(ecfs_r["mounts_per_tape"])
    print("ECFS & %s & %s & %s & %s & %s & %s & %s \\\\\\hline" % (
        prettyfy(len(mounts_per_tape)),
        prettyfy(percentile(mounts_per_tape, 0.05)),
        prettyfy(percentile(mounts_per_tape, 0.25)),
        prettyfy(percentile(mounts_per_tape, 0.50)),
        get_meanconf_string(mounts_per_tape),
        prettyfy(percentile(mounts_per_tape, 0.95)),
        prettyfy(percentile(mounts_per_tape, 0.99))
    ))


def to_latex(ecfs_r, mars_r):

    print("\n\n\n\n\n\n")

    print("\\begin{table}[ht!]")
    print("\\footnotesize")
    print("\\centering")
    print("{")
    print("\\begin{tabular}{|@{~}l@{~}|@{~}r@{~}|@{~}r@{~}|@{~}r@{~}|@{~}r@{~}|@{~}r@{~}|@{~}r@{~}|@{~}r@{~}|}")
    print("\\hline")
    print("\\textbf{System} & \\textbf{Mounts} & \\textbf{P05} & \\textbf{P25} & \\textbf{P50} & \\textbf{mean (+-95\\%)} & \\textbf{P95} & \\textbf{P99} \\\\\\hline")

    latencies = sorted([x for x in mars_r["mount_request_latency"] if x > 0 and x < 10000])
    print("MARS & %s & %s & %s & %s & %s & %s & %s \\\\\\hline" % (
        prettyfy(len(latencies)),
        prettyfy(percentile(latencies, 0.05)),
        prettyfy(percentile(latencies, 0.25)),
        prettyfy(percentile(latencies, 0.50)),
        get_meanconf_string(latencies),
        prettyfy(percentile(latencies, 0.95)),
        prettyfy(percentile(latencies, 0.99))
    ))

    latencies = sorted([x for x in ecfs_r["mount_request_latency"] if x > 0 and x < 10000])
    print("ECFS & %s & %s & %s & %s & %s & %s & %s \\\\\\hline" % (
        prettyfy(len(latencies)),
        prettyfy(percentile(latencies, 0.05)),
        prettyfy(percentile(latencies, 0.25)),
        prettyfy(percentile(latencies, 0.50)),
        get_meanconf_string(latencies),
        prettyfy(percentile(latencies, 0.95)),
        prettyfy(percentile(latencies, 0.99))
    ))
    
    print("\\end{tabular}")
    print("}")
    print("\\caption{Tape mount latencies in seconds.}")
    print("\\label{table:tape_latencies}")
    print("\\end{table}")

    print("\n\n\n\n\n\n")

def plot_mounts(name, r, target_dir):

    # for x in [int(i) for i in x_vals]:
        # print(datetime.datetime.utcfromtimestamp(x).strftime("%Y-%m-%d %H:%M:%S"))

    tape_loads = defaultdict(list)
    tape_reloads_60sek = defaultdict(list)
    tape_reloads_60sek_otherdrive = defaultdict(list)
    tape_reloads_300sek = defaultdict(list)
    tape_reloads_300sek_otherdrive = defaultdict(list)
    volume_mounts = defaultdict(list)

    x_vals = sorted(ecfs_r["perweekres"].keys())
    x_dates = set()

    for x in x_vals:

        k = datetime.datetime.utcfromtimestamp(int(x)).strftime("%Y-%m")
        tape_loads[k].append(r["perweekres"][x]["tape_loads"])
        tape_reloads_60sek[k].append(r["perweekres"][x]["tape_reloads_60sek"])
        tape_reloads_60sek_otherdrive[k].append(r["perweekres"][x]["tape_reloads_60sek_otherdrive"])
        tape_reloads_300sek[k].append(r["perweekres"][x]["tape_reloads_300sek"])
        tape_reloads_300sek_otherdrive[k].append(r["perweekres"][x]["tape_reloads_300sek_otherdrive"])
        volume_mounts[k].append(r["perweekres"][x]["volume_mounts"])
        
        x_dates.add(k)

    s_dates = sorted(list(x_dates))
    x_val_dates = dates.date2num([datetime.datetime.strptime(x, "%Y-%m") for x in s_dates])

    fig, ax1 = pyplot.subplots()
    # fig.suptitle('%s Tape Mount Details' % (name), fontsize=28)


# for the final plot use them:        
    # ax1.plot(x_val_dates, [sum(tape_loads[x]) for x in s_dates], "-s", linewidth=2, color='k', label="Tape loads", markersize=10)        
    # ax1.plot(x_val_dates, [sum(volume_mounts[x]) for x in s_dates], "-o", linewidth=2, color='k', label="Volume mounts", markersize=10)

    # ax1.plot(x_val_dates, [sum(tape_reloads_60sek[x]) for x in s_dates], ":+", linewidth=1, color='k', label="Tape Reloads 60s", markersize=10)
    # ax1.plot(x_val_dates, [sum(tape_reloads_60sek_otherdrive[x]) for x in s_dates], "--+", linewidth=1, color='k', label="Tape Reloads 60s Other Drive", markersize=10)
    
    # ax1.plot(x_val_dates, [sum(tape_reloads_300sek[x]) for x in s_dates], ":d", linewidth=1, color='k', label="Tape Reloads 300s", markersize=10)
    # ax1.plot(x_val_dates, [sum(tape_reloads_300sek_otherdrive[x]) for x in s_dates], "--d", linewidth=1, color='k', label="Tape Reloads 300s Other Drive", markersize=10)

# for the legend use these (smaller marker..)
    ax1.plot(x_val_dates, [sum(tape_loads[x]) for x in s_dates], "-s", linewidth=2, color='k', label="Tape loads")        
    ax1.plot(x_val_dates, [sum(volume_mounts[x]) for x in s_dates], "-o", linewidth=2, color='k', label="Volume mounts")

    ax1.plot(x_val_dates, [sum(tape_reloads_60sek[x]) for x in s_dates], ":+", linewidth=1, color='k', label="Tape Reloads 60s")
    ax1.plot(x_val_dates, [sum(tape_reloads_60sek_otherdrive[x]) for x in s_dates], "--+", linewidth=1, color='k', label="Tape Reloads 60s Other Drive")
    
    ax1.plot(x_val_dates, [sum(tape_reloads_300sek[x]) for x in s_dates], ":d", linewidth=1, color='k', label="Tape Reloads 300s")
    ax1.plot(x_val_dates, [sum(tape_reloads_300sek_otherdrive[x]) for x in s_dates], "--d", linewidth=1, color='k', label="Tape Reloads 300s Other Drive")



    # lgd = ax1.legend(bbox_to_anchor=(1.2, 1.1), ncol=2)

    ax1.set_ylabel('Number of requests', fontsize=28)

   #  #============ DATE FORMATTING =================

    ax1.xaxis.set_major_locator(dates.MonthLocator(bymonth=[1,4,7,10]))
    ax1.xaxis.set_minor_locator(dates.MonthLocator(interval=1))
    ax1.xaxis.set_major_formatter(dates.DateFormatter('%Y/%m'))
    ax1.tick_params(labelsize=16)
    ax1.set_xlim(x_val_dates[:1], x_val_dates[-1:])
    fig.autofmt_xdate()

   #  #============ /DATE FORMATTING =================

    
    
    ax1.yaxis.grid(True)
    ax1.xaxis.grid(True)

    sizes = fig.get_size_inches()
    fig.set_size_inches(sizes[0]*1.45, sizes[1])

    pyplot.tight_layout()
    
    handles,labels = ax1.get_legend_handles_labels()

    outfile = os.path.join(target_dir, "tape_mounts_%s.pdf" % (name))
    pyplot.savefig(outfile, bbox_inches='tight')
    print("saved %s" % (outfile))
    pyplot.close()

    pyplot.clf()

    fig, ax = pyplot.subplots()
    ax.legend(handles, labels, fancybox=True, shadow=False, ncol=2)
    ax.xaxis.set_visible(False)
    ax.yaxis.set_visible(False)
    pyplot.axis('off')

    sizes = fig.get_size_inches()
    fig.set_size_inches(sizes[0]* 0.1, sizes[1])
    pyplot.tight_layout()

    pyplot.savefig(os.path.join(target_dir, "legend.pdf"), bbox_inches='tight', pad_inches=0.1)
    pyplot.close()




def plot_mounts_per_tape_absolute(ecfs_r, mars_r, target_dir):
    counts_ecfs = sorted(ecfs_r["mounts_per_tape"], reverse=True)
    counts_mars = sorted(mars_r["mounts_per_tape"], reverse=True)
    # totals = sorted(mars_r["mounts_per_tape"] + ecfs_r["mounts_per_tape"], reverse=True)

    fig, ax = pyplot.subplots()

    ax.tick_params(axis='x', labelsize=22)
    ax.tick_params(axis='y', labelsize=22)

    linestyles = {
    "ECFS" : "--",
    "MARS" : "-",
    }

    for key, vals in [('ECFS', counts_ecfs), ('MARS', counts_mars)]:
        x_vals = []
        y_vals = []
        x = 0 
        t = 0
        for val in vals:
            x += 1
            x_vals.append(x)
            
            t += to_million(val)
            y_vals.append(t)
        ax.plot(x_vals, y_vals, linestyles[key], linewidth=2, color='k', label=key)        
   

    ax.legend(loc="lower right") 

    ax.set_ylabel('Total Tape Loads in mil', fontsize=28)
    ax.set_xlabel("Most accessed Tapes", fontsize=28)

    ax.yaxis.grid(True)
    ax.xaxis.grid(True)
  
    # sizes = fig.get_size_inches()
    # fig.set_size_inches(sizes[0]*0.5, sizes[1])

    pyplot.tight_layout()

    outfile = os.path.join(target_dir, "tape_mounts_freq_absolute.pdf")
    pyplot.savefig(outfile)
    print("saved %s" % (outfile))
    pyplot.close()

def plot_mounts_per_tape_normalized(ecfs_r, mars_r, target_dir):
    counts_ecfs = sorted(ecfs_r["mounts_per_tape"], reverse=True)
    counts_mars = sorted(mars_r["mounts_per_tape"], reverse=True)

    print ("Number of ECFS tapes: ", len(counts_ecfs))
    print ("Number of MARS tapes: ", len(counts_mars))

    ecfs_stretch_x = float(100) / len(counts_ecfs)
    ecfs_stretch_y = float(100) / sum(counts_ecfs)

    mars_stretch_x = float(100) / len(counts_mars)
    mars_stretch_y = float(100) / sum(counts_mars)

    fig, ax = pyplot.subplots()

    ax.tick_params(axis='x', labelsize=22)
    ax.tick_params(axis='y', labelsize=22)


    # ECFS 
    x_vals = []
    y_vals = []
    x = 0 
    t = 0
    for val in counts_ecfs:
        x += ecfs_stretch_x
        x_vals.append(x)
        
        t += ecfs_stretch_y * val
        y_vals.append(t)
    ax.plot(x_vals, y_vals, "--", linewidth=2, color='k', label="ECFS")

    # MARS
    x_vals = []
    y_vals = []
    x = 0 
    t = 0
    for val in counts_mars:
        x += mars_stretch_x
        x_vals.append(x)
        
        t += mars_stretch_y * val
        y_vals.append(t)
    ax.plot(x_vals, y_vals, "-", linewidth=2, color='k', label="MARS")

    
    ax.legend(loc="lower right") 

    ax.set_ylabel('Total Tape Loads in %', fontsize=28)
    ax.set_xlabel("Total Tapes in %", fontsize=28)
    ax.set_xlim(0,100)
    ax.set_ylim(0,100)
    ax.yaxis.grid(True)
    ax.xaxis.grid(True)
  
    # sizes = fig.get_size_inches()
    # fig.set_size_inches(sizes[0]*0.5, sizes[1])

    pyplot.tight_layout()

    outfile = os.path.join(target_dir, "tape_mounts_normalized.pdf")
    pyplot.savefig(outfile)
    print("saved %s" % (outfile))
    pyplot.close()


if __name__ == "__main__":

    if len(sys.argv) != 4:
        print("usage: %s ecfs_source_file mars_source_file target_dir" % sys.argv[0])
        sys.exit(1)

    ecfs_source_file = os.path.abspath(sys.argv[1])
    mars_source_file = os.path.abspath(sys.argv[2])
    target_dir = os.path.abspath(sys.argv[3])

    if not os.path.exists(ecfs_source_file):
        print("ecfs_source_file: %s does not exist" % ecfs_source_file)
        sys.exit(1)

    if not os.path.exists(mars_source_file):
        print("mars_source_file: %s does not exist" % mars_source_file)
        sys.exit(1)

    if not os.path.exists(target_dir):
        print("target_dir: does not exist: %s" % target_dir)
        sys.exit(1)

    with open(ecfs_source_file, 'r') as sf:
        ecfs_r = json.load(sf)

    with open(mars_source_file, 'r') as sf:
        mars_r = json.load(sf)
    
    

    plot_mounts_per_tape_absolute(ecfs_r, mars_r, target_dir)
    plot_mounts_per_tape_normalized(ecfs_r, mars_r, target_dir)
    plot_mounts("ecfs", ecfs_r, target_dir)
    plot_mounts("mars", mars_r, target_dir)

    to_latex(ecfs_r, mars_r)
    dump(ecfs_r, mars_r)
