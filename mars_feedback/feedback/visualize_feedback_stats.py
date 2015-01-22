#!/usr/bin/env python

__author__ = 'meatz'

import matplotlib
matplotlib.use('Agg')

from matplotlib import pyplot
from matplotlib import dates

import numpy as np

import gzip
import sys
import time
import os
import re
import glob
import json
import cPickle
import resource
import operator
import time
import calendar
import datetime
from collections import defaultdict

# add ecmwf_utils to python path
util_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
print (util_path)
sys.path.append(util_path)

from ecmwf_util import Statistics

def to_terabyte(val):
    return (float(val) / 1024 / 1024 / 1024 / 1024)

def to_petabyte(val):
    return (float(val) / 1024 / 1024 / 1024 / 1024 / 1024)

def to_millions(val):
    return (float(val) / 1000 / 1000)

def to_billions(val):
    return (float(val) / 1000 / 1000 / 1000)

def present_throughput(data, target_graphs_dir):
    total_archive_tbytes = defaultdict(int)
    total_archive_fields = defaultdict(int)
    total_retrieve_tbytes = defaultdict(int)
    total_retrieve_fields = defaultdict(int)
    total_retrieve_tbytes_online = defaultdict(int)
    total_retrieve_fields_online = defaultdict(int)
    total_retrieve_tbytes_offline = defaultdict(int)
    total_retrieve_fields_offline = defaultdict(int)

    for d, v in data["totals"]["archive"].items():
        total_archive_tbytes[d] = to_terabyte(v["bytes"]["sum"])
        total_archive_fields[d] = to_millions(v["fields"]["sum"])

    for d, v in data["totals"]["retrieve"].items():
        total_retrieve_tbytes[d] = to_terabyte(v["bytes"]["sum"])
        total_retrieve_fields[d] = to_millions(v["fields"]["sum"])
        total_retrieve_tbytes_online[d] = to_terabyte(v["bytes_online"]["sum"])
        total_retrieve_fields_online[d] = to_millions(v["fields_online"]["sum"])
        total_retrieve_tbytes_offline[d] = to_terabyte(v["bytes_offline"]["sum"])
        total_retrieve_fields_offline[d] = to_millions(v["fields_offline"]["sum"])


    x_vals = list()
    archive_tbytes_y_vals = list()
    retrieve_tbytes_y_vals = list()
    archive_fields_y_vals = list()
    retrieve_fields_y_vals = list()
    for key_date in sorted(total_archive_tbytes.keys()):
        x_vals.append(key_date)
        archive_tbytes_y_vals.append(total_archive_tbytes.get(key_date, 0))
        retrieve_tbytes_y_vals.append(total_retrieve_tbytes.get(key_date, 0))
        archive_fields_y_vals.append(total_archive_fields.get(key_date, 0))
        retrieve_fields_y_vals.append(total_retrieve_fields.get(key_date, 0))

    sma_days = 10
    archive_tbytes_y_vals = Statistics.simplemovingaverage(archive_tbytes_y_vals, sma_days)
    retrieve_tbytes_y_vals = Statistics.simplemovingaverage(retrieve_tbytes_y_vals, sma_days)
    archive_fields_y_vals = Statistics.simplemovingaverage(archive_fields_y_vals, sma_days)
    retrieve_fields_y_vals = Statistics.simplemovingaverage(retrieve_fields_y_vals, sma_days)

    fig, ax1 = pyplot.subplots()
    
    pyplot.xticks(rotation=70)
    
    time_format="%Y-%m-%d"
    x_val_dates = list()
    for d in x_vals:
        x_val_dates.append(datetime.datetime.strptime(d, time_format))
    x_val_dates = dates.date2num(x_val_dates)
    
    # x_index = np.arange(len(x_vals))
    l1 = ax1.plot(x_val_dates, archive_tbytes_y_vals, '-', linewidth=2, color="r", label="Archived TBytes")
    l2 = ax1.plot(x_val_dates, retrieve_tbytes_y_vals, '-', linewidth=2, color="b", label="Retrieved TBytes")

    ax2 = ax1.twinx()
    l3 = ax2.plot(x_val_dates, archive_fields_y_vals, '--', linewidth=1, color="r", label="#Archived Fields")
    l4 = ax2.plot(x_val_dates, retrieve_fields_y_vals, '--', linewidth=1, color="b", label="#Retrieved Fields")

    # pyplot.title('MARS total throughput', fontsize=24)
    ax1.set_ylabel('Daily throughput in TBytes', fontsize=24)
    ylabela = ax2.set_ylabel('Daily #Fields in m', fontsize=24)
    
    lns = l1 + l2 + l3 + l4
    labs = [l.get_label() for l in lns]
    ax2.legend(lns, labs, loc="upper left") 

    ax1.yaxis.grid(True)
    ax2.yaxis.grid(True)
    ax1.xaxis.grid(True)

    sizes = fig.get_size_inches()
    fig.set_size_inches(sizes[0]*1.3, sizes[1])

    #============ DATE FORMATTING =================
   
    ax1.xaxis.set_major_locator(dates.MonthLocator(bymonth=[1,4,7,10]))
    ax1.xaxis.set_minor_locator(dates.MonthLocator(interval=1))
    ax1.xaxis.set_major_formatter(dates.DateFormatter('%Y/%m'))
    ax1.tick_params(labelsize=16)
    ax1.set_xlim(x_val_dates[:1], x_val_dates[-1:])
    # fig.autofmt_xdate()

    #============ /DATE FORMATTING =================

    pyplot.tight_layout()
    target_file = os.path.join(target_graphs_dir, "feedback_throughput.pdf")
    pyplot.savefig(target_file, bbox_extra_artists=[ylabela], bbox_inches='tight')
    print("saved %s" % (target_file))
    pyplot.close()


def cdf_over_users(data, target_graphs_dir):
    archive_tbytes = defaultdict(int)
    retrieve_tbytes = defaultdict(int)
    archive_fields = defaultdict(int)
    retrieve_fields = defaultdict(int)
    
    vals = defaultdict(int)

    for user in data["by_user"].keys():
        for timeframe, v in data["by_user"][user]["archive"].items():
            if "sum" in v["bytes"]:
                archive_tbytes[user] += to_petabyte(v["bytes"]["sum"])
            if "sum" in v["fields"]:
                archive_fields[user] += to_billions(v["fields"]["sum"])
    
        for timeframe, v in data["by_user"][user]["retrieve"].items():
            if "sum" in v["bytes"]:
                retrieve_tbytes[user] += to_petabyte(v["bytes"]["sum"])
            if "sum" in v["fields"]:
                retrieve_fields[user] += to_billions(v["fields"]["sum"])
    
    archived_tbytes_vals = list()
    tmp = 0
    for y in sorted(archive_tbytes.values(), reverse=True):
        tmp += y
        archived_tbytes_vals.append(tmp)

    archived_fields_vals = list()
    tmp = 0
    for y in sorted(archive_fields.values(), reverse=True):
        tmp += y
        archived_fields_vals.append(tmp)

    retrieved_tbytes_vals = list()
    tmp = 0
    for y in sorted(retrieve_tbytes.values(), reverse=True):
        tmp += y
        retrieved_tbytes_vals.append(tmp)
    
    retrieved_fields_vals = list()
    tmp = 0
    for y in sorted(retrieve_fields.values(), reverse=True):
        tmp += y
        retrieved_fields_vals.append(tmp)

    print("archived_tbytes_vals", len(archived_tbytes_vals) , archived_tbytes_vals )
    print("archived_fields_vals", len(archived_fields_vals)) #, archived_fields_vals )
    print("retrieved_tbytes_vals", len(retrieved_tbytes_vals)) #, retrieved_tbytes_vals)
    print("retrieved_fields_vals", len(retrieved_fields_vals)) #, retrieved_fields_vals )

    fig, ax1 = pyplot.subplots()
    
    ax1.set_xscale('log')
    ax1.set_xlabel('User ids', fontsize=20)

    l1 = ax1.plot(np.arange(1, len(archived_tbytes_vals) + 1), archived_tbytes_vals, '-', linewidth=2, color="r", label="Archived PBytes")
    l2 = ax1.plot(np.arange(1, len(retrieved_tbytes_vals) + 1), retrieved_tbytes_vals, '-', linewidth=2, color="b", label="Retrieved PBytes")
    
    ax2 = ax1.twinx()
    ax2.set_xscale('log')

    l3 = ax2.plot(np.arange(1, len(archived_fields_vals) + 1), archived_fields_vals, '--', linewidth=1, color="r", label="#Archived Fields")
    l4 = ax2.plot(np.arange(1, len(retrieved_fields_vals) + 1), retrieved_fields_vals, '--', linewidth=1, color="b", label="#Retrieved Fields")
   
    lns = l1 + l2 + l3 + l4
    labs = [l.get_label() for l in lns]
    ax2.legend(lns, labs, loc="lower right") 

    ax1.set_ylabel('Accumulated PBytes', fontsize=20)
    ylabela = ax2.set_ylabel('Accumulated #Fields requested in bil', fontsize=20)

    ax1.yaxis.grid(True)
    ax1.xaxis.grid(True)
    ax2.yaxis.grid(True)
    
    ax1.tick_params(labelsize=16)
    ax2.tick_params(labelsize=16)

    sizes = fig.get_size_inches()
    fig.set_size_inches(sizes[0]*1.3, sizes[1])

    pyplot.tight_layout()
    outfile = os.path.join(target_graphs_dir, "cdf_traffic_per_user.pdf")
    pyplot.savefig(outfile, bbox_extra_artists=[ylabela], bbox_inches='tight')
    print("saved %s" % (outfile))
    pyplot.close()


    # for debugging, which is the user id that take the most actions?
    sorted_x = sorted(archive_tbytes.iteritems(), key=operator.itemgetter(1), reverse=True)
    print ("top archivers by bytes")
    for i in range(10):
        print ( "%s %.10f pb" %(sorted_x[i][0], sorted_x[i][1]))

    sorted_x = sorted(retrieve_tbytes.iteritems(), key=operator.itemgetter(1), reverse=True)
    print ("top retriever by bytes")
    for i in range(10):
        print ( "%s %.10f pb" %(sorted_x[i][0], sorted_x[i][1]))


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print ("usage: feedback_stats.json graphs_dir")
        print ("reads all the trace files and writes a pickled file with source access analysis.")
        sys.exit(1)

    source_json_file = sys.argv[1]
    target_graphs_dir = sys.argv[2]

    with open (source_json_file, 'r') as jf:
        print ("Loading: %s" % source_json_file)
        data = json.load(jf)

    print("Start plotting...")
    present_throughput(data, target_graphs_dir)
    cdf_over_users(data, target_graphs_dir)