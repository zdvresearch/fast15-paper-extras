__author__ = 'maesker'

import datetime, calendar
import sys
import os
import time
import json
import numpy as np
import gzip

import matplotlib.pyplot as plt

def drawcdf(pltdata,x, figpath, xlable, ylable, title):
    fig = plt.figure()
    plt.ylabel(ylable)
    plt.xlabel(xlable)
    plt.title(title)
    plt.yscale('log')
    ind = np.arange(len(pltdata))
    plt.plot(x, pltdata, 'b-')
    plt.grid(True)
    fig.autofmt_xdate()
    plt.savefig(figpath)
    plt.show()
    plt.close()


def draw1(pltdata,x, figpath, xlable, ylable, title):
    fig = plt.figure()

    plt.ylabel(ylable)
   # plt.xlabel(xlable)
    plt.title(title)
    ind = np.arange(len(pltdata))
    plt.plot_date(x, pltdata, 'b-')
    plt.grid(True)
    fig.autofmt_xdate()
    plt.savefig(figpath)
    plt.show()
    plt.close()

def draw2(pltdata,x, figpath, xlable, ylable, title):
    fig = plt.figure()

    ind = np.arange(len(pltdata))
    plt.plot_date(x, pltdata[0], 'b-')
    plt.plot_date(x, pltdata[1], 'g-')
    plt.grid(True)
    plt.ylabel(ylable)
    #plt.xlabel(xlable)
    plt.title(title)
    fig.autofmt_xdate()
    plt.savefig(figpath)
    plt.show()

    plt.close()


def plot_1():
    print "mounts per day"
    file = os.path.join(os.getcwd(), 'taperesults.json')
    with open(file, 'r') as f:
        tmt,vol = [],[]
        x,y1,y2 = [],[],[]
        tmp = json.load(f)

        for ts in sorted(tmp['per_day'].keys()):
            #print ts
            tmpts = datetime.datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
        #    if cnt%7:
            x.append(tmpts)
            y1.append(tmp['per_day'][ts]['tm'])
            #y2.append(tmp['per_day'][ts]['tmt'])
            tmt.append(tmp['per_day'][ts]['tmt']/3600)
            vol.append(tmp['per_day'][ts]['vol_ml']/3600)

        draw1(y1, x, 'mounts_perday.pdf', "", "Number of loads", "Number of tape loads per day")

        draw2([tmt,vol], x, 'mounttime_per_day.pdf', "", 'Length in hours', "Physical and logical tape load time")


def plot2():
    percentiles = [0.01, 0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6,0.7,0.8,0.9,0.95,0.99]
    latency =     [9 ,    11,  12,  15,   18,  24,  28,  31, 34, 41, 60, 98, 216]

    drawcdf(latency,percentiles, 'latency_cdf.pdf' , 'percentile', 'Latency in seconds (log scale)', "Latency between Un-/load request and completion (CDF)")

    percentiles = [0.01, 0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6,0.7,0.8,0.9, 0.95, 0.99]
    tbm = [0.1, 0.1, 0.1, 49, 154, 332, 635, 1255, 2698, 7360, 40150, 194064, 1552996]
    drawcdf(tbm,percentiles, 'remount_cdf.pdf' , 'percentile', 'Time in seconds (log scale)', "Time between tape unload and (re-)load request (CDF)")





def plot_1():
    print "mounts per day"
    file = os.path.join(os.getcwd(), 'taperesults.json')
    with open(file, 'r') as f:
        tmt,vol = [],[]
        x,y1,y2 = [],[],[]
        tmp = json.load(f)

        for ts in sorted(tmp['per_day'].keys()):
            #print ts
            tmpts = datetime.datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
        #    if cnt%7:
            x.append(tmpts)
            y1.append(tmp['per_day'][ts]['tm'])
            #y2.append(tmp['per_day'][ts]['tmt'])
            tmt.append(tmp['per_day'][ts]['tmt']/3600)
            vol.append(tmp['per_day'][ts]['vol_ml']/3600)

        draw1(y1, x, 'mounts_perday.pdf', "", "Number of loads", "Number of tape loads per day")

        draw2([tmt,vol], x, 'mounttime_per_day.pdf', "", 'Length in hours', "Physical and logical tape load time")



if __name__=='__main__':
        #plot_1()
        #plot2()
        stats3()