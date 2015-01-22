__author__ = 'maesker'

from bisect import bisect_left, bisect_right
from collections import Counter
import json, os, string, re, csv, StringIO, sys, time, datetime, gzip, glob, calendar, math, copy
import threading, multiprocessing, Queue, gc
from multiprocessing.sharedctypes import Value, Array
from correlation import corrPearson
from StateMachine import FiniteStateMachine

import python_daemon

try:
    from HandyHelperTools.plotting import fast_plot
    import matplotlib.pyplot as plt
    import numpy
    #from TimeTools import get_epochs, Timer
except:
    pass


## multi processing callback functions

def _cb_gzipdmp(prefix, result):
        file = os.path.join("/tmp", "%s_%s.json"%(prefix,os.getpid()))
        print file
        with open(file, 'w') as f:
            json.dump(result, f, indent=1)

def _cb_gzipload(file):
    res = None
    with open(file, 'r') as f:
        res = json.load(f)
    return res


def calc_correlation(inque, correlated, getcrtcb, prefix, interval_secs):
    success = {}
    for i in sorted(interval_secs):
        success[i] = []

    tmptotale, tmptotals = 0,0
    while True:
        try:
            cid = inque.get(True,10)
        except Queue.Empty:
            break

        procs = []
        crt = getcrtcb(cid, False)
        if crt:
            #print 'running cid', cid
            s,e = 0,0
            for cid2 in sorted(correlated[cid]):
                crt2 = getcrtcb(cid2, False)
                if crt2:
                    for event in crt.data['data']:
                        (res, diffneg, diffpos) = crt2.will_see_mount_request(event[DATA_INDEX_REQM])
                        if res:
                            if diffpos <= max(interval_secs):
                                s += 1
                                index = bisect_left(interval_secs, diffpos)
                          #      print diff, interval_secs[index]
                                success[interval_secs[index]].append(diffpos)
                                continue
                                # prefers positive diffs over negative, since positiv diffs are successful
                                # prefetces. Negativ diffs are recent mounts that could be used to prevent
                                # false prefetches

                            #print cid, cid2, event, diff
                            if abs(diffneg) <= max(interval_secs):
                                s += 1
                                index = bisect_left(interval_secs, diffneg)
                          #      print diff, interval_secs[index]
                                success[interval_secs[index]].append(diffneg)
                                continue
                                # continue with negative mount request diff.
                        e+=1
#                    print "%s-%s: e:%i,\t s+:%i, \t s-:%i, \t"%(cid,cid2,e,s,s2)
            tmptotale += e
            tmptotals += s
        else:
            print "crt %s not found."%(cid)

    output = {
        'errorcnt': tmptotale,
        'successcnt': tmptotals,
        'interval' : success
    }
    file = os.path.join("/tmp", "%s_%s.json.gz"%(prefix,os.getpid()))
    print 'writing ',file
    with gzip.open(file, 'w') as f:
        json.dump(output, f, indent=1)
    print 'exit'

def add_correlated(d, a, b):
    for i in [a,b]:
        if i not in d.keys():
            d[i]=set()
    d[a].add(b)
    d[b].add(a)

def _tapestats(inqueue, slot, crtlist, getcrt, drvlist, getdrv, atts, prefix):
    x = {slot:{}}
    while True:
        try:
            tsstr = inqueue.get(True,10)
            ts = datetime.datetime.strptime(tsstr, "%Y-%m-%d %H:%M:%S")
            epochts = int(calendar.timegm(ts.utctimetuple()))
            epochse = epochts + get_slot_size_seconds(slot, ts.month, ts.year)
            #print slot,ts, epochts, epochse
            x[slot][tsstr]={VOL_MNT_LENGTH:0, CLN_TIME_LENGTH:0}

            for crt in crtlist: # not sorted
                x[slot][tsstr][VOL_MNT_LENGTH] += crt.get_volume(slot, ts)


            wq2 = multiprocessing.Queue()
            for drv in drvlist:
                x[slot][tsstr][CLN_TIME_LENGTH] += sum(drv.estimate_cleaning_time(epochts,epochse))

            for att in atts:
                if att not in x[slot][tsstr].keys():
                    x[slot][tsstr][att] = 0
                for drv in drvlist:
                    val = drv.get_pertime(slot, ts, att)
                    #print drv, slot, ts, att, val
                    x[slot][tsstr][att] += val

        except Queue.Empty:
            break
    fn = "%s_%s"%(prefix,slot)
    _cb_gzipdmp(fn,x)

def _cb_pertime(inqueue):
    while True:
        try:
            obj = inqueue.get(True, 10)
            obj.pertime()
        except Queue.Empty:
            break

def _cb_drvclntime(epochts,epochse, inqueue, output, lock):
    res = 0
    while True:
        try:
            drv = inqueue.get(True, 10)

        except Queue.Empty:
            break
    lock.acquire(True)
    output.value = output.value + res
    lock.release()

def get_epoch(ts):
    t = time.strptime(ts, '%Y%m%d:%H%M%S')
    epoch = calendar.timegm(t)
    return epoch

def unix_time(dt):
    epoch = datetime.datetime.utcfromtimestamp(0)
    delta = dt - epoch
    return delta.total_seconds()

HID_INT_MAP = {'o':0}
DRV_INT_MAP = {'o':0}

def plot_dict(d, fn=None):
    keys = sorted(d.keys())
    n = numpy.arange(len(keys))
    ticksstep = int(math.sqrt(len(keys)))
    #print keys
    for k in keys:
        plt.bar(k, d[k])
    plt.xticks(n[0::ticksstep], keys[0::ticksstep])
    if fn:
        plt.savefig(fn)
    else:
        plt.show()

def percentile(N, P):
    """
    Find the percentile of a list of values
    @parameter N - A list of values.  N must be sorted.
    @parameter P - A float value from 0.0 to 1.0
    @return - The percentile of the values.
    """
    if len(N)==0:
        return 0.0
    n = int(round(P * len(N) + 0.5))
    return N[n-1]

def linebuf_write(buf, value):
    if type(value)==type(0.0):
        buf.write(";%.3f"%round(value,3))
    elif type(value)==type(2):
        buf.write(";%i"%value)
    else:
        buf.write(";%s"%value)

crtfilter_a = ['B' ,'P', 'Q', 'E', 'D']
crtfilter_b = ['MP', 'RT', 'ME', 'MS', 'M0', 'MA']

FULLRUN=False
TIME_BTWN_MNTS = 'tbm'              # time between dismount and mount cartridge
CRT_MNT_LENGTH = 'ml'               # time between mnt and dismount cartridge
VOL_MNT_LENGTH = "vol_ml"           # volume mount length
CLN_TIME_LENGTH = 'clntm'           # cleaning time
TOTAL_MNTS = 'tm'
TOTAL_MNT_TIME = 'tmt'
LATENCY_MNT = "latmnt"              # time between mnt request and mount
LATENCY_DMNT = "latdismnt"          # time between dismnt request and dismnt
LATENCY_DMV_DMCR = "lat_dmv_dmcr"    # time between volume dismount and cartridge dismount request

LATENCY_HOME_AVG    = "latency_mean"
HOME_TOTAL_OPS      = "home_total_ops"

REMNT_30_SEC = 'rem_30_sec'     # in percent
REMNT_60_SEC = 'rem_60_sec'     # in percent
REMNT_120_SEC = 'rem_120_sec'     # in percent
REMNT_300_SEC = 'rem_300_sec'     # in percent
REMNT_600_SEC = 'rem_600_sec'   # in percent
REMNT_1800_SEC = 'rem_1800_sec'
REMNT_3600_SEC = 'rem_3600_sec' # in percent
REMNT_7200_SEC = 'rem_7200_sec' # in percent
REMNT_86400_SEC = 'rem_86400_sec' # in percent
REMNT_regexpattern = re.compile("rem_([0-9]+)_sec")

HOTNESS = "hot"          # hotness in hours

PERTIMESLOT = ['per_hour', 'per_day', 'per_week','per_month', 'per_year']

def get_slot_end(ts, slot):
    return ts+datetime.timedelta(seconds=get_slot_size_seconds(slot,ts.month, ts.year))

def get_slot_size_seconds(slot, month, year):
    hour = 3600.0
    if slot=='per_hour':
        return hour
    elif slot=='per_day':
        return hour*24
    elif slot=='per_week':
        return hour*24*7
    elif slot == 'per_month':
        if month in [0, 2, 4, 6, 7, 9, 11]:
            return hour*24*31
        if month in [3, 5, 8, 10]:
            return hour*24*30
        if not year % 4:
            return hour*24*29
        return hour*24*28
    elif slot == 'per_year':
        if not year%4:
            return hour*24*366  # leap year
        return hour*24*365

GLOBAL_CRT      = [TOTAL_MNTS, REMNT_30_SEC, REMNT_60_SEC, REMNT_120_SEC, REMNT_300_SEC , REMNT_600_SEC, REMNT_1800_SEC, REMNT_3600_SEC, REMNT_7200_SEC,REMNT_86400_SEC ]
GLOBAL_DRV      = [TOTAL_MNTS, REMNT_30_SEC, REMNT_60_SEC, REMNT_120_SEC, REMNT_300_SEC , REMNT_600_SEC, REMNT_1800_SEC, REMNT_3600_SEC, REMNT_7200_SEC,REMNT_86400_SEC ]
SPECIAL_CRT     = [CRT_MNT_LENGTH, TIME_BTWN_MNTS, LATENCY_MNT, LATENCY_DMNT, LATENCY_DMV_DMCR]
SPECIAL_DRV     = [CRT_MNT_LENGTH, TIME_BTWN_MNTS, LATENCY_MNT, LATENCY_DMNT, CLN_TIME_LENGTH]
GLOBAL_HM       = [LATENCY_HOME_AVG, HOME_TOTAL_OPS]

HOME_OPERATION_EJECT    = 0
HOME_OPERATION_INJECT   = 1

DATA_INDEX_REQM     = 0     # request mount
DATA_INDEX_M        = 1     # mount done
DATA_INDEX_VOLUME   = 2     # volume (mount-dismounts)
DATA_INDEX_REQD     = 3     # request dismount cartridge
DATA_INDEX_D        = 4     # dismount cartridge
DATA_INDEX_MH       = 5
DATA_INDEX_DH       = 6
DATA_INDEX_DRV      = 7     # drive id, cartridge id or home id,

FSM_CRT_HOME = 1
FSM_CRT_LOADING = 3
FSM_CRT_VOLMNTED = 4
FSM_CRT_LOADED = 5
FSM_CRT_UNLOADING = 6
FSM_CRT_NEW = 7
FSM_CRT_IMPLICIT_CRTMNT = 10
FSM_CRT_VOLMNTDISMNTFU = 11
FSM_CRT_ERROR = 20
FSM_CRT_ERROR_FATAL = 21
FSM_CRT_D2DMV = 22
FSM_CRT_EJECTED = 23

FSM_DRV_LOADED = 30
#FSM_DRV_DISMNTREQ = 31
FSM_DRV_EMPTY = 32
FSM_DRV_ERROR = 40
FSM_DRV_ERROR_FATAL = 41
FSM_DRV_MAYBERECOVERED = 42

FSM_EVNT_INJECT = 50
FSM_EVNT_EJECT = 51
FSM_EVNT_MNTREQ = 52
FSM_EVNT_MNTCMPLT = 53

FSM_EVNT_VOLDMNT = 54
FSM_EVNT_VOLMNT = 55
FSM_EVNT_DISMNTCRTCMPLT = 56
FSM_EVNT_DISMNTCRTREQ = 57

FSM_EVNT_FATALERROR_1 = 60
FSM_EVNT_RECOVER_FAT1 = 61
FSM_EVNT_D2DMV = 62
FSM_EVNT_ROB1 = 63          # robot unable to find cartridge
FSM_EVNT_DELDRIVE = 64

class BaseStats:
    def __init__(self, basedir):
        self.data = {
            'id':None,
            'errors':[],
            'inject':[],
            'eject':[],
            'data': [],
        }
        self.fsm = FiniteStateMachine()

        self.basedir = basedir
        if not os.path.exists(self.basedir ):
                os.makedirs(self.basedir)
        self.pertimeres = None
        self.flushactive()

    def active_is_nonempty(self):
        for i in [self.active['reqm'], self.active['m'], self.active['reqd'], self.active['d']]:
            return True
        if max(self.active['vol'].keys()) > 0:
            return True
        return False

    def flusherror(self):
        if self.active_is_nonempty():
            self.data['errors'].append(self.active)
        self.flushactive()

    def _transform_dictentry(self, entry):
        y = []
        if type(entry['vol'])==type({}):
            for k,v in sorted(entry['vol'].items()):
                y.append((int(k),int(v)))
        if type(entry['vol']) == type([]):
            for elem in sorted(entry['vol']):
                for k,v in elem.items():
                    y.append((int(k),int(v)))
        return [entry['reqm'], entry['m'], y, entry['reqd'], entry['d'], entry['mh'],entry['dh'], entry['drv']]

    def dataappend(self):
        if len(self.active['drv']) > 0 and \
            (self.active['m'] > 0 or self.active['reqm']) and \
            (self.active['d'] > 0 or self.active['reqd']):
            # entry look at DATA_INDEX_xxx global variables
            self.data['data'].append(self._transform_dictentry(self.active))
            self.flushactive()
        else:
            self.flusherror()

    def datainsert(self, entry):
        for i in self.data['data']:
            if i[DATA_INDEX_REQM] >= entry['d']:
                index = self.data['data'].index(i)
                self.data['data'].insert(index, self._transform_dictentry(entry))
                return
        self.data['data'].append(self._transform_dictentry(entry))

    def flushactive(self):
        self.active = {
            'reqm': 0,
            'm' : 0,
            'vol':{0:0},
            'reqd':0,
            'd':0,
            'mh':0,
            'dh':0,
            'drv':""
        }

    def __repr__(self):
        return self.data['id']

    def estimate_cleaning_time(self, start=None, end=None):
        return []# dummy, cartridge doesnt need this

    # -----------------------------
    def collect_recovered_errors(self):
        total = len(self.data['errors'])
        def isvalid(entry):
            for k in ['m','d','reqd','reqm']:
                if not k in entry:
                    return False
                if entry[k] <= 0:
                    return False
            for k in ['mh','dh','drv']:
                if not k in entry:
                    return False
                if type(entry[k])==type(1) or len(entry[k]) < 2:
                    return False
            self.datainsert(entry)
            return True

        self.data['errors'][:] = [x for x in self.data['errors'] if not isvalid(x)]
        remaining = len(self.data['errors'])
        return (total-remaining, remaining)

    def get_successful_cycles(self):
        return len(self.data['data'])

    def get_failed_cycles(self):
        return len(self.data['errors'])

    def binsearch(self, data, entry, referenceindex, lo=0, hi=None):
        if hi is None:
            hi = max(0,len(data)-1 )
        if abs(hi-lo)<2:
            if len(data)<hi:
                if data[hi][referenceindex] < entry:
                    return hi
            return lo
        pos = (hi+lo)/2
        if data[pos][referenceindex]>entry:
            hi = pos
        else:
            lo = pos
        return self.binsearch(data,entry,referenceindex,lo,hi)

    def robot_mount(self, epoch, drive, library):
        most_likeley_entry = {}
        startindex = self.binsearch(self.data['data'], epoch, DATA_INDEX_REQM)
        while(startindex < len(self.data['data'])):
            entry = self.data['data'][startindex]
            if entry[DATA_INDEX_REQM] < epoch:
                if entry[DATA_INDEX_DRV] == drive:
                    diff_m = entry[DATA_INDEX_M] - epoch
                    if abs(diff_m) <= 600 :
                        index = self.data['data'].index(entry)
                        most_likeley_entry[diff_m]=index
            else:
                break
            startindex += 1
        if len(most_likeley_entry)>0:
            x = min(most_likeley_entry.keys())
            #print x, most_likeley_entry[x]
            entry = self.data['data'][most_likeley_entry[x]]
            entry[DATA_INDEX_MH] = library
            return True
        else:
            plausible_entries = {}
            for entry in self.data['errors']:
                if entry['drv'] == drive:
                    diff = entry['m'] - epoch
                    if abs(diff) <= 120:
                        #print "ok error entry found... ", entry
                        entry['mh']=library
                        return True
                    diff_mr = epoch - entry['reqm']
                    if diff_mr > 0 and diff_mr < 120:
                        entry['m']=epoch
                        entry['mh']=library
                        return True
                    if entry['d'] > epoch:
                        diff = entry['d'] - epoch
                        plausible_entries[diff] = entry
            if len(plausible_entries)>0:
                entry = plausible_entries[min(plausible_entries.keys())]
                entry['m']=epoch
                entry['mh']=library
                return True
            self.data['errors'].append({'m':epoch, 'reqm':0, 'reqd':0, 'd':0, 'vol':{0:0}, 'mh':library, 'drv':drive, 'dh':""})
            #print "mount Nothing found"

    def robot_dismount(self, epoch, drive, library):
        most_likeley_entry = {}
        startindex = self.binsearch(self.data['data'], epoch, DATA_INDEX_REQD)
        while(startindex < len(self.data['data'])):
            entry = self.data['data'][startindex]

        #for entry in self.data['data']:
            if entry[DATA_INDEX_REQD] < epoch:
                if entry[DATA_INDEX_DRV] == drive:
                    diff_m = entry[DATA_INDEX_D] - epoch
                    if diff_m >= 0 or entry[DATA_INDEX_D]==0:
                        index = self.data['data'].index(entry)
                        most_likeley_entry[diff_m]=index
            else:
                break
            startindex += 1
        if len(most_likeley_entry)>0:
            x = min(most_likeley_entry.keys())
            #print x, most_likeley_entry[x]
            entry = self.data['data'][most_likeley_entry[x]]
            entry[DATA_INDEX_DH] = library
            if entry[DATA_INDEX_D]==0:
                entry[DATA_INDEX_D]=epoch

            return True
        else:
            plausible_entries = {}
            for entry in self.data['errors']:
                if entry['drv']==drive:
                    if entry['d'] <= epoch:
                        diff = entry['d'] - epoch
                        if diff >= 0 and diff < 120:
                            #print "ok error entry found... ", entry
                            entry['dh']=library
                            return True
                    if entry['reqd'] <= epoch:
                        diff = epoch - entry['reqd']
                        if diff < 120:
                            #print "ok error entry found... ", entry
                            entry['d'] = epoch
                            entry['dh']=library
                            return True
                    if entry['m'] < epoch:
                        diff = epoch - entry['m']
                        plausible_entries[diff]=entry
            if len(plausible_entries)>0:
                entry = plausible_entries[min(plausible_entries.keys())]
                entry['d']=epoch
                entry['dh']=library
                return True
            self.data['errors'].append({'d':epoch, 'reqm':0, 'reqd':0, 'm':0, 'vol':{0:0}, 'dh':library, 'drv':drive, 'mh':""})
            #print "dismount Nothing found"

    def get_predecessor_of_entry(self, epoch, reference=DATA_INDEX_M): #
        for e in self.data['data']:
            if e[reference] > epoch:
                index = self.data['data'].index(e) - 1
                if index >= 0:
                    return self.data['data'][index]

    def will_see_mount_request(self, epoch):
        # return
        #   (False,None): not mounted, never willbe
        #   (True, x)   : x diff to mount event
        #                : negativ x means already mounted
        x = None
        index = self.binsearch(self.data['data'], epoch, referenceindex=DATA_INDEX_REQM)
        mindistneg = -sys.maxint
        mindistpos = sys.maxint
        breakat = 1800

        while True:
            if len(self.data['data'])>index:
                entry = self.data['data'][index]
                entrydiff = entry[DATA_INDEX_REQM] - epoch  # negative or zero

                if entry[DATA_INDEX_REQM] < epoch:
                    if entry[DATA_INDEX_REQD] >= epoch:    # already mounted
                        return (True, max(-1799, entrydiff),mindistpos) # is still mounted
                    mindistneg = max(mindistneg, entrydiff)
                else:                                       # else switch to positive mounts
                    if entrydiff <= breakat:
                        mindistpos = entrydiff
                    return (True, mindistneg, mindistpos)                  # negative mount closer to zero

                if entrydiff > breakat:
                    break
                index += 1
            else:
                break
        if mindistneg == -sys.maxint and mindistpos == sys.maxint:
            return (False, None, None)
        return (True, mindistneg, mindistpos)

    # ---------------------
    def coredump(self):
        for i in self.data['data']:
            print i
        print self.active
        print self
        sys.exit(1)

    def sanitycheck(self):
        lm=None
        restart = None
        for i in self.data['data']:
            if i[DATA_INDEX_REQM] > i[DATA_INDEX_M]:
                print "Error ", i[DATA_INDEX_REQM], i[DATA_INDEX_M]
                return False
            if i[DATA_INDEX_REQD] > i[DATA_INDEX_D]:
                print "Error ", i[DATA_INDEX_REQD], i[DATA_INDEX_D]
                return False
            if lm:
                if lm[DATA_INDEX_D] > i[DATA_INDEX_M]:
                    print "Error, dmnt larger than following mnt",lm[DATA_INDEX_D], i[DATA_INDEX_M], self
                    print lm
                    print i
                    if 1:
                        index = self.data['data'].index(lm)
                        del self.data['data'][index]
                        index = self.data['data'].index(i)
                        del self.data['data'][index]
                        return self.sanitycheck()
                    else:
                        return False
            lm = i
        return True

    def sortdata(self):
        tmp = []
        ref = self.data['data']
        for i in ref:
            index = ref.index(i)
            if index > 0:
                if ref[index][DATA_INDEX_M] < ref[index-1][DATA_INDEX_M]:
                    print "error mount timestamp"
                if ref[index][DATA_INDEX_D] < ref[index-1][DATA_INDEX_D]:
                    print "error dismount timestamp"
                if ref[index][DATA_INDEX_M] < ref[index-1][DATA_INDEX_D]:
                    print "error dismount timestamp m-d"

    def checkerrors(self):
        self.sortdata()
        if not self.sanitycheck():
            self.coredump()
        if len(self.data['errors'])>0:
            #print self.data['errors']
            pass
        return len(self.data['errors'])

    def handle_special(self,res, data, key):
        if len(data)>0:
            res['%s_mean'%key] = numpy.mean(data)
            res['%s_sum'%key] = sum(data)
            res['%s_min'%key] = min(data)
            res['%s_max'%key] = max(data)
            res['%s_p05'%key] = percentile(sorted(data), 0.05)
            res['%s_p10'%key] = percentile(sorted(data), 0.1)
            #res['%s_p20'%key] = percentile(sorted(data), 0.2)
            res['%s_p33'%key] = percentile(sorted(data), 0.33)
            #res['%s_p40'%key] = percentile(sorted(data), 0.4)
            res['%s_p50'%key] = percentile(sorted(data), 0.5)
            res['%s_p67'%key] = percentile(sorted(data), 0.67)
            #res['%s_p70'%key] = percentile(sorted(data), 0.7)
            #res['%s_p80'%key] = percentile(sorted(data), 0.8)
            res['%s_p90'%key] = percentile(sorted(data), 0.9)
            res['%s_p95'%key] = percentile(sorted(data), 0.95)

    def pertime(self, data=None):
        def handle_lowlevel(ref, tmpts, curmnt, curdmnt, increment):
            tmpmnt = max(curmnt, tmpts)
            nextts=None
            tmptsstring = tmpts.strftime("%Y-%m-%d %H:%M:%S")
            if not tmptsstring in ref:
                ref[tmptsstring] = {}
            obj = ref[tmptsstring]
            for x in [TOTAL_MNT_TIME, TOTAL_MNTS]:
                if not x in obj.keys():
                    obj[x] = 0
            if tmpmnt < tmpts:
                print "wtf", tmpmnt, tmpts, increment, curdmnt
                #return tmpts
                return (False, tmpts)

            if type(increment)==type("string"):
                if increment=='month':
                    m = (tmpts.month)%12
                    y = tmpts.year
                    if (tmpts.month)/12:
                        y += 1
                    nextts = datetime.datetime(year=y, month=m+1, day=1)
                elif increment == 'year':
                    nextts = datetime.datetime(year=tmpts.year+1, month=1, day=1)
            else:
                nextts = tmpts + increment
            #print nextts, increment,tmpts
            if tmpmnt <= nextts: # noch im momentanen ts weitermachen
                if curdmnt <= nextts:                ### case a
                    td = curdmnt - tmpmnt
                    obj[TOTAL_MNT_TIME] += td.seconds + td.days*24*60*60
                    obj[TOTAL_MNTS] += 1
                else:
                    td = nextts - tmpmnt
                    obj[TOTAL_MNT_TIME] += td.seconds + td.days*24*60*60 ### case b
                    return (True, nextts)
                    #return handle_lowlevel(ref, nextts, nextts, curdmnt, increment)
            else:   # neuen ts nutzen
                #return handle_lowlevel(ref, nextts, curmnt, curdmnt, increment)
                return (True, nextts)
            return (False, tmpts)

        if self.pertimeres != None:
            return self.pertimeres

        res = {
                'per_hour'  : {},
                'per_day'   : {},
                'per_week'  : {},
                'per_month' : {},
                'per_year'  : {}
        }

        file = os.path.join(self.basedir,"crt_%s_pertime.json"%self)
        if not os.path.isfile(file):
            if data==None:
                data = self.data['data']
            if len(data)>0:
                init_mount = datetime.datetime.fromtimestamp(data[0][DATA_INDEX_M])
                tmpts_perhour = datetime.datetime(init_mount.year, init_mount.month, init_mount.day, init_mount.hour,0)
                tmpts_perday = datetime.datetime(init_mount.year, init_mount.month, init_mount.day,0,0)
                tmpts_perweek = datetime.datetime(init_mount.year, init_mount.month, 1, 0, 0)
                tmpts_permonth = datetime.datetime(init_mount.year, init_mount.month, 1, 0, 0)
                tmpts_peryear = datetime.datetime(init_mount.year, 1, 1, 0, 0)

                increment_hour = datetime.timedelta(hours=1)
                increment_day = datetime.timedelta(days=1)
                increment_week = datetime.timedelta(days=7)

                for i in data:
                    mount = datetime.datetime.fromtimestamp(i[DATA_INDEX_M])
                    dismount = datetime.datetime.fromtimestamp(i[DATA_INDEX_D])

                    tmpts_perhour = datetime.datetime(mount.year, mount.month, mount.day, 0,0)
                    tmpts_perday = datetime.datetime(mount.year, mount.month, mount.day, 0,0)
    #                for (ref, tmpts, incr) in ['per_hour', ]
                    cont = True
                    while cont:
                        (cont,tmpts_perhour) = handle_lowlevel(res['per_hour'], tmpts_perhour, mount, dismount,increment_hour)
                    cont = True
                    while cont:
                        (cont,tmpts_perday) = handle_lowlevel(res['per_day'], tmpts_perday, mount, dismount,increment_day)
                    cont = True
                    while cont:
                        (cont,tmpts_perweek) = handle_lowlevel(res['per_week'], tmpts_perweek, mount, dismount,increment_week)
                    cont = True
                    while cont:
                        (cont,tmpts_permonth) = handle_lowlevel(res['per_month'], tmpts_permonth, mount, dismount,"month")
                    cont = True
                    while cont:
                        (cont,tmpts_peryear) = handle_lowlevel(res['per_year'], tmpts_peryear, mount, dismount,"year")
            else:
                print self, "no data available"

            for slot in PERTIMESLOT:    ## add attribute hotness
                hotness = 0.0
                for entryts in sorted(res[slot].keys()):
                    #print entryts
                    dt = datetime.datetime.strptime(entryts, "%Y-%m-%d %H:%M:%S")
                    totalslottime = get_slot_size_seconds(slot, dt.month, dt.year)
                    hotness = (hotness + res[slot][entryts].get(TOTAL_MNT_TIME,0)/totalslottime)/2.0
                    res[slot][entryts][HOTNESS] = hotness

            for slot in PERTIMESLOT:    ## print to csv
                name = os.path.join(self.basedir, "%s_%s.csv"%(self,slot))
                with open(name, 'w') as csv_file:
                    sortres = []
                    lineBuf = StringIO.StringIO()
                    cnt=1
                    for entryts in sorted(res[slot].keys()):
                        if lineBuf.len==0:
                            lineBuf.write("timestamp;index")
                            sortres = sorted(res[slot][entryts].keys())
                            for k in sortres:
                                lineBuf.write(";%s"%k)
                            lineBuf.write("\n")
                        lineBuf.write(";%s;%i"%(entryts,cnt))
                        for k in sortres:
                            linebuf_write(lineBuf, res[slot][entryts].get(k,0))
                        lineBuf.write("\n")
                        cnt+=1
                    csv_file.write(lineBuf.getvalue())
                    csv_file.close()
            with open(file, 'w') as f:
                json.dump(res, f, indent=1)
        else:
            print "reading file"
            with open(file, 'r') as f:
                res = json.load(f)
        self.pertimeres = res
        return res

    def stats(self, atts, special):
        res = {}
        for a in atts:
            match = REMNT_regexpattern.match(a)
            if match:
                slot = int(match.group(1))
                lm = None
                tmpres = []
                for i in self.data['data']:
                    if lm != None:
                        if lm > 0:
                            if i[DATA_INDEX_M] - lm <= slot:
                                tmpres.append(1)
                            else:
                                tmpres.append(0)
                        else:
                            print 'no lm'
                    lm = i[DATA_INDEX_D]
                res[a] = numpy.mean(tmpres)

            elif a == 'id':
                res['id'] = self.data['id']
            elif a == TOTAL_MNTS:
                res[a] = len(self.data['data'])

        for a in special:
            x = []
            errcnt = 0
            if a == CRT_MNT_LENGTH:
                for i in self.data['data']:
                    if i[DATA_INDEX_M] > 0 and i[DATA_INDEX_D] > 0:
                        diff = i[DATA_INDEX_D] - i[DATA_INDEX_M]
                        if diff >= 0:
                            x.append(diff)
                        else:
                            print "Mount before dismount wtf"
                            errcnt += 1
                    #for (m,d) in i[DATA_INDEX_VOLUME].items():
                    #    diff = int(m)-int(d)
                    #    if diff > 0:
                    #        x.append(diff)
                    else:
                        errcnt+=1
            elif a == TIME_BTWN_MNTS:
                lm = 0
                for i in self.data['data']:
                    if lm > 1:
                        if i[DATA_INDEX_M] > 0:
                            x.append(i[DATA_INDEX_M]-lm)
                    lm = i[DATA_INDEX_D]
            elif a == LATENCY_MNT:
                for i in self.data['data']:
                    diff = i[DATA_INDEX_M] - i[DATA_INDEX_REQM]
                    if diff < 0 or diff > 6*3600:
                        errcnt += 1
                        #print "error diff:",diff,  '\t', self
                    else:
                        x.append(diff)
            elif a == LATENCY_DMNT:
                for i in self.data['data']:
                    diff = i[DATA_INDEX_D] - i[DATA_INDEX_REQD]
                    if diff < 0 or diff > 6*3600:
                        errcnt+=1
                        #print "error diff:",diff,  '\t', self
                    else:
                        x.append(diff)
            elif a == LATENCY_DMV_DMCR:
                for i in self.data['data']:
                    last_voldismnt = 0
                    for m,d in i[DATA_INDEX_VOLUME]:
                        last_voldismnt = max(last_voldismnt, d)
                    diff = i[DATA_INDEX_REQD] - last_voldismnt
                    if diff < 0 or diff > 6*3600:
                        errcnt+=1
                        #print "error diff:",diff,  '\t', self
                    else:
                        x.append(diff)
    #        if errcnt:
     #           print self, a, len(x), 'error cnt', errcnt
            self.handle_special(res, x, a)
        return res

    def get_pertime(self,slot, ts, attribute):
        res = self.pertime()
        uts = unicode(ts)
        if slot in res.keys():
            #print res[slot]
            if uts in res[slot].keys():
                #print res[slot][uts]
                return res[slot][uts].get(attribute,0)
            if type(ts) == type(" "):
                dt = datetime.datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
                if dt in res[slot].keys():
                    return res[slot][dt].get(attribute,0)

        else:
            print "no slot", slot
        return 0

    def handle_error(self, epoch, drive=None):
        self.data['minor'].append(epoch)

    def handle_mount_req(self, epoch):  # either full cartridge mount or volume mount
        drv = None
        if not self.active['reqm']:
            if int(epoch) < self.active['m'] or self.active['m'] == 0:
                self.active['reqm'] = int(epoch)
                drv = self.active['drv']

        keys = self.active['vol'].keys()
        keys.append(0)
        m = max(keys)
        if m > 0:
            voldm = self.active['vol'][m]
            if voldm <= int(epoch) or int(epoch) == self.active['reqm']:      # ok, last mount dismount happened before current timestamp
                self.active['vol'][int(epoch)]=0
                drv = self.active['drv']
            else:
                print "voldm greater epoch", epoch
                #self.coredump()
        else:
            if epoch != self.active['reqm']:
                print "m less equal 0", epoch
                #self.coredump()
        return drv

    def handle_dismount_req(self, epoch):
        drv = None
        if not self.active['reqd']:
             self.active['reqd'] = int(epoch)
             drv = self.active['drv']
        else:
            print "handle dismount request: why existing"
        return drv

    #def old_search_for_mountentry(self, epoch, reference=DATA_INDEX_M):
    #    for e in self.data['data']:
    #        if e[reference] >= epoch:
    #            #if e[DATA_INDEX_D] == 0:
    #                return self.data['data'].index(e)
    #    print "no entry found", self, epoch
    #    return None

    #def old_entryexists(self, epoch, reference=DATA_INDEX_M, drive=""):
    #    for e in self.data['data']:
    #        if abs(e[reference] - epoch) <= 10:
    #            if e[DATA_INDEX_DRV] == drive:
    #                return True

    #def old_errorentryexists(self, epoch, reference=DATA_INDEX_D):
    #    for e in self.data['errors']:
    #        diff = e[reference] - epoch
    #        if diff:
    #q            return True

class Cartridge(BaseStats):
    def __init__(self,id, basedir):
        BaseStats.__init__(self, os.path.join(basedir, "cartridges"))
        self.data['id']=id
        self.fsm.state=FSM_CRT_ERROR

        self.fsm.add_transition(FSM_EVNT_INJECT, FSM_CRT_NEW, FSM_CRT_HOME, self._cb_inject)
        self.fsm.add_transition(FSM_EVNT_INJECT, FSM_CRT_ERROR, FSM_CRT_HOME, self._cb_inject)
        self.fsm.add_transition(FSM_EVNT_INJECT, FSM_CRT_EJECTED, FSM_CRT_HOME, self._cb_inject)
        self.fsm.add_transition(FSM_EVNT_INJECT, FSM_CRT_ERROR_FATAL, FSM_CRT_HOME, self._cb_inject)
        self.fsm.add_transition(FSM_EVNT_INJECT, FSM_CRT_HOME,FSM_CRT_HOME, None)

        self.fsm.add_transition(FSM_EVNT_MNTREQ, FSM_CRT_LOADING,FSM_CRT_LOADING, None) # ignore double mount req
        self.fsm.add_transition(FSM_EVNT_MNTREQ, FSM_CRT_IMPLICIT_CRTMNT, FSM_CRT_LOADING, None)
        self.fsm.add_transition(FSM_EVNT_MNTREQ, FSM_CRT_HOME, FSM_CRT_LOADING, self._cb_mntreq)
        self.fsm.add_transition(FSM_EVNT_MNTREQ, FSM_CRT_VOLMNTDISMNTFU, FSM_CRT_LOADING , self._cb_voldmfu_to_loading)
        self.fsm.add_transition(FSM_EVNT_MNTREQ, FSM_CRT_ERROR, FSM_CRT_LOADING, self._cb_recover_to_loading)

        self.fsm.add_transition(FSM_EVNT_MNTREQ, FSM_CRT_UNLOADING, FSM_CRT_LOADING, self._cb_unloading_timeout)
        self.fsm.add_transition(FSM_EVNT_MNTREQ, FSM_CRT_LOADED, FSM_CRT_LOADING, self._cb_loaded_to_loading)
        self.fsm.add_transition(FSM_EVNT_MNTREQ, FSM_CRT_D2DMV, FSM_CRT_ERROR, self._cb_error)
        self.fsm.add_transition(FSM_EVNT_MNTREQ, FSM_CRT_VOLMNTED, FSM_CRT_LOADING, self._cb_volmnt_to_loading)
        self.fsm.add_transition(FSM_EVNT_MNTREQ, FSM_CRT_ERROR_FATAL, FSM_CRT_ERROR_FATAL, self._cb_error)

        self.fsm.add_transition(FSM_EVNT_MNTCMPLT, FSM_CRT_D2DMV, FSM_CRT_VOLMNTED, self._cb_d2dmv_to_loaded)
        self.fsm.add_transition(FSM_EVNT_MNTCMPLT, FSM_CRT_LOADING, FSM_CRT_VOLMNTED, self._cb_mntcmplt)
        self.fsm.add_transition(FSM_EVNT_MNTCMPLT, FSM_CRT_IMPLICIT_CRTMNT, FSM_CRT_VOLMNTED, self._cb_implicit_crtmnt_cmplt)
        self.fsm.add_transition(FSM_EVNT_MNTCMPLT, FSM_CRT_HOME, FSM_CRT_ERROR_FATAL, self._cb_error)
        self.fsm.add_transition(FSM_EVNT_MNTCMPLT, FSM_CRT_UNLOADING, FSM_CRT_ERROR, self._cb_error)
        self.fsm.add_transition(FSM_EVNT_MNTCMPLT, FSM_CRT_LOADED, FSM_CRT_ERROR, self._cb_error)
        self.fsm.add_transition(FSM_EVNT_MNTCMPLT, FSM_CRT_VOLMNTED, FSM_CRT_VOLMNTED, None)
        self.fsm.add_transition(FSM_EVNT_MNTCMPLT, FSM_CRT_ERROR, FSM_CRT_ERROR, self._cb_error)
        self.fsm.add_transition(FSM_EVNT_MNTCMPLT, FSM_CRT_ERROR_FATAL, FSM_CRT_ERROR_FATAL, self._cb_error)
        self.fsm.add_transition(FSM_EVNT_MNTCMPLT, FSM_CRT_VOLMNTDISMNTFU, FSM_CRT_ERROR, self._cb_error )

        self.fsm.add_transition(FSM_EVNT_VOLMNT, FSM_CRT_UNLOADING, FSM_CRT_UNLOADING, None)
        self.fsm.add_transition(FSM_EVNT_VOLMNT, FSM_CRT_LOADING, FSM_CRT_LOADING, None)
        self.fsm.add_transition(FSM_EVNT_VOLMNT, FSM_CRT_LOADED, FSM_CRT_VOLMNTED, self._cb_volmnt)
        self.fsm.add_transition(FSM_EVNT_VOLMNT, FSM_CRT_VOLMNTED, FSM_CRT_VOLMNTDISMNTFU, None)
        self.fsm.add_transition(FSM_EVNT_VOLMNT, FSM_CRT_HOME, FSM_CRT_IMPLICIT_CRTMNT, self._cb_implicit_crtmnt)
        self.fsm.add_transition(FSM_EVNT_VOLMNT, FSM_CRT_IMPLICIT_CRTMNT, FSM_CRT_IMPLICIT_CRTMNT,None)
        self.fsm.add_transition(FSM_EVNT_VOLMNT, FSM_CRT_ERROR, FSM_CRT_ERROR, self._cb_error)
        self.fsm.add_transition(FSM_EVNT_VOLMNT, FSM_CRT_ERROR_FATAL, FSM_CRT_ERROR_FATAL, self._cb_error)
        self.fsm.add_transition(FSM_EVNT_VOLMNT, FSM_CRT_EJECTED, FSM_CRT_ERROR, self._cb_error)
        self.fsm.add_transition(FSM_EVNT_VOLMNT, FSM_CRT_VOLMNTDISMNTFU,FSM_CRT_VOLMNTDISMNTFU,None )
        self.fsm.add_transition(FSM_EVNT_VOLMNT, FSM_CRT_D2DMV, FSM_CRT_ERROR, self._cb_error)

        self.fsm.add_transition(FSM_EVNT_VOLDMNT, FSM_CRT_VOLMNTED, FSM_CRT_LOADED, self._cb_voldmnt)
        self.fsm.add_transition(FSM_EVNT_VOLDMNT, FSM_CRT_VOLMNTDISMNTFU, FSM_CRT_VOLMNTED , None)
        self.fsm.add_transition(FSM_EVNT_VOLDMNT, FSM_CRT_IMPLICIT_CRTMNT, FSM_CRT_ERROR, self._cb_error)
        self.fsm.add_transition(FSM_EVNT_VOLDMNT, FSM_CRT_D2DMV, FSM_CRT_ERROR, self._cb_error)
        self.fsm.add_transition(FSM_EVNT_VOLDMNT, FSM_CRT_LOADING, FSM_CRT_ERROR, self._cb_error)
        self.fsm.add_transition(FSM_EVNT_VOLDMNT, FSM_CRT_LOADED, FSM_CRT_ERROR, self._cb_error)
        self.fsm.add_transition(FSM_EVNT_VOLDMNT, FSM_CRT_HOME, FSM_CRT_ERROR, self._cb_error)
        self.fsm.add_transition(FSM_EVNT_VOLDMNT, FSM_CRT_ERROR, FSM_CRT_ERROR, self._cb_error)
        self.fsm.add_transition(FSM_EVNT_VOLDMNT, FSM_CRT_ERROR_FATAL, FSM_CRT_ERROR_FATAL, self._cb_error)
        self.fsm.add_transition(FSM_EVNT_VOLDMNT, FSM_CRT_UNLOADING, FSM_CRT_ERROR, self._cb_error)

        self.fsm.add_transition(FSM_EVNT_DISMNTCRTREQ, FSM_CRT_D2DMV,FSM_CRT_ERROR, self._cb_error)
        self.fsm.add_transition(FSM_EVNT_DISMNTCRTREQ, FSM_CRT_UNLOADING,FSM_CRT_UNLOADING, None)
        self.fsm.add_transition(FSM_EVNT_DISMNTCRTREQ, FSM_CRT_VOLMNTDISMNTFU, FSM_CRT_UNLOADING, self._cb_dismnt_crt_req)
        self.fsm.add_transition(FSM_EVNT_DISMNTCRTREQ, FSM_CRT_LOADED, FSM_CRT_UNLOADING, self._cb_dismnt_crt_req)
        self.fsm.add_transition(FSM_EVNT_DISMNTCRTREQ, FSM_CRT_VOLMNTED, FSM_CRT_UNLOADING, self._cb_implicit_crtdism_while_volmnt)
        self.fsm.add_transition(FSM_EVNT_DISMNTCRTREQ, FSM_CRT_IMPLICIT_CRTMNT, FSM_CRT_ERROR, self._cb_error)
        self.fsm.add_transition(FSM_EVNT_DISMNTCRTREQ, FSM_CRT_HOME, FSM_CRT_ERROR, self._cb_error)
        self.fsm.add_transition(FSM_EVNT_DISMNTCRTREQ, FSM_CRT_LOADING, FSM_CRT_ERROR, self._cb_error)
        self.fsm.add_transition(FSM_EVNT_DISMNTCRTREQ, FSM_CRT_ERROR, FSM_CRT_ERROR, self._cb_error)
        self.fsm.add_transition(FSM_EVNT_DISMNTCRTREQ, FSM_CRT_ERROR_FATAL, FSM_CRT_ERROR_FATAL, self._cb_error)

        self.fsm.add_transition(FSM_EVNT_DISMNTCRTCMPLT, FSM_CRT_VOLMNTDISMNTFU, FSM_CRT_ERROR, self._cb_error)
        self.fsm.add_transition(FSM_EVNT_DISMNTCRTCMPLT, FSM_CRT_LOADING, FSM_CRT_HOME, self._cb_recover_to_home)
        self.fsm.add_transition(FSM_EVNT_DISMNTCRTCMPLT, FSM_CRT_UNLOADING, FSM_CRT_HOME, self._cb_dismnt_crt_cmplt)
        self.fsm.add_transition(FSM_EVNT_DISMNTCRTCMPLT, FSM_CRT_LOADED, FSM_CRT_HOME, self._cb_dismnt_crt_cmplt)
        self.fsm.add_transition(FSM_EVNT_DISMNTCRTCMPLT, FSM_CRT_ERROR, FSM_CRT_HOME, self._cb_recover_to_home)
        self.fsm.add_transition(FSM_EVNT_DISMNTCRTCMPLT, FSM_CRT_ERROR_FATAL, FSM_CRT_HOME, self._cb_recover_to_home)
        self.fsm.add_transition(FSM_EVNT_DISMNTCRTCMPLT, FSM_CRT_VOLMNTED, FSM_CRT_ERROR, self._cb_error)
        self.fsm.add_transition(FSM_EVNT_DISMNTCRTCMPLT, FSM_CRT_HOME,FSM_CRT_HOME,None) # ignore duplicates
        self.fsm.add_transition(FSM_EVNT_DISMNTCRTCMPLT, FSM_CRT_D2DMV, FSM_CRT_HOME, self._cb_recover_to_home )
        self.fsm.add_transition(FSM_EVNT_DISMNTCRTCMPLT, FSM_CRT_IMPLICIT_CRTMNT, FSM_CRT_HOME, self._cb_recover_to_home )

        #d2d mv
        self.fsm.add_transition(FSM_EVNT_D2DMV, FSM_CRT_VOLMNTED,FSM_CRT_D2DMV, self._cb_loaded_to_d2dmv)
        self.fsm.add_transition(FSM_EVNT_D2DMV, FSM_CRT_D2DMV,FSM_CRT_D2DMV, None)
        self.fsm.add_transition(FSM_EVNT_D2DMV, FSM_CRT_LOADING,FSM_CRT_ERROR, self._cb_error)
        self.fsm.add_transition(FSM_EVNT_D2DMV, FSM_CRT_UNLOADING,FSM_CRT_ERROR, self._cb_error)
        self.fsm.add_transition(FSM_EVNT_D2DMV, FSM_CRT_ERROR_FATAL,FSM_CRT_ERROR_FATAL, self._cb_error)
        self.fsm.add_transition(FSM_EVNT_D2DMV, FSM_CRT_ERROR, FSM_CRT_ERROR, self._cb_error)
        self.fsm.add_transition(FSM_EVNT_D2DMV, FSM_CRT_HOME, FSM_CRT_ERROR, self._cb_error)
        self.fsm.add_transition(FSM_EVNT_D2DMV, FSM_CRT_LOADED, FSM_CRT_ERROR, self._cb_error)


        self.fsm.add_transition(FSM_EVNT_ROB1, FSM_CRT_LOADING, FSM_CRT_ERROR, self._cb_error)
        self.fsm.add_transition(FSM_EVNT_ROB1, FSM_CRT_ERROR_FATAL, FSM_CRT_ERROR_FATAL, self._cb_error)
        self.fsm.add_transition(FSM_EVNT_ROB1, FSM_CRT_ERROR, FSM_CRT_ERROR, self._cb_error)
        self.fsm.add_transition(FSM_EVNT_ROB1, FSM_CRT_HOME, FSM_CRT_ERROR, self._cb_error)
        self.fsm.add_transition(FSM_EVNT_ROB1, FSM_CRT_D2DMV, FSM_CRT_ERROR, self._cb_error)

        self.fsm.add_transition(FSM_EVNT_EJECT, FSM_CRT_ERROR, FSM_CRT_EJECTED, self._cb_eject_error)
        self.fsm.add_transition(FSM_EVNT_EJECT, FSM_CRT_HOME, FSM_CRT_EJECTED, self._cb_eject)
        self.fsm.add_transition(FSM_EVNT_EJECT, FSM_CRT_UNLOADING, FSM_CRT_EJECTED, self._cb_eject_error)
        self.fsm.add_transition(FSM_EVNT_EJECT, FSM_CRT_ERROR_FATAL, FSM_CRT_ERROR, self._cb_eject)
        self.fsm.add_transition(FSM_EVNT_EJECT, FSM_CRT_VOLMNTED, FSM_CRT_ERROR, self._cb_eject_error)
        self.fsm.add_transition(FSM_EVNT_EJECT, FSM_CRT_LOADED, FSM_CRT_ERROR, self._cb_eject_error)
        self.fsm.add_transition(FSM_EVNT_EJECT, FSM_CRT_LOADING, FSM_CRT_ERROR, self._cb_eject_error)
        self.fsm.add_transition(FSM_EVNT_EJECT, FSM_CRT_IMPLICIT_CRTMNT, FSM_CRT_ERROR, self._cb_eject_error)
        self.fsm.add_transition(FSM_EVNT_EJECT, FSM_CRT_EJECTED, FSM_CRT_EJECTED, None)

        self.fsm.add_transition(FSM_EVNT_FATALERROR_1, FSM_CRT_D2DMV, FSM_CRT_ERROR , self._cb_error)
        self.fsm.add_transition(FSM_EVNT_FATALERROR_1, FSM_CRT_ERROR_FATAL, FSM_CRT_ERROR_FATAL, self._cb_error)
        self.fsm.add_transition(FSM_EVNT_FATALERROR_1, FSM_CRT_EJECTED, FSM_CRT_EJECTED, None)

        for state in [FSM_CRT_HOME,FSM_CRT_LOADING,FSM_CRT_VOLMNTED,FSM_CRT_LOADED,FSM_CRT_UNLOADING,FSM_CRT_NEW,\
            FSM_CRT_IMPLICIT_CRTMNT,FSM_CRT_VOLMNTDISMNTFU,FSM_CRT_ERROR]:
            self.fsm.add_transition(FSM_EVNT_FATALERROR_1, state, FSM_CRT_ERROR_FATAL, self._cb_error)

    def handle_event(self, event, args):
        return self.fsm.handle_event(event, args)

    def _cb_volmnt(self,event, args):
        self.active['vol'][int(args['epoch'])] = 0
        return (True, None)

    def _cb_voldmnt(self,event, args):
        m = max(self.active['vol'].keys())
        if m > 0:
            voldm = self.active['vol'][m]
            if voldm <= 0 and m <= int(args['epoch']):
                self.active['vol'][m]=int(args['epoch'])
                return (True, None)
        return (False, None)

    def _cb_inject(self,event, args):
        self.data['inject'].append(args['epoch'])
        self.flusherror()
        return (True,None)

    def _cb_error(self,event, args):
        if event == FSM_EVNT_DISMNTCRTCMPLT:
            self.active['d'] = args['epoch']
            ret={'drive':self.active['drv'],'reqm':self.active['reqm'],'reqd':self.active['reqd'],'cid':self.data['id']}
            return (True, ret)
        elif event == FSM_EVNT_VOLMNT:
            self.active['vol'][int(args['epoch'])] = 0
        elif event == FSM_EVNT_VOLDMNT:
            return self._cb_voldmnt(event, args)
        elif event == FSM_EVNT_MNTCMPLT:
            self.active['m'] = args['epoch']
            self.active['vol'] = {int(args['epoch']):0}
            self.active['drv'] = args['drive']
            return (True, {'drive':args['drive']})
        elif event == FSM_EVNT_DISMNTCRTREQ:
            self.active['reqd'] = args['epoch']
        elif event == FSM_EVNT_MNTREQ:
            self.active['reqm'] = args['epoch']
        return (True, None)

    def _cb_recover_to_loading(self, event, args):
        self.flusherror()
        return self._cb_mntreq(event,args)

    def _cb_recover_to_home(self,event, args):
        self.active['d'] = args['epoch']
        self.flusherror()
        return (True, args)

    # - - - -
    def _cb_error_fatal(self, event, args):
        self.flusherror()
        return (True,None)

    def _cb_eject(self, event,args):
        self.data['eject']=args['epoch']
        return (True, None)

    def _cb_eject_error(self,event, args):
        self.flusherror()
        return self._cb_eject(event,args)

    def _cb_voldmfu_to_loading(self,event, args):
        self.flusherror()
        return self._cb_mntreq(event,args)

    def _cb_volmnt_to_loading(self,event, args):
        self.flusherror()
        return self._cb_mntreq(event,args)

    def _cb_loaded_to_loading(self,event, args):
        self.flusherror()
        return self._cb_mntreq(event,args)

    def _cb_mntcmplt(self, event,args):
        self.active['m'] = args['epoch']
        self.active['vol'] = {int(args['epoch']):0}
        self.active['drv'] = args['drive']
        return (True, {'drive':args['drive']})

    def _cb_mntreq(self,event, args):
        self.active['reqm'] = args['epoch']
        return (True, None)

    def _cb_dismnt_crt_cmplt(self,event, args):
        self.active['d'] = args['epoch']
        ret ={'drive':self.active['drv'], 'reqm':self.active['reqm'], 'reqd':self.active['reqd'], 'cid':self.data['id']}
        self.dataappend()
        return (True, ret)

    def _cb_dismnt_crt_req(self,event, args):
        self.active['reqd'] = args['epoch']
        return (True, None)

    def _cb_implicit_crtmnt(self,event, args):
        self.active['reqm'] = args['epoch']
        return (True, None)

    def _cb_implicit_crtmnt_cmplt(self,event, args):
        return self._cb_mntcmplt(event,args)

    def _cb_implicit_crtdism_while_volmnt(self,event, args):
        (ret, a) = self._cb_voldmnt(event,args)
        if ret:
            return self._cb_dismnt_crt_req(event,args)
        raise BaseException("What happened")

    def _cb_unloading_timeout(self,event, args):
        self.flusherror()
        return self._cb_mntreq(event,args)

    def _cb_loaded_to_d2dmv(self,event, args):
        #self.active['d'] = args['epoch']
        #self.active['reqd'] = args['epoch']

        ret = {
            'reqm' : self.active['reqm'],
            #'reqd' : self.active['reqd']
            'reqd' : args['epoch']
        }
        #self.dataappend()
        #self.active['reqm'] = args['epoch']
        self._cb_voldmnt(event,args)
        return (True, ret)

    def _cb_d2dmv_to_loaded(self,event, args):
        self.active['drv'] = args['drive']
        self.active['m'] = args['epoch']
        self.active['reqm'] = args['epoch']
        self.active['vol'] = {args['epoch']:0}
        return (True, args)

## --------------------------------
    def force_last_event_flush(self, epoch):
        if len(self.data['data']) > 0:
            evntdata = self.data['data'].pop()
            e = {
                'm':evntdata[DATA_INDEX_M],
                'd':evntdata[DATA_INDEX_D],
                'reqm':evntdata[DATA_INDEX_REQM],
                'reqd':evntdata[DATA_INDEX_REQD],
                'drv':evntdata[DATA_INDEX_DRV],
                'vol':{}
            }
            for [m,d] in evntdata[DATA_INDEX_VOLUME]:
                #e['vol'].append({m:d})
                e['vol'][int(m)]=int(d)
            self.data['errors'].append(e)
        self.handle_event(FSM_EVNT_FATALERROR_1, {'epoch':epoch})

##-------------------
    def handle_enter(self, epoch):
        self.data['enter'] = epoch
        #self.tmp_state = {'m':0, 'd':0, 'reqM':0, 'reqD':0}

    def handle_eject(self, epoch):
        self.data['eject'] = epoch
        #self.tmp_state = {'m':0, 'd':0, 'reqM':0, 'reqD':0}

    def get_volume(self, slot, ts):
        if slot == 'per_year':
            nextts = datetime.datetime(year=ts.year+1, month=1, day=1, hour=0 ,minute=0)
        if slot == 'per_month':
            m = (ts.month)%12
            y = ts.year
            if (ts.month)/12:
                y += 1
            nextts = datetime.datetime(year=y, month=m+1, day=1)
        if slot == 'per_week':
            nextts = ts + datetime.timedelta(days=7)
        if slot == 'per_day':
            nextts = ts + datetime.timedelta(days=1)
        if slot == 'per_hour':
            nextts = ts + datetime.timedelta(hours=1)

        res = 0
        tsep = unix_time(ts)
        nexttsep = unix_time(nextts)
        index = self.binsearch(self.data['data'],tsep,DATA_INDEX_M)
        while True:
            if len(self.data['data']) > index:
                e = self.data['data'][index]
                for m,d in e[DATA_INDEX_VOLUME]:
                    if m != 0 and d != 0:
                        end = min(nexttsep, d)
                        if m >= tsep and m <= nexttsep:
                            res += d - m
                        elif m > nexttsep:
                            break
                if e[DATA_INDEX_M]> nexttsep:
                    break
                index += 1
            else:
                break
        return res
                    #print m,d

    def get_tbm(self):
        res = []
        lastevent = None
        for event in self.data['data']:
            if lastevent!=None:
                res.append(event[DATA_INDEX_REQM] - lastevent[DATA_INDEX_D])
            lastevent=event
        return res

    def get_latency(self):
        res = []
        for event in self.data['data']:
            lm = event[DATA_INDEX_M] - event[DATA_INDEX_REQM]
            ld = event[DATA_INDEX_D] - event[DATA_INDEX_REQD]
            if ld < 1800:
                res.append(ld)
            if lm < 1800:
                res.append(lm)
        return res

class Drive(BaseStats):
    def __init__(self, id, basedir):
        BaseStats.__init__(self,os.path.join(basedir, "drives"))
        self.data['id']=id
        self.data['data'] =[]    # (dummy, mount epoch, dummy, dismount epoch, cid)
        self.data['cleaning']=[]
        self.fsm.state=FSM_DRV_ERROR

        self.fsm.add_transition(FSM_EVNT_DISMNTCRTCMPLT, FSM_DRV_LOADED, FSM_DRV_EMPTY, self._cb_dmntcmplt)
        self.fsm.add_transition(FSM_EVNT_MNTCMPLT, FSM_DRV_EMPTY, FSM_DRV_LOADED, self._cb_mountcmplt)

        # from error states
        self.fsm.add_transition(FSM_EVNT_D2DMV, FSM_DRV_LOADED, FSM_DRV_ERROR, self._cb_loadedtoerror)
        self.fsm.add_transition(FSM_EVNT_D2DMV, FSM_DRV_ERROR_FATAL, FSM_DRV_ERROR_FATAL, None)
        self.fsm.add_transition(FSM_EVNT_D2DMV, FSM_DRV_MAYBERECOVERED, FSM_DRV_ERROR_FATAL, self._cb_error_fatal)
        self.fsm.add_transition(FSM_EVNT_D2DMV, FSM_DRV_ERROR, FSM_DRV_ERROR, None)
        self.fsm.add_transition(FSM_EVNT_D2DMV, FSM_DRV_EMPTY, FSM_DRV_EMPTY, None)

        self.fsm.add_transition(FSM_EVNT_MNTCMPLT, FSM_DRV_ERROR, FSM_DRV_LOADED, self._cb_errortoloaded)
        self.fsm.add_transition(FSM_EVNT_DISMNTCRTCMPLT, FSM_DRV_ERROR, FSM_DRV_EMPTY, self._cb_errortoempty)
        self.fsm.add_transition(FSM_EVNT_MNTCMPLT, FSM_DRV_LOADED, FSM_DRV_ERROR, self._cb_loadedtoerror)

        # fatal error, admin action going on
        for state in [FSM_DRV_LOADED,FSM_DRV_EMPTY, FSM_DRV_ERROR, FSM_DRV_ERROR_FATAL, FSM_DRV_MAYBERECOVERED]:
            self.fsm.add_transition(FSM_EVNT_FATALERROR_1, state, FSM_DRV_ERROR_FATAL, self._cb_error_fatal)
            self.fsm.add_transition(FSM_EVNT_DELDRIVE, state, FSM_DRV_ERROR_FATAL,self._cb_error_fatal)

        self.fsm.add_transition(FSM_EVNT_DISMNTCRTCMPLT, FSM_DRV_ERROR_FATAL, FSM_DRV_ERROR_FATAL, self._cb_errortoempty)
        self.fsm.add_transition(FSM_EVNT_MNTCMPLT, FSM_DRV_ERROR_FATAL, FSM_DRV_MAYBERECOVERED, self._cb_errortoloaded)

        self.fsm.add_transition(FSM_EVNT_DISMNTCRTCMPLT, FSM_DRV_MAYBERECOVERED, FSM_DRV_EMPTY, self._cb_dmntcmplt)
        self.fsm.add_transition(FSM_EVNT_MNTCMPLT, FSM_DRV_MAYBERECOVERED, FSM_DRV_ERROR_FATAL, self._cb_error_fatal)


        self.fsm.add_transition(FSM_EVNT_ROB1, FSM_DRV_EMPTY,FSM_DRV_EMPTY, None)
        self.fsm.add_transition(FSM_EVNT_ROB1, FSM_DRV_LOADED,FSM_DRV_ERROR, self._cb_error_fatal)
        self.fsm.add_transition(FSM_EVNT_ROB1, FSM_DRV_ERROR_FATAL,FSM_DRV_ERROR_FATAL, None)

        self.fsm.add_transition(FSM_EVNT_DISMNTCRTCMPLT, FSM_DRV_EMPTY, FSM_DRV_EMPTY, None)

        # probably initial drive start
        self.fsm.add_transition(FSM_EVNT_RECOVER_FAT1, FSM_DRV_ERROR, FSM_DRV_EMPTY, self._cb_recover_fatal1)
        self.fsm.add_transition(FSM_EVNT_RECOVER_FAT1, FSM_DRV_LOADED, FSM_DRV_EMPTY, self._cb_recover_fatal1)
        self.fsm.add_transition(FSM_EVNT_RECOVER_FAT1, FSM_DRV_EMPTY,FSM_DRV_EMPTY, self._cb_double_admin_enable)
        self.fsm.add_transition(FSM_EVNT_RECOVER_FAT1, FSM_DRV_ERROR_FATAL,FSM_DRV_EMPTY, self._cb_recover_fatal1)
        self.fsm.add_transition(FSM_EVNT_RECOVER_FAT1,FSM_DRV_MAYBERECOVERED,FSM_DRV_EMPTY, self._cb_recover_fatal1)

    def _transform_dictentry(self, entry):
        return [entry['reqm'], entry['m'], 0, entry['reqd'], entry['d'], entry['mh'],entry['dh'], entry['drv']]

    def handle_event(self, event, args):
        return self.fsm.handle_event(event, args)

    def _cb_error_fatal(self, event, args):
        self.flusherror()
        return (True,None)

    def _cb_double_admin_enable(self, event, args):
        self._cb_error_fatal(event,args)
        return (True, None)

    def _cb_mountcmplt(self,event,args):
        self.active['m'] = args['epoch']
        self.active['drv'] = args['cid']
        return (True, None)

    def _cb_dmntcmplt(self,event, args):
        if args['cid'] != self.active['drv']:
            #print "invalid dismount received"
            return (False, {'cid':args['cid']})
        else:
#            if not 'reqd' in args.keys():
            self.active['reqd'] = args['reqd']
            self.active['reqm'] = args['reqm']
            self.active['d'] = args['epoch']
            self.dataappend()
            return (True, None)

    def _cb_loadedtoerror(self,event, args):
        self.flusherror()
        self.fsm.subsequent_events.append([FSM_EVNT_MNTCMPLT, args])
        return (True, None)

    def _cb_errortoloaded(self, event,args):
        self.flusherror()
        return self._cb_mountcmplt(event,args)

    def _cb_errortoempty(self,event, args):
        self.flusherror()
        return (True, None)

    def _cb_recover_fatal1(self,event, args):
        self.flusherror()
        return (True, None)

    def get_latencies(self):
        res = []
        def _add(start, end, home, cid, op):
            diff = end-start
            if 0<=diff and diff <= 3600:
                res.append((end, diff, home, cid, op))
        for (reqm, m, dmvol, reqd, d, mh, dh, cid) in self.data['data']:
            _add(reqm, m, mh, cid, HOME_OPERATION_EJECT) # home to drive
            _add(reqd, d, dh, cid, HOME_OPERATION_INJECT)# drive to home
        return res

    def data_import(self, reqm, m, reqd, d, mh, dh, cid):
        ent = (reqm,m,None,reqd,d,mh,dh,cid)
        for tmp in self.data['data']:
            if m > 0:
                if tmp[1] > m:
                    index = self.data['data'].index(tmp)
                    self.data['data'].insert(index, ent)
                    return
            if reqm > 0:
                if tmp[0] > reqm:
                    index = self.data['data'].index(tmp)
                    self.data['data'].insert(index, ent)
                    return
        if m == 0:
            self.data['errors'].append({'reqm':reqm,'m':m,'reqd':reqd,'d':d,'mh':mh,'dh':dh,'drv':cid, 'vol':0 })
        else:
            self.data['data'].append(ent)

    def handle_disable(self, epoch):
        self.data['disabled'].append([epoch,0])
        self.flusherror()
        #self.state = DRIVE_STATE_DISABLED

    def handle_enable(self, epoch):
        x = self.data['disabled'].pop()
        print x
        if x[1] != 0:
            print "wtf drive enable"
        else:
            x[1]=epoch
            self.data['disabled'].append(x)
        self.state = DRIVE_STATE_IDLE

    def handle_cleaning_crt_dismnt(self, epoch):
        self.data['cleaning'].append(epoch)

    def estimate_cleaning_time(self, start=None, end=None):
        t = []
        for cln in self.data['cleaning']:
            if start < cln and cln <= end:
                entry = self.get_predecessor_of_entry(cln, DATA_INDEX_D)
                if entry:
                    t.append(cln-entry[DATA_INDEX_D])
            if cln > end:
                break
        return t

class Home:
    def __init__(self, hid, basedir):
        self.basedir = os.path.join(basedir, "homes")
        if not os.path.exists(self.basedir):
            os.makedirs(self.basedir)
        self.stats_calculated=False
        self.data = {
            'id' : str(hid),
            'data' : [],
            'perdrive' : {}  # entries: driveid= (numops, avglatency)
        }

    def __repr__(self):
        return self.data['id']

    def handle_event(self, epoch, latency, drive, cid, optype):
    #    if optype == HOME_OPERATION_EJECT:
    #        self.eject_event(epoch, latency, drive, cid)
    #    elif optype == HOME_OPERATION_INJECT:
    #        self.inject_event(epoch, latency, drive, cid)
        self.data['data'].append((epoch, latency, drive, cid, optype))

    def get_stats(self, atts):
        if not self.stats_calculated:
            self.stats()
        res = {}
        for i in atts:
            res[i] = self.data.get(i, 0)
        return res

    def stats(self):
        _drivestats = {}
        for (epoch, latency, drive, cid, optype) in self.data['data']:
            obj = _drivestats.setdefault(drive, [])
            obj.append(latency)

        tmp = []
        l = 0
        for drive, val in _drivestats.items():
            t = numpy.mean(val)
            tmp.append(t)
            l += len(val)
            self.data['perdrive'][drive] = (len(val),t)
        self.data[LATENCY_HOME_AVG] = numpy.mean(tmp)
        self.data[HOME_TOTAL_OPS] = l

        with open(os.path.join(self.basedir, str(self)), 'w') as csv_file:
            lineBuf = StringIO.StringIO()
            lineBuf.write("drive;%s;%s"%(HOME_TOTAL_OPS, LATENCY_HOME_AVG))
            for d,(num,lat) in self.data['perdrive'].items():
                linebuf_write(lineBuf, "\n%s;%s;%s"%(d,num,lat))
            csv_file.write(lineBuf.getvalue())
            csv_file.close()
        self.stats_calculated=True

    def cost_drive(self, drv=None):
        if not drv:
            return self.data['perdrive']
        res = self.data['perdrive'].get(drv, None)
        if not res:
            return self.data[LATENCY_HOME_AVG]
        else:
            return res[1]

class God:
    def __init__(self, basedir):
        self.basedir = basedir
        self.outputdir = os.path.join(basedir, "stats")
        self.whpssdir = os.path.join(basedir, "../whpss")
        self.robotdir = os.path.join(self.basedir, '../robot/robot_mounts*.gz')
        #self._perdrive_outputdir = os.path.join(self.outputdir, 'drives')
        #self._percartridge_outputdir = os.path.join(self.outputdir, 'cartridges')

        self._global_crt_csv = os.path.join(self.outputdir ,'global_crt.csv')
        self._global_drv_csv = os.path.join(self.outputdir ,'global_drv.csv')
        self._global_hm_csv = os.path.join(self.outputdir ,'global_hm.csv')
        self._json_dump_robot = os.path.join(basedir, "robot_classes")

        self.crt = {}
        self.drv = {}
        self.hm = {}
        self.robot_totalcnt = 0
        self.robot_errorcnt_crt = 0
        self.robot_errorcnt_drv = 0

        for i in [self.outputdir]:
            if not os.path.exists(i):
                os.makedirs(i)

    def _get_hm(self, hid, create=True):
        if hid not in self.hm and create:
            self.hm[hid] = Home(hid, self.outputdir)
        return self.hm.get(hid,None)

    def _get_crt(self, cid, create=True):
        if cid not in self.crt and create:
            self.crt[cid] = Cartridge(cid, self.outputdir)
        return self.crt.get(cid,None)

    def _get_drv(self, drv, create=True):
        if drv not in self.drv and create:
            self.drv[drv] = Drive(drv, self.outputdir)
        return self.drv.get(drv,None)

    def handle_event(self, evnt, arguments):
        if 'cid' in arguments.keys():
            cartridge_id = arguments['cid']
            if cartridge_id[:2] in crtfilter_b or cartridge_id[:1] in crtfilter_a:
                # filter cartridges
                return

            crt = self._get_crt(cartridge_id )
            if evnt == None:
                pass #dummy
            else:
                a = crt.handle_event(evnt,arguments)
                if a:
                    for k,v in a.items():
                        arguments[k]=v
        #else:
        #    print "no cid... what the fuck"
        if 'drive' in arguments.keys():
            if len(arguments['drive'])>0:
                #if arguments['drive'][0] not in ['4', '3']:
                if len(arguments['drive'])  <= 6:
                    drv = self._get_drv(arguments['drive'])
                    if evnt == None:
                        pass #dummy
                    else:
                        ret = drv.handle_event(evnt, arguments)
                        if ret:
                            if 'cid' in ret.keys():
                                # error in cids last event
                                self.force_last_event_flush(cid=ret['cid'], epoch=arguments['epoch'])

        else:
            "no drive provided, or needed"

    def force_last_event_flush(self, cid, epoch):
        crt = self._get_crt(cid, False)
        if crt:
            crt.force_last_event_flush(epoch)

    def robot_dismount(self, cid, epoch, drive, library):
        self.robot_totalcnt+=1
        crt = self._get_crt(cid, False)
        if crt:
            if not crt.robot_dismount(epoch, drive, library):
                #print "error robot dismount"
                self.robot_errorcnt_crt+=1
        else:
            pass
            #print "Unknown crt",cid, epoch
        drv = self._get_drv(drive , False)
        if drv:
            if cid[:3] != 'CLN':
                if not drv.robot_dismount(epoch, cid, library):
                    self.robot_errorcnt_drv+=1
            else:
                drv.handle_cleaning_crt_dismnt(epoch)

    def robot_mount(self,cid, epoch, drive, library):
        self.robot_totalcnt+=1
        crt = self._get_crt(cid, False)
        if crt:
            if not crt.robot_mount(epoch, drive, library):
                #print "error robot mount"
                self.robot_errorcnt_crt+=1
        else:
            pass
            #print "Unknown crt",cid, epoch

        drv = self._get_drv(drive , False)
        if drv:
            if not drv.robot_mount(epoch, cid, library):
                self.robot_errorcnt_drv+=1

    def collect_recovered_errors(self):
        recovered = 0
        remaining = 0
        for cid,crt in self.crt.items():
            a,b = crt.collect_recovered_errors()
            recovered += a
            remaining += b
        for did,drv in self.drv.items():
            a,b = drv.collect_recovered_errors()
            recovered += a
            remaining += b
        print "Recovered:%i, Remaining:%i"%(recovered, remaining)

    def handle_warning(self, epoch, drive, cid):
        crt = self._get_crt(cid, False)
        if crt:
            crt.flushactive()

    def jsonload(self, skipdrives=False):
        jsondir = os.path.join(self.basedir, 'json')
        if not skipdrives:
            drvf = "%s_drv.json"%self._json_dump_robot
            if os.path.isfile(drvf):
                with open(drvf, 'r') as f:
                    #print "Reading %s"%drvf
                    for e in json.load(f):
                        obj = Drive(e['id'], self.outputdir)
                        obj.data = e
                        self.drv[e['id']]=obj

        all_crt_files = sorted(glob.glob(os.path.join(jsondir, 'crt_*.json')))
        for crtf in all_crt_files:
            with open(crtf, 'r') as f:
                #print "Reading %s"%crtf
                e = json.load(f)
                obj = Cartridge(e['id'], self.outputdir)
                obj.data = e
                self.crt[e['id']]=obj

        hmf = "%s_hm.json"%self._json_dump_robot
        if os.path.isfile(hmf):
            with open(hmf, 'r') as f:
                #print "Reading %s"%hmf
                for e in json.load(f):
                    obj = Home(e['id'], self.outputdir)
                    obj.data = e
                    self.hm[e['id']]=obj

    def jsondump(self):
        def _dmp(file, data):
            with open(file, 'w') as f:
                json.dump(data, f, indent=1)

        jsondir = os.path.join(self.basedir, "json")
        if not os.path.isdir(jsondir):
            os.makedirs(jsondir)
        drv = []
        hm = []

        for obj in self.crt.values():
            fn = os.path.join(jsondir,"crt_%s.json"%obj.data['id'])
            _dmp(fn, obj.data)
        for obj in self.drv.values():
            drv.append(obj.data)
        for obj in self.hm.values():
            hm.append(obj.data)

        _dmp(os.path.join(jsondir,"%s_drv.json"%self._json_dump_robot), drv)
        _dmp(os.path.join(jsondir,"%s_hm.json"%self._json_dump_robot), hm)

    def derive_homes(self):
        #for cid, crt in self.crt.items():
        #    for op in crt.data['data']:
        #        if len(op[DATA_INDEX_DRV]) > 0:
        #            drv = self._get_drv(op[DATA_INDEX_DRV], True)
        #            reqm = op[DATA_INDEX_REQM]
        #            m = op[DATA_INDEX_M]
        #            reqd = op[DATA_INDEX_REQD]
        #            d = op[DATA_INDEX_D]
        #            dh = op[DATA_INDEX_DH]
        #            mh = op[DATA_INDEX_MH]
        #            drv.data_import(reqm, m, reqd, d, mh, dh, cid)
        for id,drv in self.drv.items():
            for (epoch, latency, home, cid, optype) in drv.get_latencies():
                if home and len(home) >3:
                    hobj = self._get_hm(home, True)
                    hobj.handle_event(epoch, latency, id, cid, optype)

    def stats(self):
        if FULLRUN or 1:
            ### generate global cartridge statistics
            with open(self._global_crt_csv, 'w') as csv_file:
                atts = ['id']
                atts.extend(GLOBAL_CRT)
                special = []
                special.extend(SPECIAL_CRT)

                sortres = []
                lineBuf = StringIO.StringIO()
                cnt=1
                for id in sorted(self.crt.keys()):
                    obj = self.crt.get(id)
                    res = obj.stats(atts,special)
                    if lineBuf.len==0:
                        lineBuf.write("index")
                        sortres = sorted(res.keys())
                        for k in sortres:
                            lineBuf.write(";%s"%k)

                    lineBuf.write("\n%i"%cnt)
                    for k in sortres:
                        linebuf_write(lineBuf, res.get(k,0))
                    #lineBuf.write("\n")
                    cnt+=1
                csv_file.write(lineBuf.getvalue())
                csv_file.close()
                lineBuf.flush()

        if FULLRUN or 1:
            ### generate global cartridge statistics
            with open(self._global_hm_csv, 'w') as csv_file:
                sortres = []
                lineBuf = StringIO.StringIO()
                cnt=1
                for id in sorted(self.hm.keys()):
                    obj = self.hm.get(id)
                    res = obj.get_stats(GLOBAL_HM)
                    if lineBuf.len==0:
                        lineBuf.write("id;index")
                        sortres = sorted(res.keys())
                        for k in sortres:
                            lineBuf.write(";%s"%k)
                        lineBuf.write("\n")
                    lineBuf.write("%s;%i"%(id,cnt))
                    for k in sortres:
                        linebuf_write(lineBuf, res.get(k,0))
                    lineBuf.write("\n")
                    cnt+=1
                csv_file.write(lineBuf.getvalue())
                csv_file.close()
                lineBuf.flush()

        if FULLRUN or 1:
            ### generate lobal drive statistics
            with open(self._global_drv_csv, 'w') as csv_file:
                atts = ['id']
                atts.extend(GLOBAL_DRV)
                special = []
                special.extend(SPECIAL_DRV)

                lineBuf = StringIO.StringIO()
                sortres = []
                cnt=1
                for id in sorted(self.drv.keys()):
                    obj = self.drv.get(id)
                    res = obj.stats(atts,special)
                    if lineBuf.len==0:
                        lineBuf.write("id;index")
                        sortres = sorted(res.keys())
                        for k in sortres:
                            lineBuf.write(";%s"%k)
                        lineBuf.write("\n")
                    lineBuf.write("%s;%i"%(id,cnt))
                    for k in sortres:
                        linebuf_write(lineBuf,res.get(k,0))
                    lineBuf.write("\n")
                    cnt+=1
                csv_file.write(lineBuf.getvalue())
                csv_file.close()
                lineBuf.flush()

        ### generate per timeslot statistics
        if FULLRUN or 0: # stopit
            tup = []
            tup.append((self.crt,'cartridges'))
            #tup.append((self.drv,'drives'))
            for (dataref,stringref) in tup:
                for key in ['per_hour','per_day','per_week','per_month', 'per_year']:
                    for opt in [TOTAL_MNT_TIME, TOTAL_MNTS, HOTNESS]:
                        f = os.path.join(self.outputdir, '%s_%s_%s.csv'%(stringref, opt, key))
                        ids = []
                        with open(f, 'w') as csv_file:
                            d = {}
                            for id, obj in sorted(dataref.items()):
                                if id not in ids:
                                    ids.append(id)
                                res = obj.pertime(obj.data['data'])
                                for ts,data in res[key].items():
                                    if ts not in d.keys():
                                        d[ts] = {}
                                    d[ts][id] = data.get(opt,0)
                            lineBuf = StringIO.StringIO()
                            lineBuf.write("timestamp")
                            for id in ids:
                                lineBuf.write(";%s"%id)
                            for ts in sorted(d.keys()):
                                lineBuf.write('\n%s'%ts)
                                for id in ids:
                                    linebuf_write(lineBuf,d[ts].get(id,0))
                            csv_file.write(lineBuf.getvalue())
                            csv_file.close()
                            lineBuf.flush()


        if FULLRUN or 0:
            for id,hobj in self.hm.items():
                hobj.stats()

    def robot_read(self):
        def _handle_file(filelist, obj):
            for filename in filelist:
                with gzip.open(filename, 'r') as source_file:
                    for line in source_file:
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
                                obj.robot_mount(cartridge_id, epoch, drive, library_pos)
                                continue

                            if action == "DISMOUNT":  # be aware of cleaning cartridges
                                obj.robot_dismount(cartridge_id, epoch, drive, library_pos)
                                continue

                         #   elif action == "ENTER":
                         #       self.handle_enter(cartridge_id, epoch)

                         #   elif action == "EJECT":
                         #       self.handle_eject(cartridge_id, epoch)

                         #   elif action == 'ACSMV':
                         #       self.handle_move(epoch, library_pos, drive)
                        if action == 'ACSCR':
                            if line.__contains__('STATUS_VOLUME_NOT_FOUND'):
                                continue

                        print 'unparsed' ,line,


        re_line = re.compile(".*([0-9]{8}:[0-9]{6}).* (ACSMV|ACSCR|AUDIT|EJECT|ENTER|MOUNT|DISMOUNT) ([0-9a-zA-Z]{6}) Home ([0-9,]*) ([a-zA-Z\s]+) ([0-9,]*) .*")
        re_line_not_found = re.compile(".*([0-9]{8}:[0-9]{6}).* (ACSCR|AUDIT) ([0-9a-zA-Z]{6}).* (STATUS_VOLUME_NOT_FOUND) .*")

        numthrads = 2
        threadfiles  = {}
        for i in range(numthrads):
            threadfiles[i]= []

        all_log_files = sorted(glob.glob(self.robotdir))

        #for i in all_log_files:
         #   index = all_log_files.index(i)%numthrads
          #  threadfiles[index].append(i)

        #threads = []
        #for i in range(numthrads):
        #    t1 = threading.Thread(target=_handle_file, args=(threadfiles[i],self))
         #   threads.append(t1)
          #  t1.start()
        #for i in threads:
         #   i.run()

    #    alive = 0
        if 1:
          for filename in all_log_files:
            print filename
            with Timer("Finished:  %s:" % (filename)):
                with gzip.open(filename, 'r') as source_file:
                    for line in source_file:
#                        alive +=1
                        match = re_line.search(line)
 #                       if not alive%1000:
  #                          print line,
   #                         alive=0
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
                                self.robot_mount(cartridge_id, epoch, drive, library_pos)
                                continue

                            if action == "DISMOUNT":  # be aware of cleaning cartridges
                                self.robot_dismount(cartridge_id, epoch, drive, library_pos)
                                continue

                         #   elif action == "ENTER":
                         #       self.handle_enter(cartridge_id, epoch)

                         #   elif action == "EJECT":
                         #       self.handle_eject(cartridge_id, epoch)

                         #   elif action == 'ACSMV':
                         #       self.handle_move(epoch, library_pos, drive)
                        if action == 'ACSCR':
                            if line.__contains__('STATUS_VOLUME_NOT_FOUND'):
                                continue

                        print 'unparsed' ,line,
        print 'total', self.robot_totalcnt, 'error', self.robot_errorcnt_crt, self.robot_errorcnt_drv

    def whpss_read(self):
        re_generic_crt = re.compile(".* cartridge\s*=\s*\"([0-9A-Z]+)\".*")
        re_generic_drv = re.compile(".* drive = \"([0-9]+)\".*")
        # 05/26 00:15:46 ***
        re_time = re.compile("([0-9]{2})/([0-9]{2}) ([0-9]{2}):([0-9]{2}):([0-9]{2}) ([A-Z]+) .*")
        re_filename_date = re.compile("([0-9]{4})([0-9]{2})([0-9]{2})_([0-9]{2})([0-9]{2})([0-9]{2}).gz")

        re_crtanddrv = re.compile(".* cartridge = \"([0-9A-Z]+)\", drive = \"([0-9]+)\".*")

        # 03/12 20:53:36 RQST PVRS0004 Entering pvr_Mount, cartridge = "CC0968", drive = "0"
        re_pvr_Mount = re.compile(".* pvr_Mount,.*cartridge = \"([0-9A-Z]+)\".*")

        re_pvl_Mount = re.compile(".* \"pvl_MountAdd\", .* arg = \"([0-9A-Z]+)\".*")

        # 03/07 17:51:11 DBUG PVRS0379 STK Request:   acs_mount: seq= 23557, cart= WB2125
        re_acs_mount = re.compile(".* cart= ([0-9A-Z]+)")

        # 05/26 00:15:46 RQST PVLS0002 Exiting, function = "pvl_MountCompleted", jobid = "11644979", drive = "101101", arg = "WB3134"
        re_pvl_MountCompleted = re.compile(".* jobid = \"([0-9]+)\".* drive = \"([0-9]+)\", arg = \"([0-9A-Z]+)\".*")
        re_pvl_MountCompleted2 = re.compile(".* jobid = \"([0-9]+)\".* arg = \"([0-9A-Z]+)\"")

        re_warning = re.compile(".* jobid = \"([0-9]+)\".* arg = \"([0-9A-Z]+)\"")
        re_warn_d1 = re.compile(".* Error reading device ([0-9]+).* vol ([A-Z0-9]+).*")

        re_warn_drive = re.compile(".* Drive disabled, drive = \"(([0-9]+))\".*")
        re_warn_c = re.compile(".*Could not mount volume\: ([A-Z0-9]+)\:.*")

        re_evnt_drive_enabl = re.compile(".* Drv= ([0-9]+), Enabled.*")

        re_alrm_a = re.compile(".*Dismount ([A-Z0-9]+) pends.*")

        re_pvr_DismountCart = re.compile(".*cartridge = \"([0-9A-Z]+)\".*")
        re_pvl_DismountVol =  re.compile(".*arg\s*=\s*\"([0-9A-Z]+)\".*")

        # 03/01 08:28:11 RQST PVRS0012 Entering pvr_Inject, cartridge = "RC2773", drive = "0"
        re_pvr_Inject = re.compile(". cartridge = \"([0-9A-Z]+)\"")

        # 03/01 08:23:37 EVNT PVRS0043 Ejecting cartridge="P54892", manufacturer="IBM LTO3-1", lot="Jul09", began service Tue Aug 25 12:44:27 2009, last maintained Thu Jan  1 00:00:00 1970, last mounted Tue Feb 26 22:10:33 2013, total mounts=12, mounts since last maintained=12
        re_pvr_Eject = re.compile(".* cartridge = \"([0-9A-Z]+)\".*")

        # 01/02 18:34:59 MINR PVRS0141 Robot unable to find cartridge, cartridge = "CB5564", drive = "1,3,1,3", drive = "0"
        re_minr_lost_cartridge = re.compile(".* MINR .* Robot unable to find .* cartridge = \"([0-9A-Z]+)\", drive = \"([,0-9]+)\", .*")
        re_minr_lost_cartridge2 = re.compile(".* MINR .* Robot unable to find .* cartridge = \"([0-9A-Z]+)\", drive = \"0\".*")
        re_minr_drv_stuck = re.compile('.* drive in use or locked by .* drive = \"([0-9,]+)\", drive .*')

        # nunn lines
        re_nunn_importerr = re.compile(".*Import of cartridge '([A-Z0-9]+)' failed.*")

        last_epoch=0
        log_creation_date = None
        files = sorted(glob.glob(os.path.join(self.whpssdir,'whpss_log_*.gz')))
        for f in files:
            m = re.search(re_filename_date, os.path.basename(f))
            if not m:
                print ("ERROR, cannot process invalid file name: %s" % (f))
                return
            x = m.groups()
            if not log_creation_date:
                log_creation_date = datetime.datetime(int(x[0]), int(x[1]),int(x[2]),int(x[3]),int(x[4]),int(x[5]))

            print "--------------------------------------------------------------------------------------------------"
            print "Reading file %s.\n"%f
            with gzip.open(f, 'r') as source:
                for line in source.readlines():
                    try:
                        time_match = re_time.search(line)
                        if time_match:
                            x = time_match.groups()
                            log_entry_date = datetime.datetime(log_creation_date.year, int(x[0]), int(x[1]),int(x[2]),int(x[3]),int(x[4]))
                            epoch = int(calendar.timegm(log_entry_date.utctimetuple()))

                            if epoch >= last_epoch:
                                # chronologic order
                                last_epoch = epoch
                            else:
                                print "last epoch", last_epoch
                                print 'current epoch', epoch, x
                                if int(x[0])==1 and int(x[1])==1:
                                    log_creation_date = datetime.datetime(log_creation_date.year+1, int(x[0]), int(x[1]),int(x[2]),int(x[3]),int(x[4]))
                                    log_entry_date = datetime.datetime(log_creation_date.year, int(x[0]), int(x[1]),int(x[2]),int(x[3]),int(x[4]))
                                    epoch = int(calendar.timegm(log_entry_date.utctimetuple()))
                                print 'fixed epoch', epoch
                                #print "Hard abort due to chronologic error in line: \n\t%s" % (line)
                                #sys.exit(1)
                                #raw_input("Is this ok?")

                            if line.__contains__(" DBUG "):
                                continue

                            if line.__contains__(' RQST '):
                                if line.__contains__('Entering'):
                                    if line.__contains__("pvl_MountAdd"): # ok #
                                        match = re_pvl_Mount.search(line)
                                        if match:
                                            cartridge_id = match.groups()[0][:6]
                                            if cartridge_id[:2] not in crtfilter_b and cartridge_id[:1] not in crtfilter_a:
                                                self.handle_event(FSM_EVNT_VOLMNT, {'cid':cartridge_id, 'epoch':epoch})
                                            continue
                                    if line.__contains__('pvr_Mount'):
                                        match2 = re_pvr_Mount.search(line)
                                        if match2:
                                            cartridge_id = match2.groups()[0][:6]
                                            if cartridge_id[:2] not in crtfilter_b and cartridge_id[:1] not in crtfilter_a:
                                                self.handle_event(FSM_EVNT_MNTREQ, {'cid':cartridge_id, 'epoch':epoch})
                                            continue
                                    if line.__contains__("pvr_DismountCart"):
                                        match = re_pvr_DismountCart.search(line)
                                        if match:
                                            cartridge_id = match.groups()[0][:6]
                                            if cartridge_id[:2] not in crtfilter_b and cartridge_id[:1] not in crtfilter_a:
                                                self.handle_event(FSM_EVNT_DISMNTCRTREQ, {'cid':cartridge_id, 'epoch':epoch})
                                            continue
                                    if line.__contains__("pvl_DismountVolume"):
                                        match = re_pvl_DismountVol.search(line)
                                        if match:
                                            cartridge_id = match.groups()[0][:6]
                                            if cartridge_id[:2] not in crtfilter_b and cartridge_id[:1] not in crtfilter_a:
                                                self.handle_event(FSM_EVNT_VOLDMNT, {'cid':cartridge_id, 'epoch':epoch})
                                            continue
                                    if line.__contains__("pvr_Inject"):
                                        # a new cartridge is added, add it to list of injected_cartridges
                                        match = re_pvr_Inject.search(line)
                                        if match:
                                            self.handle_event(FSM_EVNT_INJECT, {'epoch': epoch, 'cid':match.groups()[0][:6]})
                                            continue

                                    if line.__contains__("pvr_Eject"):
                                        match = re_pvr_Eject.search(line)
                                        if match:
                                            self.handle_event(FSM_EVNT_EJECT, {'cid':match.groups()[0][:6], 'epoch':epoch})
                                            continue

                                if line.__contains__("Exiting"):
                                    # can happen multiple times within 'cartridge_mount_process' last seen pvl_MountCompleted is assumed to be successfull.
                                    if line.__contains__("pvl_MountCompleted"):
                                        match = re_pvl_MountCompleted.search(line)
                                        if match:
                                            job_id = match.groups()[0]
                                            drive = match.groups()[1]
                                            cartridge_id = match.groups()[2][:6]
                                            if cartridge_id[:2] not in crtfilter_b and cartridge_id[:1] not in crtfilter_a:
                                                self.handle_event(FSM_EVNT_MNTCMPLT, {'cid':cartridge_id, 'epoch':epoch, 'drive':drive})
                                            continue

                                        match2 = re_pvl_MountCompleted2.search(line)
                                        if match2:
                                            job_id = match2.groups()[0]
                                            cartridge_id = match2.groups()[1][:6]
                                            #print "bad mount: maybe ejected from library. check CB5564 2.jan 19.06.17 entry, jobid:", job_id, cartridge_id
                                            self.handle_event(FSM_EVNT_FATALERROR_1, {'epoch':epoch, 'cid':cartridge_id})
                                            continue

                                    if line.__contains__("pvr_DismountCart"):
                                        match = re_pvr_DismountCart.search(line)
                                        if match:
                                            cartridge_id = match.groups()[0][:6]
                                            if cartridge_id[:2] not in crtfilter_b and cartridge_id[:1] not in crtfilter_a:
                                                self.handle_event(FSM_EVNT_DISMNTCRTCMPLT, {'cid':cartridge_id, 'epoch':epoch})
                                            continue

                                    if line.__contains__("pvl_DeleteDrive"):
                                        m = re_generic_drv.match(line)
                                        if m:
                                            self.handle_event(FSM_EVNT_DELDRIVE, {'drive':m.groups()[0], 'epoch':epoch})
                                            continue

                                if line.__contains__("Entering"):
                                    if line.__contains__("pvl_MountCompleted") or \
                                            line.__contains__("pvl_DeleteDrive"):
                                        continue
                                if line.__contains__("Exiting"):
                                    if line.__contains__('pvl_MountAdd') or \
                                        line.__contains__('pvr_Mount') or \
                                        line.__contains__('pvl_DismountVolume') or \
                                        line.__contains__('pvr_Eject') or \
                                        line.__contains__('pvr_Inject') :
                                            continue

                                docont = False
                                for x in ['pvl_RequestSetAttrs', 'pvl_RequestGetAttrs','pvl_MountNew',
                                          'pvr_CartridgeGetAttrs', 'pvr_CartridgeSetAttrs', 'pvr_MountComplete',
                                          'pvl_MountCommit',"pvl_QueueGetAttrs","pvl_QueueSetAttrs",'pvl_DriveSetAttrs',
                                        'pvl_VolumeGetAttrs', 'pvl_VolumeSetAttrs', 'pvl_PVLSetAttrs',
                                        "pvr_ServerSetAttrs",'pvl_ServerSetAttrs', 'pvl_DismountJobId',
                                        'pvl_AllocateVol', 'pvl_DeallocateVol', 'pvl_Import', 'pvl_Export',
                                        'pvl_CheckInCompleted', 'pvl_DriveGetAttrs',
                                        'pvl_CreateDrive','pvl_Terminate', 'pvl_Move',
                                        'pvl_CancelAllJobs']:
                                    if line.__contains__(x):
                                        docont = True
                                        break
                                if docont:
                                    continue

                            if line.__contains__(' MINR '):
                                m = re_minr_lost_cartridge.search(line)
                                if m:
                                    cartridge_id = m.groups()[0][:6]
                                    drive = m.groups()[1]
                                    rawdrive = string.split(drive, ',')
                                    if len(rawdrive)>3:
                                        drive = "%i%02i%i%02i"%(int(rawdrive[0]),int(rawdrive[1]),int(rawdrive[2]),int(rawdrive[3]))
                                    self.handle_event(FSM_EVNT_ROB1, {'epoch':epoch, 'cid':cartridge_id, 'drive':drive})
                                    continue

                                m = re_minr_lost_cartridge2.match(line)
                                if m:
                                    cartridge_id = m.groups()[0][:6]
                                    self.handle_event(FSM_EVNT_ROB1, {'epoch':epoch, 'cid':cartridge_id})
                                    continue

                                m = re_minr_drv_stuck.match(line)
                                if m:
                                    drive = m.groups()[0]
                                    rawdrive = string.split(drive, ',')
                                    if len(rawdrive)>3:
                                        drive = "%i%02i%i%02i"%(int(rawdrive[0]),int(rawdrive[1]),int(rawdrive[2]),int(rawdrive[3]))
                                    self.handle_event(FSM_EVNT_FATALERROR_1, {'epoch':epoch, 'drive':drive})
                                    continue

                                if line.__contains__("Intervention necessary"):
                                    m = re_generic_crt.match(line)
                                    if m:
                                        cartridge_id = m.groups()[0]
                                        self.handle_event(FSM_EVNT_FATALERROR_1, {'epoch':epoch, 'cid':cartridge_id})
                                        continue

                                if line.__contains__('Dismount failed due to STK IN_TRANSIT status'):
                                    m = re_generic_crt.match(line)
                                    if m:
                                        self.handle_event(FSM_EVNT_FATALERROR_1, {'epoch':epoch, 'cid':m.groups()[0]})
                                        continue

                                if line.__contains__("Could not mount volume"):
                                    m =re_warn_c.match(line)
                                    if m:
                                        cr = m.groups()[0][:6]
                                        self.handle_event(FSM_EVNT_FATALERROR_1,{'epoch':epoch, 'cid':cr})
                                        continue

                                if line.__contains__("Drive Disabled"):
                                    m = re_pvl_MountCompleted.match(line) # to extract vars
                                    if m:
                                        j, drv, crt = m.groups()
                                        self.handle_event(FSM_EVNT_FATALERROR_1, {'epoch':epoch, 'cid':crt[:6], 'drive':drv})
                                        continue

                                if line.__contains__("Not owner") or \
                                        line.__contains__(" Rewind of device 40") or \
                                        line.__contains__("Stage failed, all retries exhausted") or \
                                        line.__contains__("Request for locked or disabled device") or \
                                        line.__contains__('Unexpected error in LTO Library') or \
                                        line.__contains__('LTO I/O failure') or \
                                        line.__contains__('"sendIOD"') or \
                                        line.__contains__('hpss_RPCGetReply') or \
                                        line.__contains__("gk_Cleanup") or \
                                        line.__contains__("gk_Close failed") or \
                                        line.__contains__('Invalid parameters passed to LTO Library') or \
                                        line.__contains__('Open of device 4') or \
                                        line.__contains__('pos failed on dev 4') or \
                                        line.__contains__('Retrying stage from level') or \
                                        line.__contains__('Cartridge not found in LTO library') or \
                                        line.__contains__("Read of label on") or \
                                        line.__contains__('Can not find the PVL') or \
                                        line.__contains__('all retries exhausted') or \
                                        line.__contains__('locked by non-HPSS') or \
                                        line.__contains__('SCSI ') or \
                                        line.__contains__('hpss_RPCSendRequest') or \
                                        line.__contains__('Forward space file failed') or \
                                        line.__contains__('Verification of label on dev') or \
                                        line.__contains__('Open of device') or \
                                        line.__contains__('No space left on device') or \
                                        line.__contains__('Metadata manager error') or \
                                        line.__contains__('Connection refused') or \
                                        line.__contains__('Connection timed out') or \
                                        line.__contains__('ACSLM spawned process') or \
                                        line.__contains__('Cannot Establish Connection') or \
                                        line.__contains__('VV metadata') or \
                                        line.__contains__('Open of delog') or \
                                        line.__contains__('database deadlock condition') or \
                                        line.__contains__('to execute stateme') or \
                                        line.__contains__('LOCKING the DRIVE') or \
                                        line.__contains__('Returned, function') or\
                                        line.__contains__('storage service start') or \
                                        line.__contains__('Invalid session') or \
                                        line.__contains__('repair to server') or \
                                        line.__contains__('MM error'):
                                    continue # minor

                            if line.__contains__(' WARN '):
#                                 drive = "102100", arg = "XB063500"
                                #m = re_warning.match(line)
                                #if m:
                                    #self.handle_warning(epoch, m.groups()[0], m.groups()[1][:6])
                                m2 = re_warn_drive.match(line)
                                if m2:
                                    drive = m2.groups()[0]
                                    self.handle_event(FSM_EVNT_FATALERROR_1, {'drive':drive,'epoch':epoch})
                                    continue
                                m3 = re_warn_d1.match(line)
                                if m3:
                                    cartridge_id = m3.groups()[1][:6]
                                    drive = m3.groups()[0]
                                    #self.handle_event(FSM_EVNT_FATALERROR_1, {'drive':drive, 'cid':cartridge_id, 'epoch':epoch})
                                    continue

                                if line.__contains__("will retry in another drive,"):
                                    m = re_crtanddrv.match(line)
                                    if m:
                                        crt = m.groups()[0]
                                        drv = m.groups()[1]
                                        self.handle_event(FSM_EVNT_D2DMV, {'drive':drv, 'epoch':epoch, 'cid':crt})
                                        continue

                                if line.__contains__('"read_label"') or \
                                        line.__contains__('are disabled,') or \
                                        line.__contains__("LOCKING the DRIVE will exit the dismount loop") or \
                                        line.__contains__(' no response from robot') or \
                                        line.__contains__('rtm_GetRequestEntries') or \
                                        line.__contains__('NOT exist in DriveTable') or \
                                        line.__contains__('cartridge = "MP') or \
                                        line.__contains__('label written') or \
                                        line.__contains__('Client Cancels All Jobs') or \
                                        line.__contains__('Job recovered, di') or \
                                        line.__contains__('hardware defined in HPSS does not exist') or \
                                        line.__contains__('Dismount reason') or \
                                        line.__contains__('Job not found in queue') or \
                                        line.__contains__(' PVR) are disabled, arg = "MA') or \
                                        line.__contains__('Cache Overflow') or \
                                        line.__contains__('Cartridge has not been checked in') or \
                                        line.__contains__('No drives of this type in') or \
                                        line.__contains__('not found in LTO') or \
                                        line.__contains__('Not enough drives of this type') or \
                                        line.__contains__('Drive Notify failed') or \
                                        line.__contains__('= "eject_cart"') or \
                                        line.__contains__('information request failed') or \
                                        line.__contains__(' STATUS_') or \
                                        line.__contains__('Address types'):
                                    continue #warn

                                if line.__contains__(' Dismount reason') and line.__contains__('drive = "4'):
                                    continue

                            if line.__contains__(' EVNT '):
                                m1 = re_evnt_drive_enabl .match(line)
                                if m1:
                                    drive = m1.groups()[0]
                                    self.handle_event(FSM_EVNT_RECOVER_FAT1, {'drive':drive, 'epoch':epoch})
                                    continue
                                if line.__contains__("Client logged ") or \
                                        line.__contains__('Total Drive Count') or \
                                        line.__contains__(' logfiles ') or \
                                        line.__contains__('Storage map state') or \
                                        line.__contains__("CONAN") or \
                                        line.__contains__("erver 'Mover") or \
                                        line.__contains__("'STK PVR'") or \
                                        line.__contains__("Connection table full") or \
                                        line.__contains__("Open files on connection shutdown") or \
                                        line.__contains__("Repack Completed SClassId") or \
                                        line.__contains__('robot is offline, drive = "0') or \
                                        line.__contains__("Deferred state change") or \
                                        line.__contains__("End of media on ") or \
                                        line.__contains__('Reclaim completed for storage') or \
                                        line.__contains__("Request w/o client") or \
                                        line.__contains__("Exporting cartridge") or \
                                        line.__contains__("Export of ") or \
                                        (line.__contains__(", Disabled") and line.__contains__("dmin drive change") )or\
                                        line.__contains__("av_Initialize") or \
                                        line.__contains__("Mount failed, no drives") or \
                                        line.__contains__("Import of cartridge ") or \
                                        line.__contains__("STK volume ejects are done asynchronously") or \
                                        line.__contains__("could not be mounted, Condition") or \
                                        line.__contains__('Job not found in queue') or \
                                        line.__contains__("Core Server shutting") or \
                                        line.__contains__('All disk storage maps') or \
                                        line.__contains__('SSMS0115') or \
                                        line.__contains__('Core Server Shutdown Complete') or \
                                        line.__contains__('Running with restricted') or \
                                        line.__contains__('No initialization is necessary') or \
                                        line.__contains__('Reissuing ') or \
                                        line.__contains__(' in PVR') or \
                                        line.__contains__('mm_ReadPVR') or \
                                        line.__contains__('Ejecting cartridge=') or \
                                        line.__contains__('Core Server startup') or \
                                        line.__contains__('Starting server') or \
                                        line.__contains__('been shutdown') or \
                                        line.__contains__('Delog complete') or \
                                        line.__contains__('Startup of server') or \
                                        line.__contains__('core_SignalThread') or \
                                        line.__contains__('has been renamed') or \
                                        line.__contains__('abel written') or \
                                        line.__contains__('CHECK_DISK_') or \
                                        line.__contains__('Core Server Admin'):
                                    continue #evnt

                            if line.__contains__(" ALRM "):
                                if line.__contains__(" Write request failed") or \
                                    line.__contains__(" Read request failed") or \
                                    line.__contains__('Data copy operation failed') or \
                                    line.__contains__("Cannot lock VV cache record") or \
                                    line.__contains__("Connection timed out") or\
                                    line.__contains__("Not owner") or \
                                    line.__contains__('No such file or ') or \
                                    line.__contains__("HPSS system failure") or \
                                    line.__contains__(" request descriptor table") or \
                                    line.__contains__('Error creating credentials') or \
                                    line.__contains__('File too large') or \
                                    line.__contains__('hpss_FilesetGetAttributes') or \
                                    line.__contains__('request threads busy') or \
                                    line.__contains__('DB connection has been busy') or \
                                    line.__contains__('Failed to get RTM records') or \
                                    line.__contains__("Retrying read from level") or \
                                    line.__contains__(" Rewind of device") or \
                                    line.__contains__('PVR reports mounting a cartridge in a drive which') or \
                                    line.__contains__('Request queue full') or \
                                    line.__contains__('No space left on device') or \
                                    line.__contains__('Internal software error') or \
                                    line.__contains__('Unable to obtain the Fileset') or \
                                    line.__contains__(' CAP priorit') or \
                                    line.__contains__('Restricted User list') or \
                                    line.__contains__('sending RPC reply') or \
                                    line.__contains__('Error sending data') or \
                                    line.__contains__('Deferred state change') or \
                                    line.__contains__('rtm_Reconnect') or \
                                    line.__contains__(' SAN3P ') or \
                                    line.__contains__('Resource locked') or \
                                        line.__contains__('missing mover error ') :
                                        continue #alrm

                                if line.__contains__('Cartridge reported IN_TRANSIT'):
                                    m = re_alrm_a.match(line)
                                    if m:
                                        self.handle_event(FSM_EVNT_FATALERROR_1, {'epoch':epoch, 'cid':m.groups()[0]})
                                        continue

                            if line.__contains__(" NUNN "):
                                m = re_nunn_importerr.match(line)
                                if m:
                                    self.handle_event(FSM_EVNT_FATALERROR_1, {'epoch':epoch, 'cid':m.groups()[0]})
                                    continue
                                if line.__contains__('Error no owner found') or \
                                        line.__contains__('on write()') or \
                                        line.__contains__('SS and BFS '):
                                    continue


                            if line.__contains__(" MAJR "):
                                if line.__contains__("Gatekeeper Server") or \
                                        line.__contains__("RPC reply") or \
                                        line.__contains__('hpss_ConnMgrGrabConn') or \
                                        line.__contains__('died on host') or \
                                        line.__contains__('not initialize socket') or \
                                        line.__contains__('Error receiving data') or \
                                        line.__contains__('rror obtaining transmit'):
                                    continue

                            if line.__contains__("ECFS "):
                                    if line.__contains__("CORE") or \
                                            line.__contains__('MPS'):
                                        continue

                            if line.__contains__('MARS '):
                                if line.__contains__('CORE') or \
                                    line.__contains__('MPS') :
                                    continue

                            if line.__contains__('ROOT' ):
                                if line.__contains__(' MPS') or \
                                    line.__contains__(' CORE'):
                                    continue

                            if line.__contains__(' HPSS '):
                                continue

                            if line.__contains__('Checking out cartridge') or \
                                    line.__contains__('Shutdown of server') or \
                                    line.__contains__('Tape aggregation') or \
                                    line.__contains__('itfile') or \
                                    line.__contains__('PVR reestablished') or \
                                    line.__contains__('eer uuid') or \
                                    line.__contains__('hpss_RPC') or \
                                    line.__contains__('RPC runtime error') or \
                                    line.__contains__('pvr_PVRSetAttrs') or \
                                    line.__contains__("Gatekeeper") or \
                                    line.__contains__("GateKeeper") or \
                                    line.__contains__('Authentication') or \
                                    line.__contains__('Bad connection handle') or \
                                    line.__contains__("PVR 'STK PVR") or \
                                    line.__contains__(' log files ') or \
                                    line.__contains__(' Mover ') or \
                                    line.__contains__('passive side of') or \
                                    line.__contains__('einitialization ') or \
                                    line.__contains__('hpss_prod') or \
                                    line.__contains__('136.156.') or \
                                    line.__contains__(' TRAC ') or \
                                    line.__contains__('-mvr1') or \
                                    line.__contains__('USERSPACE1') or \
                                    line.__contains__('pvr_Check') or \
                                    line.__contains__('hdrv01'):
                                continue

                            if line.__contains__('Failure'):
                                if line.__contains__('querying') or \
                                        line.__contains__:
                                    continue

                            print "unparsed line", line,

                        else:
                            pass
                            #if len(line)> 16:
                            #    print "time did not match", line
                    except:
                        print "unknown", line
                        raise
                        sys.exit()
                #print " file done"
                #sys.exit()

    def correlation(self):
        #with Timer("Correlation-Finished:"):
        def _exec_(csvfile, p):
            c = corrPearson()
            print datetime.datetime.now()
            print csvfile
            c.read(csvfile)
            #while True:
                #p = q.get()
            if p in string.ascii_uppercase:
            #        p = 'Y'
     #           for p in reversed(string.ascii_uppercase):
    #                print p

                    fields = c.filter_fields(['%s.+'%p], [])
                    if len(fields) > 1:
                        print "Run for argument ", p
                        res = c.full_correlation_matrix(fields, "correlation_%s"%p)
                        c.jsondump("%s_proj_%s.json"%(csvfile, p),res)
                        #sorted_res = {}
                        #for x in res.keys():
                        #    for y,v in res[x].items():
                        #        if not v in sorted_res.keys():
                        #            sorted_res[v[0]]=[]
                        #        sorted_res[v[0]].append((x,y))
                        #for i in sorted(sorted_res.keys()):
                        #    print i, sorted_res[i]
            #    else:
            #        break

        a = 'cartridges_tmt_per_hour.csv'
        csvfile = os.path.join(self.outputdir,a)

        #for p in ['Z']:
        for p in reversed(string.ascii_uppercase):
            _exec_(csvfile,p)
            gc.collect()
        print "done"

    def highestlevelstats(self):
        res = {}
        res3d = []
        for drv in sorted(self.drv.keys()):
            curmax = max(DRV_INT_MAP.values())
            DRV_INT_MAP[drv] = curmax+1
        for hid in sorted(self.hm.keys()):
            curmax = max(HID_INT_MAP.values())
            HID_INT_MAP[hid] = curmax+1

        for hid, obj in self.hm.items():
            c = obj.cost_drive()
            for k,v in c.items():
                if v[1]< 600:
                    res3d.append([HID_INT_MAP[hid],DRV_INT_MAP[k],v[1], v[0]])
                latency = round(v[1] ,0)
                ent = res.setdefault(latency, 0)
                res[latency] = ent+1

        filtered = {}
        for i in range(0,300):
            v = res.setdefault(i, 0)
            filtered[i]=v
        figp = os.path.join(self.outputdir, "drive_home_latency.png")
        plot_dict(filtered, figp)

        costhd = os.path.join(self.outputdir, "home_drive_costs.csv")
        with open(costhd , 'w') as mesh_file:
            lineBuf = StringIO.StringIO()
            lineBuf.write("home;drive;latency;observations")
            for [x,y,z,n] in res3d:
                lineBuf.write("\n%i;%i;%.1f;%.1f"%(x,y,z,n))
            mesh_file.write(lineBuf.getvalue())
            mesh_file.close()
        #vis = fast_plot.Visualizer("Home", "Drive", "AvgLatency")
        #vis.new_group_data(res3d)
        #vis.show()

    def checkerrors(self):
        operations = 0
        successful_crt_cycles = 0.0
        failed_crt_cycles = 0.0

        for crt, obj in self.crt.items():
            successful_crt_cycles += obj.get_successful_cycles()
            failed_crt_cycles += obj.get_failed_cycles()
            #err = obj.checkerrors()
            #if err >0:
            #    print crt, err#obj, err
                #sys.exit(1)
        print 'Failed ',failed_crt_cycles
        print 'Successful:' , successful_crt_cycles
        print 'Failed Percent', failed_crt_cycles/((failed_crt_cycles+successful_crt_cycles)/100)
        sys.exit()
        for d,obj in self.drv.items():
            err = obj.checkerrors()
            if err >0:
                print err, d #, obj
                #sys.exit(1)
            o = len(obj.data['data'])
            c = len(obj.data['cleaning'])
            #print d, o, c
            operations += o+c
        print operations

    def paperoutput(self):
        numrmounts = 0
        numvmounts = 0
        tapemnts = {}
        for cid, obj in self.crt.items():
            r,v,e = 0,0,0
            for m in obj.data['data']:
                r += 1
                v += len(m[DATA_INDEX_VOLUME])
            for m in obj.data['errors']:
                if m['m']>0:
                    e += 1
            numvmounts += v
            numrmounts += r
            tapemnts[cid] = (r,v,e)
        self._jsondmp(tapemnts, 'crtmnts.json', self.basedir)
        print "Tapemount stats done"

        # general data
        # number of tapes:
        numtapes = len(self.crt.keys())
        numdrives = len(self.drv.keys())
        numhomes = len(self.hm.keys())

        print '#Robot mounts', numrmounts
        print '#Volume mounts', numvmounts
        print '#Tapes', numtapes
        print '#Drives', numdrives
        print '#Homes', numhomes

    def _jsondmp(self, obj, file, basedir=None):
        x = file
        if basedir:
            x = os.path.join(basedir, file)
        with open(x, 'w') as f:
            json.dump(obj, f, indent=1)


    def parallel_correlation_analysis(self):
        print " entering parallel correlation analysis "
        interval_secs = [-1800, -1200,  -600, -300, -120, -60, 0, 60, 120, 300, 600,1200, 1800]
        pat = re.compile(".*cartridges_tmt_per_hour\.csv_proj_([A-Z]+)\.json")

        marstotal = {}
        for i in interval_secs:
            marstotal[i] = []
        marstotal['possum']=0
        marstotal['negsum']=0
        marstotal['totalerror']=0

        lineBuf = StringIO.StringIO()
        lineBuf.write("Project")
        for i in sorted(interval_secs):
            lineBuf.write(";%i"%i)
        lineBuf.write(";TotalFail;NegativeSum;PositiveSum;Correlation0.8Pairs;AllPairs")
        infiles = glob.glob(os.path.join(self.outputdir, 'cartridges_tmt_per_hour.csv_proj_*.json'))
        for infile in reversed(infiles):
            scnt=0
            cnt = 0
            gc.collect()
            print "Starting with file ", infile
            totalsuccess,totalfail = 0,0
            correlated = {}
            m = pat.match(infile)
            project = m.groups()[0]
            lineBuf.write("\n%s"%project)
            with open(infile, 'r') as f:
                entries = json.load(f)
                t = len(entries)
                for (x,y,p,s) in entries:
                    #print "Entry ", cnt, ' of ', t
                    if p >= 0.8 :
                        add_correlated(correlated, x,y)
                        scnt += 1
                    cnt += 1
                print "finished generating correlations, found %s, failed:%i"%(scnt,cnt)

            #globalsuccess = Counter()
            #for i in sorted(interval_secs):
            #    globalsuccess[i] = []

            #lock = multiprocessing.Lock()
            prefix = "correlation_result_%s"%project
            corrkeys = multiprocessing.Queue()
            for cid in sorted(correlated.keys()):
                corrkeys.put(cid)
            procs = []
            for i in range(multiprocessing.cpu_count()):
                proc = multiprocessing.Process(target=calc_correlation, args=(corrkeys, correlated, self._get_crt, prefix, interval_secs))
                proc.daemon=False
                procs.append(proc)
                proc.start()

            for p in procs:
                #p = procs.pop()
                p.join()
                print "waiting for process ", p

            possum, negsum = 0,0
            r2 = {}
            for i in interval_secs:
                r2[i]=[]
            files = glob.glob(os.path.join("/tmp", "%s_*.json.gz"%(prefix)))
            for file in files:
                print 'reading',file
                with gzip.open(file, 'r') as f:
                    result = json.load(f)
                    for interv, entries in result['interval'].items():
                        #print interv, entries
                        i = int(interv)
                        r2[i].extend(entries)
                        if i > 0:
                            possum += len(entries)
                        else:
                            negsum += len(entries)

                    totalfail += result['errorcnt']

            if project in ['R','S', 'U','V','W', 'X', 'Y']:
                marstotal['totalerror'] += totalfail
                marstotal['possum'] += possum
                marstotal['negsum'] += negsum

                for i in sorted(interval_secs):
                    marstotal[i].extend(r2[i])

            for i in sorted(interval_secs):
                lineBuf.write(";%s"%len(r2[i]))
            lineBuf.write(";%i;%i;%i"%(totalfail,negsum,possum))

        lineBuf.write('\nMARS')
        for i in sorted(interval_secs):
            lineBuf.write(";%s"%len(marstotal[i]))
        lineBuf.write(";%i;%i;%i;%i;%i"%(marstotal['totalerror'],marstotal['negsum'],marstotal['possum'],scnt,cnt))

        with open(os.path.join(self.outputdir, "crt_correlation.csv"), 'w') as csv_file:
            csv_file.write(lineBuf.getvalue())
            csv_file.close()
            lineBuf.flush()


    def debug(self):
        pat = re.compile(".*cartridges_tmt_per_hour\.csv_proj_([A-Z]+)\.json")
        total=0
        slot='per_day'
        lock = multiprocessing.Lock()

        if FULLRUN or 1:    # tape system stats
            #res = self.drv.values()[0].pertime()
            res = set()
            atts = set()
            #tsset = set()
            slotted_ts = {}
            crtlist = []
            drvlist=[]

            wq1 = multiprocessing.Queue()
            for id in self.drv.keys():
                wq1.put(id)
                drvlist.append(id)

            for id in self.crt.keys(): # not sorted
                wq1.put(id)
                crtlist.append(id)

            procs = []
            for i in range(multiprocessing.cpu_count()):
                proc = multiprocessing.Process(target=_cb_pertime, args=(wq1,))
                proc.daemon=False
                procs.append(proc)
                proc.start()
            for p in procs:
                p.join()
                print "waiting for process ", p

            print datetime.datetime.now(), "starting"
            for id, drv in self.drv.items():
                tmp = drv.pertime()
                for k,v in tmp.items():
                    print datetime.datetime.now(), id, "drv.pertime ", k
                    if not k in slotted_ts.keys():
                        slotted_ts[k]=[]
                    slot = slotted_ts.get(k)
                    for ts in v.keys():
                        if not ts in slot:
                            slot.append(ts)
                    res.update([k])
                    if len(atts)==0:
                        atts.update(v[v.keys()[0]].keys())
                    #tsset.update(v.keys())

            x = Counter()
            prefix = 'tapestats'
            print "first loop done ", datetime.datetime.now()
            gc.collect()
            for slot in res:
                x[slot]={}
                print datetime.datetime.now(), "slot " , slot
                inqueue = multiprocessing.Queue()

                for tsstr in sorted(slotted_ts[slot]):
                    inqueue.put(tsstr)

                procs = []
                #for i in range(1):
                for i in range(multiprocessing.cpu_count()/2):
                    proc = multiprocessing.Process(target=_tapestats, args=(inqueue, slot, crtlist, self._get_crt, drvlist, self._get_drv, atts, prefix))
                    proc.daemon=False
                    procs.append(proc)
                    proc.start()
                for p in procs:
                    p.join()
                    print "waiting for process ", p

            files = glob.glob("/tmp/%s_*.json"%prefix)
            for file in files:
                print "reading ", file
                res = _cb_gzipload(file)
                for slot,elems in res.items():
                    if not slot in x.keys():
                        x[slot]={}
                    xs =  x[slot]
                    for ts, tmpatt in elems.items():
                        if ts not in xs.keys():
                            xs[ts] = {}
                        xsts = xs[ts]
                        for a,v in tmpatt.items():
                            xsts[a] = v

            for slot in sorted(x.keys()):
                atts.add(VOL_MNT_LENGTH)
                atts.add(CLN_TIME_LENGTH)
                f = os.path.join(self.outputdir,"tapesystem_%s.csv"%slot)
                print f

                with open(f, 'w') as csv_file:
                    lineBuf = StringIO.StringIO()
                    lineBuf.write("timestamp")
                    for att in atts:
                        lineBuf.write(";%s"%att)
                    for ts in sorted(x[slot].keys()):
                        lineBuf.write("\n%s"%ts)
                        for att in sorted(atts):
                            linebuf_write(lineBuf,x[slot][ts].get(att, 0) )
                    csv_file.write(lineBuf.getvalue())
                    csv_file.close()
                    lineBuf.flush()

    def debug2(self):
        res = {}
        files = glob.glob(os.path.join(self.outputdir, "drives","crt_*_pertime.json"))
        for file in files:
            #print file
            with open(file, 'r') as f:
                tmp = json.load(f)
                for slot, slotobj in tmp.items():
                    if slot != 'per_hour':
                        continue
                    if not slot in res.keys():
                        res[slot] = {}
                    for ts, args in slotobj.items():
                        if not ts in res[slot].keys():
                            res[slot][ts] = {}
                        for k,v in args.items():
                            if not k in res[slot][ts].keys():
                                res[slot][ts][k]=0
                            res[slot][ts][k] += v
                            #print slot,ts,k,v

        for slot in res.keys():
            if slot != 'per_hour':
                continue
            for tsstr in sorted(res[slot].keys()):
                ts = datetime.datetime.strptime(tsstr, "%Y-%m-%d %H:%M:%S")
                epochts = int(calendar.timegm(ts.utctimetuple()))
                epochse = epochts + get_slot_size_seconds(slot, ts.month, ts.year)
             #   print slot,ts
                res[slot][tsstr][VOL_MNT_LENGTH]=0
                res[slot][tsstr][CLN_TIME_LENGTH]=0
                for crt in self.crt.values(): # not sorted
                    res[slot][tsstr][VOL_MNT_LENGTH] += crt.get_volume(slot, ts)

                for drv in self.drv.values():
                    res[slot][tsstr][CLN_TIME_LENGTH] += sum(drv.estimate_cleaning_time(epochts,epochse))

        self._jsondmp(res, 'taperesults.json', self.outputdir)
        atts = [CLN_TIME_LENGTH,VOL_MNT_LENGTH,TOTAL_MNT_TIME,TOTAL_MNTS]
        for slot,slotobj in res.items():
            if slot != 'per_hour':
                continue
            file = os.path.join(self.outputdir, "tapesystem2_%s.csv"%slot)
            with open(file, 'w') as csv_file:
                lineBuf = StringIO.StringIO()
                lineBuf.write("timestamp")
                for att in atts:
                    lineBuf.write(";%s"%att)
                for ts in sorted(slotobj.keys()):
                    lineBuf.write("\n%s"%ts)
                    for att in atts:
                        linebuf_write(lineBuf,slotobj[ts].get(att, 0) )
                csv_file.write(lineBuf.getvalue())
                csv_file.close()
                lineBuf.flush()


    def debug3(self):
        # cdf time between mounts
        res = []
        for cid, crt in self.crt.items():
            res.extend(crt.get_tbm())
        file = os.path.join(self.outputdir, "timebetweenmounts_.csv")
        r = sorted(res)
        with open(file, 'w') as csv_file:
                lineBuf = StringIO.StringIO()
                lineBuf.write("percentile;timebetweenmounts")
                for p in [0.01,0.05,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,0.95,0.99]:
                    pt = percentile(r,p)
                    lineBuf.write("\n%s;%s"%(p,pt))

                csv_file.write(lineBuf.getvalue())
                csv_file.close()
                lineBuf.flush()
        res = []
        for cid, crt in self.crt.items():
            res.extend(crt.get_latency())
        file = os.path.join(self.outputdir, "latency.csv")
        r = sorted(res)
        with open(file, 'w') as csv_file:
                lineBuf = StringIO.StringIO()
                lineBuf.write("percentile;latency")
                for p in [0.01,0.05,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,0.95,0.99]:
                    pt = percentile(r,p)
                    lineBuf.write("\n%s;%s"%(p,pt))

                csv_file.write(lineBuf.getvalue())
                csv_file.close()
                lineBuf.flush()


# usage:
# 1. aggregate results, using the robot analyse py "aggregate" function
# 2. run this script with either
#  a) full (takes looong, dont know how long)
#  b) "-w" to load the whpss data and
#     "-r" to integrate robot logs
#     "-z" to check wheather the robotlogs fixed some broken whpss entries
#     "-y" to derive "home" classes
# 3.  "-c" to run correlation results
#     "-s" to collect general statistics
#     "-p" for some stats generated specifically for the fast paper, (not all of them are generated here!)
if __name__ == '__main__':
    if len(sys.argv)>1:
        if '-d' in sys.argv:
            print "daemonizing"
            python_daemon.createDaemon()
            sys.stdout.close() #we close /dev/null
            sys.stderr.close()

            os.close(2) # and associated fd's
            os.close(1)

            # now we open a new stdout
            # * notice that underlying fd is 1
            # * bufsize is 1 because we want stdout line buffered (it's my log file)
            sys.stdout = open('/tmp/bla','w',1) # redirect stdout
            os.dup2(1,2) # fd 2 is now a duplicate of fd 1
            sys.stderr = os.fdopen(2,'a',0) # redirect stderr
            # from now on sys.stderr appends to fd 2
            # * bufsize is 0, I saw this somewhere, I guess no bufferization at all is better for stderr

            # now some tests... we want to know if it's bufferized or not
            print "stdout"
            print >> sys.stderr, "stderr"
            os.system("echo stdout-echo") # this is unix only...
            os.system("echo stderr-echo > /dev/stderr")
            # cat /tmp/bla and check that it's ok; to kill use: pkill -f bla.py

    dodump = False
    if '-f' in sys.argv:
        FULLRUN = True
    x = os.path.join(os.getcwd(),sys.argv[1])
    god = God(x)

    if '-c' in sys.argv or FULLRUN:
#            with Timer('Correlation (-c)'):
            god.correlation()
            #dodump=True
            sys.exit()

    elif '--correlationanalysis' in sys.argv:
        #god.debug()
        god.jsonload(True)
        god.parallel_correlation_analysis()
        sys.exit()

    else:
 #       with Timer('jsonload'):
        god.jsonload()

        if '--debug' in sys.argv:
            god.debug()
            sys.exit()

        if '--debug2' in sys.argv:
            god.debug2()
            sys.exit()

        if '--debug3' in sys.argv:
            god.debug3()
            sys.exit()

    #    with Timer('WHPSS read (-w)'):
        if '-w' in sys.argv or FULLRUN:
                god.whpss_read()
       #         with Timer('WHPSSdone, Immediately json dump'):
                god.jsondump()

        #with Timer("Robot read: (-r)"):
        if '-r' in sys.argv or FULLRUN:
                god.robot_read()
    #            with Timer('Robotdone, Immediately json dump'):
                god.jsondump()
                pass

     #   with Timer("Check for fixed errors: (-z)"):
        if '-z' in sys.argv or FULLRUN:
                god.collect_recovered_errors()
                dodump=True

      #  with Timer("derive homes (-y)"):
        if '-y' in sys.argv or FULLRUN:
                god.derive_homes()
                dodump = True

       # with Timer("Cnt errors (-e)"):
        if '-e' in sys.argv:
                god.checkerrors()
                dodump=True

        #with Timer('stats: (-s)'):
        if '-s' in sys.argv or FULLRUN:
                god.stats()
                #dodump=True

        #with Timer("Highest Level Stats (-hh)"):
        if '-hh' in sys.argv or FULLRUN:
                god.highestlevelstats()

        #with Timer ("Generate Paper output (-p)"):
        if '-p' in sys.argv or FULLRUN:
                god.paperoutput()

        if dodump:
        #    with Timer('json dump'):
                god.jsondump()



