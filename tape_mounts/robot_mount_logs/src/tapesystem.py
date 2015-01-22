__author__ = 'maesker'

import glob, sys,os,datetime, json, re, string, multiprocessing, Queue, calendar, bisect, time
from collections import Counter
import python_daemon
try:
    import numpy, scipy, scipy.stats
except:
    print "no numpy available"






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



DATA_INDEX_REQM     = 0     # request mount
DATA_INDEX_M        = 1     # mount done
DATA_INDEX_VOLUME   = 2     # volume (mount-dismounts)
DATA_INDEX_REQD     = 3     # request dismount cartridge
DATA_INDEX_D        = 4     # dismount cartridge
DATA_INDEX_MH       = 5
DATA_INDEX_DH       = 6
DATA_INDEX_DRV      = 7     # drive id, cartridge id or home id,


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

def init_per_week():
    res = {}
    end = datetime.datetime(2014,1,1,0,0)
    dt = datetime.datetime(2012,1,1,0,0)
    while dt < end:
        epoch = calendar.timegm(dt.timetuple())
        res[unicode(epoch)] = Counter({
            u'volume_mounts':0,
            u'tape_loads':0,
            u'volume_remounts_60sek':0,
            u'volume_remounts_300sek':0,
            u'tape_reloads_60sek':0,
            u'tape_reloads_300sek':0,
            u'tape_reloads_60sek_samedrive':0,
            u'tape_reloads_300sek_samedrive':0,
            u'tape_reloads_60sek_otherdrive':0,
            u'tape_reloads_300sek_otherdrive':0
        })
        dt = dt + datetime.timedelta(days=globals()['steps'])
    return res

def init_per_hour():
    res = {}
    end = datetime.datetime(2014,1,2,0,0)
    dt = datetime.datetime(2012,1,1,0,0)
    while dt < end:
        epoch = calendar.timegm(dt.timetuple())
        res[unicode(epoch)] = Counter({
            u'volume_mountlength':0,
            u'tape_load_length':0,
        })
        dt = dt + datetime.timedelta(hours=1)
    return res

def _check_tape_remount(robmnts, event, window):
    reloaded, same, other = 0,0,0
    index = robmnts.index(event)
    if index > 0:
        diff = event[DATA_INDEX_REQM] - robmnts[index-1][DATA_INDEX_D]
        if diff >= 0 and diff <= window:
            reloaded = 1
            if event[DATA_INDEX_DRV] ==  robmnts[index-1][DATA_INDEX_DRV]:
                same = 1
            else:
                other = 1
    return (reloaded, same,other)

def _check_volume_remount(allvolmnts, volentry, window):
    index = allvolmnts.index(volentry)
    if index > 0:
        diff = volentry[0] - allvolmnts[index-1][1]
        if diff >= 0 and diff <= window:
            return 1
    return 0

def getdata(path):
    with open(path, 'r') as f:
        res = json.load(f)
        return res

def process(inqueue, prefix):
    perweek_res = init_per_week()

    mounts_per_tape = []
    mount_request_latency = []
    globalres = {
            u'total_tapes':0,
            u'total_volume_mounts':0,
            u'total_tape_loads':0,
        }

    sorted_perweekkeys = []
    for i in sorted(perweek_res.keys()):
        sorted_perweekkeys.append(int(i))

    while True:
        try:
            file = inqueue.get(True,10)
            datax = getdata(file)
            data = datax['data']
            #id = datax['id']

            mounts_per_tape.append(len(data))
            globalres[u'total_tapes'] += 1 # one more tape

            for event in data:
                index = bisect.bisect_left(sorted_perweekkeys,event[DATA_INDEX_REQM])-1
                obj = perweek_res[unicode(sorted_perweekkeys[index])]
                obj[u'tape_loads'] += 1

                (reloaded, same, other) = _check_tape_remount(data, event, 60)
                obj[u'tape_reloads_60sek'] += reloaded
                obj[u'tape_reloads_60sek_samedrive'] += same
                obj[u'tape_reloads_60sek_otherdrive'] += other

                (reloaded, same, other) = _check_tape_remount(data, event, 300)
                obj[u'tape_reloads_300sek'] += reloaded
                obj[u'tape_reloads_300sek_samedrive'] += same
                obj[u'tape_reloads_300sek_otherdrive'] += other

                mount_request_latency.append(event[DATA_INDEX_M]-event[DATA_INDEX_REQM])
                globalres[u'total_tape_loads'] += 1    # one more load
                for volentry in event[DATA_INDEX_VOLUME]:
                    volm,vold = volentry
                    globalres[u'total_volume_mounts'] += 1
                    obj[u'volume_mounts'] += 1
                    obj[u'volume_remounts_60sek'] += _check_volume_remount(event[DATA_INDEX_VOLUME], volentry, 60)
                    obj[u'volume_remounts_300sek'] += _check_volume_remount(event[DATA_INDEX_VOLUME], volentry, 300)

        except Queue.Empty:
            break

    file = os.path.join("/tmp", "%s_%s.json"%(prefix,os.getpid()))
    print 'writing ',file
    with open(file, 'w') as f:
        json.dump( {'globalres':globalres,
                    'perweekres':perweek_res,
                    'mounts_per_tape':mounts_per_tape,
                    'mount_request_latency' : mount_request_latency
                   } , f, indent=1)
    print 'exit'

def analyse(files, prefix):
    inqueue = multiprocessing.Queue()
    for file in sorted(files):
        inqueue.put(file)

    procs = []
    for i in range(multiprocessing.cpu_count()):
        proc = multiprocessing.Process(target=process, args=(inqueue, prefix))
        proc.daemon=False
        procs.append(proc)
        proc.start()

    for p in procs:
        p.join()
        print "waiting for process ", p

    globalres = Counter()
    perweekres = init_per_week()
    mount_request_latency = []
    mounts_per_tape = []


    tmpfiles = glob.glob(os.path.join('/tmp',"%s_*.json"%prefix))
    for file in tmpfiles:
        with open(os.path.join('/tmp', file), 'r') as f:
            tmpres = json.load(f)
            mounts_per_tape.extend(tmpres['mounts_per_tape'])
            mount_request_latency.extend(tmpres['mount_request_latency'])
            globalres = globalres + Counter(tmpres['globalres'])
            for k,v in sorted(perweekres.items()):
                for k2,v2 in tmpres['perweekres'][k].items():
                    v[k2] += tmpres['perweekres'][k][k2]
    #print prefix, perweekres
    #print globalres


    with open(os.path.join("/tmp", "full_%s.json"%prefix), 'w') as out:
        json.dump({'globalres':globalres,
                   'perweekres':perweekres,
                   'mount_request_latency':mount_request_latency,
                   'mounts_per_tape':mounts_per_tape
                  }, out, indent=1)
    print 'done', prefix

################################################
def get_mountlen(event, resdict, sorted_perweekkeys):
    excnt = 0
    slots = sorted(resdict.keys())
    slotsize = sorted_perweekkeys[1]-sorted_perweekkeys[0]
    absdiff = event[DATA_INDEX_D] - event[DATA_INDEX_M]
    try:
        total_voltime = 0.0
        for volm, vold in event[DATA_INDEX_VOLUME]:
            index = bisect.bisect_left(sorted_perweekkeys,volm)-1
            #print sorted_perweekkeys[index], volm
            slotstartepoch = sorted_perweekkeys[index]
            slotendepoch = slotstartepoch
            while (vold>=slotstartepoch):
                slotendepoch += slotsize
                barrierstart = max(volm, slotstartepoch)
                barrierend = min(vold, slotendepoch)
                spent = barrierend-barrierstart
                resdict[unicode(slotstartepoch)][u'volume_mountlength'] += spent
                total_voltime += spent
                slotstartepoch = slotendepoch
    except:
        excnt += 1
        print "some exception", excnt

    try:
        if total_voltime/float(absdiff) <= 0.001 and absdiff>=1000:
            if event[DATA_INDEX_M] > 1325388664:
                print "not plausible",total_voltime,absdiff
                print event
        else:
            index = bisect.bisect_left(sorted_perweekkeys,event[DATA_INDEX_M])-1
            #print sorted_perweekkeys[index], event[DATA_INDEX_M]
            slotstartepoch = sorted_perweekkeys[index]
            slotendepoch = slotstartepoch
            while (event[DATA_INDEX_D]>slotstartepoch):
                slotendepoch += slotsize
                barrierstart = max(event[DATA_INDEX_M], slotstartepoch)
                barrierend = min(event[DATA_INDEX_D], slotendepoch)
                spent = barrierend-barrierstart
                #if (event[DATA_INDEX_D]-event[DATA_INDEX_M])> 86400: #24 hours:
                #    print event
                #if spent > 3600:
                #    print "WTF", event
                #    print barrierend ,barrierstart, spent
                #if
                resdict[unicode(slotstartepoch)][u'tape_load_length'] += spent
                #if resdict[unicode(slotstartepoch)][u'tape_load_length'] > 650000:
                #    print resdict[unicode(slotstartepoch)][u'tape_load_length'],slotstartepoch
                slotstartepoch = slotendepoch

    except:
        excnt += 1
        print "some exception", excnt


def mountlengthanalyse(files):
    inqueue = multiprocessing.Queue()
    for file in sorted(files):
        inqueue.put(file)

    procs = []
    for i in range(multiprocessing.cpu_count()):
        proc = multiprocessing.Process(target=processmountlength, args=(inqueue,))
        proc.daemon=False
        procs.append(proc)
        proc.start()

    for p in procs:
        p.join()
        print "waiting for process ", p

    perweekres = init_per_hour()

    tmpfiles = glob.glob(os.path.join('/tmp',"mountlength_*.json"))
    for file in tmpfiles:
        with open(os.path.join('/tmp', file), 'r') as f:
            tmpres = json.load(f)
            print file
            for k,v in sorted(perweekres.items()):
                for k2,v2 in tmpres[k].items():
                    v[k2] += tmpres[k][k2]

    with open(os.path.join("/tmp", "full_mountlength.json"), 'w') as out:
        json.dump(perweekres, out, indent=1)
    print 'done'

def processmountlength(inqueue):
    perweek_res = init_per_hour()
    totalsize = inqueue.qsize()
    sorted_perweekkeys = []
    for i in sorted(perweek_res.keys()):
        sorted_perweekkeys.append(int(i))
    time.sleep(2)
    while True:
        try:
            file = inqueue.get(True,10)
            print file, inqueue.qsize(), ' of ', totalsize
            datax = getdata(file)
            data = datax['data']
            for event in data:
                get_mountlen(event, perweek_res,sorted_perweekkeys)

        except Queue.Empty:
            break
    file = os.path.join("/tmp", "mountlength_%s.json"%(os.getpid()))
    print 'writing ',file
    with open(file, 'w') as f:
        json.dump( perweek_res, f, indent=1)
    print 'exit'

def processoutput(path):
    print path
    with open(path, 'r') as f:
        res = json.load(f)
        tapeloads = []
        for i in sorted(res['perweekres'].keys()):
            v = res['perweekres'][i][u'tape_loads']
            tapeloads.append(v)
    for p in [0.01, 0.05,0.25,0.50, 0.75, 0.95, 0.99]:
        print p, percentile(sorted(tapeloads), p)
    print get_meanconf_string(tapeloads)


if __name__=='__main__':
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

    if '-p1' in sys.argv:
        os.system("rm /tmp/mountlength_*")
        base = os.path.join(os.getcwd(), sys.argv[1])
        #tmpecfsfiles = glob.glob(os.path.join(base,"crt_*.json"))
        tmpecfsfiles = glob.glob(os.path.join(base,"crt_*.json"))
        mountlengthanalyse(tmpecfsfiles)

    elif '-p2' in sys.argv:
        loaded_mounted_ratio = []
        path = sys.argv[2]
        with open(os.path.join(os.getcwd(), path), 'r') as out:
            totaltll = 0.0
            totalvml = 0.0
            xx=""
            res = json.load(out)
            for st, val in sorted(res.items()):
                #diff = val['tape_load_length']- val['volume_mountlength']
                tll = float(val['tape_load_length'])
                vml = float(val['volume_mountlength'])
                totaltll += tll
                totalvml += vml
                if tll > 0:
                    ratio =  vml / tll
                    if ratio < 0.2:
                        xx= "xx"
                    else:
                        xx = ""
                    print st, '\t',vml,'\t',tll, xx
                    if tll > 650000:
                        sys.exit()
                    loaded_mounted_ratio.append(ratio)
        for p in [0.01, 0.05,0.25,0.50, 0.75, 0.95, 0.99]:
            print p, round(percentile(sorted(loaded_mounted_ratio), p), 2)
        print 'totaltll', totaltll, ' totalvml', totalvml , ' ratio ', totalvml/totaltll

    elif '-p3' in sys.argv:
        processoutput(os.path.join(os.getcwd(), sys.argv[1]))
    else:
        os.system("rm /tmp/mars_*.json")
        os.system("rm /tmp/ecfs_*.json")

        globals()['steps'] = int(sys.argv[2])

        base = os.path.join(os.getcwd(), sys.argv[1])
        tmpecfsfiles = glob.glob(os.path.join(base,"crt_*.json"))
        #tmpecfsfiles = glob.glob(os.path.join(base,"crt_C*.json"))
        #tmpmarsfiles = glob.glob(os.path.join(base,"crt_[R|S|W|U|V|X|Y]*.json"))

        ecfsfiles=[]
        for i in tmpecfsfiles:
            ecfsfiles.append(os.path.join(base, i))
        #marsfiles=[]
        #for i in tmpmarsfiles:
        #    marsfiles.append(os.path.join(base, i))

        ecfs_res = analyse(ecfsfiles, 'ecfs')
        #mars_res = analyse(marsfiles, 'mars')








