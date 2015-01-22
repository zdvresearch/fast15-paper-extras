__author__ = 'maesker'


import gzip,os, sys, csv, re, json, math, glob, StringIO, multiprocessing, Queue, time

try:
    from matplotlib import pyplot as plt
    import numpy
    from scipy.stats import spearmanr, pearsonr
except:
    pass


def plot_dict(d, fn=None):
    keys = sorted(d.keys())
    n = numpy.arange(len(keys))
    ticksstep = int(math.sqrt(len(keys)))

    for k in keys:
        #print k, ";" , d[k]
        plt.bar(k, d[k], width=0.01)
    #plt.xticks(n[0::ticksstep], keys[0::ticksstep])
    if fn:
        plt.savefig(fn)
    else:
        plt.show()


def correlate_pearson(inque, fields, data, prefix):
    result = []
    while inque.qsize():
        #print inque.qsize()
        try:
            (x,y) = inque.get(True,3)
        except Queue.Empty:
#                print "Done"
            break
        #print x,y
        indexx = fields.get(x, None)
        indexy = fields.get(y, None)
        if indexx == None or indexy == None:
            print "unknown index ", x, y
            #return [0,0]
            #sys.exit(1)

        vecX, vecY = [],[]
        for elem in data:
            if float(elem[indexx])>0 or float(elem[indexy])>0:
                vecX.append(float(elem[indexx]))
                vecY.append(float(elem[indexy]))

        pc = pearsonr(vecX, vecY)
        res=[x,y]
        res.append(round(pc[0], 3))
        res.append(round(pc[1], 3))
        #print res
        result.append(res)

    file = os.path.join("/tmp", "%s_%s.json.gz"%(prefix,os.getpid()))
    print file
    with gzip.open(file, 'w') as f:
        json.dump(result, f, indent=1)
    print 'exit'


class corrPearson:
    def __init__(self):
        self.roundto = 3
        self.data = []
        self.fields = {}

    def read(self, file):
        with open(file, 'rb') as csvfile:
            spamreader = csv.reader(csvfile, delimiter=';')
            items = spamreader.next()
            for i in items:
                if len(i)>0:
                    self.fields[i] = items.index(i)
            for row in spamreader:
                self.data.append(row)
            csvfile.close()

    def full_correlation_matrix(self, fields, prefix):
        combinations = multiprocessing.Queue()
        for x in fields:
            for y in fields[fields.index(x)+1:]:
#                if x not in res.keys():
#                    res[x]={}
                combinations.put((x,y))
                #res[x][y]=self.correlate_pearson(x,y)

        #os.system('ls /tmp/%s*'%prefix)
        procs = []
        for i in range(multiprocessing.cpu_count()):
            proc = multiprocessing.Process(target=correlate_pearson, args=(combinations, self.fields, self.data, prefix))
            proc.daemon=False
            procs.append(proc)
            proc.start()
            time.sleep(3)

        for p in procs:
            #p = procs.pop()
            p.join()
            print "waiting for process ", p

        r2 = []
        files = glob.glob(os.path.join("/tmp", "%s_*.json.gz"%(prefix)))
        for file in files:
            print file
            with gzip.open(file, 'r') as f:
                result = json.load(f)
                r2.extend(result)
        return r2

    def filter_fields(self, whitelist_pattern=None, blacklist_pattern=[]):
        if type(blacklist_pattern) != type([]):
            print "blacklist type invalid"
            sys.exit()
        fields = []
        if whitelist_pattern == None:
            fields = self.fields.keys()
        else:
            for pat in whitelist_pattern:
                p = re.compile(pat)
                for f in self.fields.keys():
                    if p.match(f):
                        if f not in fields:
                            fields.append(f)
        for i in blacklist_pattern:
            p = re.compile(i)
            for f in fields:
                if p.match(f):
                    fields.remove(f)
        return fields

    def jsondump(self, fn, result):
        with open(fn, 'w') as f:
            json.dump(result, f, indent=1)

    def jsonload(self, fn):
        with open(fn, 'rb') as f:
            res = json.load(f)
        return res

    def analyse(self, jsonfile):
        res = self.jsonload(jsonfile)
        sorted_res = {}
        for i in range(-100,101):
            x = i*0.01
            sorted_res[round(x,2)]= 0

        for x in res.keys():
            for y,v in res[x].items():
                rounded = round(v[0],2)
                if not rounded in sorted_res.keys():
                    #print rounded,y,v
                    pass
                else:
                    sorted_res[rounded] += 1

        #for i in sorted(sorted_res.keys()):
            #print i, sorted_res[i]
        plot_dict(sorted_res, "%s.png"%jsonfile)
        return sorted_res

    def collect(self, srcdir):
        ret = {}
        d = os.path.join(srcdir, 'cartridges_tmt_per_hour.csv_proj_*.json')
        all_log_files = sorted(glob.glob(d))
        for i in all_log_files:
            print i, os.path.basename(i)

            proj = re.compile('cartridges_tmt_per_hour\.csv_proj_([A-Z]+)\.json')
            m = proj.match(os.path.basename(i))
            if m:
                ret[m.group(0)] = self.analyse(i)

        fn = "cartridge_correlation_matrix.csv"
        with open(fn , 'w') as file:
            lineBuf = StringIO.StringIO()
            lineBuf.write("Project")
            for i in range(-100,101):
                lineBuf.write(";%.2f"%(i*0.01))
            lineBuf.write('\n')

            for p in ret.keys():
                lineBuf.write(p)
                for i in sorted(ret[p].keys()):
                    lineBuf.write(";%.2f"%(ret[p][i]))
                lineBuf.write('\n')
            file.write(lineBuf.getvalue())
            file.close()

if __name__ == '__main__':
    c = corrPearson()
    if  '-a' in sys.argv:
        #f = os.path.join(os.getcwd(),"cartridges_tmt_per_hour.csv_proj_X.json")
        f = os.path.join(os.getcwd(),sys.argv[2])
        print f
        c.analyse(f)

    if '-c' in sys.argv:
        c.collect(os.getcwd())
