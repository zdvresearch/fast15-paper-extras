__author__ = 'maesker'

import csv, sys


with open(sys.argv[1], 'rb') as csvfile:
    cols = sys.argv[2:]
    keys = {}
    spamreader = csv.reader(csvfile, delimiter=';')
    items = spamreader.next()
    for i in items:
        if len(i)>0 and i in cols:
            keys[i] = items.index(i)

    for i in sorted(keys):
        print i,'\t',
    for row in spamreader:
        print
        for i in sorted(keys):
            print row[keys[i]], '\t',

    csvfile.close()