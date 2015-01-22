#!/usr/bin/env python

"""repack.py: unpacks all *.Z files and rewrites them as *.gz"""
__author__ = "Matthias Grawinkel (grawinkel@uni-mainz.de)"
__status__ = "Production"


import os
import glob

files = glob.glob('*.Z')

for source in files:
    target = "%s.gz" % (os.path.basename(source)[:-2])
    print source, target
    os.system("zcat %s | gzip > %s" % (source,target))
    os.system("rm %s" % source)


# TODO: currently, this file needs to be in the logs directory to work. or it needs to be called from cwd=logs dir.