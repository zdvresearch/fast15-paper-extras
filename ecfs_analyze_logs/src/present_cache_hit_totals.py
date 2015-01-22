# #!/usr/bin/env python

import os
import sys
import json

ecfs_base_cache_hit_ratio_file = sys.argv[1]

with open(ecfs_base_cache_hit_ratio_file, 'r') as f:
    ratios = json.load(f)

    disk_requests = 0
    tape_requests = 0
    
    disk_bytes = 0
    tape_bytes = 0

    for date, values in ratios.items():
        disk_requests += values["total"]["disk_requests"]
        tape_requests += values["total"]["tape_requests"]
        disk_bytes += values["total"]["disk_size"]
        tape_bytes += values["total"]["tape_size"]

print ("requests:" , float(disk_requests) / (disk_requests + tape_requests))
print ("bytes:" , float(disk_bytes) / (disk_bytes + tape_bytes))
