#!/usr/bin/env python

import sys
import json
import random
import time
import os

import CacheStats

import AbstractCache

from LRU import LRUCache
from Fifo import FifoCache

CACHE_ATIME = 0
CACHE_SIZE = 1
CACHE_FRESHER_ID = 2
CACHE_OLDER_ID = 3
       
class SplitLRUCache(AbstractCache.AbstractCache):

    def __init__(self, cache_size, min_obj_size, max_obj_size):

        """
            cache_size in bytes.
        """
        self._max_size = cache_size

        self.stats = CacheStats.CacheStats("LRU", cache_size)
        self.daily_stats = CacheStats.DailyCacheStats(cache_size)

        ts = int(time.time())

        get_size = int(0.333333 * cache_size)
        put_size = int(0.666666 * cache_size)
        self.get_lru = LRUCache(get_size, 0, 0)
        self.put_fifo = LRUCache(put_size, 0, 0)


    def get_cache_stats_total(self):
        return self.stats.to_dict()

    def get_cache_stats_day(self):
        # self.daily_stats.cache_used = self._used_size
        s = self.daily_stats.to_dict()
        self.daily_stats.reset()
        return s

    def get_num_cached_objects(self):
        return self.get_lru.get_num_cached_objects() + self.put_fifo.get_num_cached_objects()

    def is_cached(self, obj_id):
        return self.get_lru.is_cached(obj_id) or self.put_fifo.is_cached(obj_id)

    def is_remembered(self, obj_id):
        return self.is_cached(obj_id)

    def get_free_cache_bytes(self):
        return self.get_lru.get_free_cache_bytes() + self.put_fifo.get_free_cache_bytes() 

    def update_obj_size(self, obj_id, size, delta):
        if self.get_lru.is_cached(obj_id):
            self.get_lru.update_obj_size(obj_id, size, delta)
        if self.put_fifo.is_cached(obj_id):
            self.put_fifo.update_obj_size(obj_id, size, delta)

    def remove_cached(self, obj_id):

        if self.get_lru.is_cached(obj_id):
            self.stats.deleted_objects += 1
            self.stats.cached_objects_current -= 1
            self.daily_stats.deleted_objects += 1
            return self.get_lru.remove_cached(obj_id)

        if self.put_fifo.is_cached(obj_id):
            self.stats.deleted_objects += 1
            self.stats.cached_objects_current -= 1
            self.daily_stats.deleted_objects += 1
            return self.put_fifo.remove_cached(obj_id)

        return None

    def cache_object(self, obj_id, size, xtime, next_line=None, force=True, is_new=False):
        if is_new:
            self.put_fifo.cache_object(obj_id, size, xtime, next_line, force)
        else:
            self.put_fifo.cache_object(obj_id, size, xtime, next_line, force)            


    def get_cached(self, obj_id, xtime, next_line=None):
        if self.get_lru.is_cached(obj_id) or self.put_fifo.is_cached(obj_id):
            self.stats.cache_hits += 1
            self.daily_stats.cache_hits += 1
            return True

        self.stats.cache_misses += 1
        self.daily_stats.cache_misses += 1
        return False

    def rename(self, from_obj_id, to_obj_id):
        self.get_lru.rename(from_obj_id, to_obj_id)
        self.put_fifo.rename(from_obj_id, to_obj_id)

    def check_sanity(self):
        return True

    def dump_cache(self, reason):
        print ("dump")

    # def dump_cache(self, reason):

    #     print("===================================")
    #     print("freshest: %r" % self._freshest_obj_id)

    #     print ("oldest: %r" % self._oldest_obj_id)
    #     print ("num_elems: %r" % len(self._cached_objects))

    #     ptr = self._freshest_obj_id

    #     print("============CACHE_DUMP===================")
    #     for obj_id, obj in self._cached_objects.items():
    #         print ("%r \t -> %r" % (obj_id, obj))

    #     print("============/CACHE_DUMP==================")

    #     for x in range(len(self._cached_objects)):
    #         obj = self._cached_objects[ptr]
    #         print ("%r \t %r->%r" % (x, ptr, obj))
    #         ptr = obj[CACHE_OLDER_ID]
    #     print("===================================")
    #     print("freshest: %r" % self._freshest_obj_id)
    #     print ("oldest: %r" % self._oldest_obj_id)
    #     print ("num_elems: %r" % len(self._cached_objects))
    #     print ("reason: %s "% reason)

    #     print("===================================")

    #     with open(os.path.join("/", "tmp", "dumpfile"), 'w') as ff:
    #         ff.write(json.dumps(self.get_cache_stats_day(), indent=4, sort_keys=True))
    #         ff.write("===================================\n")
    #         ff.write(json.dumps(self.get_cache_stats_total(), indent=4, sort_keys=True))
    #         ff.write("===================================\n")
    #         ff.write("freshest: %r\n" % self._freshest_obj_id)

    #         ff.write ("oldest: %r\n" % self._oldest_obj_id)
    #         ff.write ("num_elems: %r\n" % len(self._cached_objects))

    #         ptr = self._freshest_obj_id
    #         for x in range(len(self._cached_objects)):
    #             obj = self._cached_objects[ptr]
    #             ff.write("%r \t %r->%r\n" % (x, ptr, obj))
    #             ptr = obj[CACHE_OLDER_ID]
    #         ff.write("===================================\n")
    #         ff.write("freshest: %r\n" % self._freshest_obj_id)
    #         ff.write ("oldest: %r\n" % self._oldest_obj_id)
    #         ff.write ("num_elems: %r\n" % len(self._cached_objects))
    #         ff.write ("reason: %s "% reason)


def main(argv=None):
    # test_a()
    # test_b()
    # test_c()
    # test_d()
    test_e()
    #
    #

if __name__ == "__main__":
    sys.exit(main())
