#!/usr/bin/env python

import sys
import CacheStats

CACHE_CREATION_TIMES = 0
CACHE_ATIMES = 1
TAPE_ATIMES = 2
MODIFICATION_TIMES = 3
CACHE_EVICTION_TIMES = 4

# size of the object
OBJ_SIZE = 0

# when was the object created? fist put
OBJ_CTIME = 1

# when was the object deleted?
OBJ_DTIME = 2


class StorageSystem():
    def __init__(self, cache):
        self._cache = cache
        self.stats = CacheStats.StorageStats()
        self.daily_stats = CacheStats.StorageStats()

    def get_cache(self):
        return self._cache

    def rename_object(self, from_obj_id, to_obj_id):
        self.stats.rename_requests += 1
        self.daily_stats.rename_requests += 1
        self._cache.rename(from_obj_id, to_obj_id)

    def put_object(self, obj_id, uid, size, xtime, next_line):
        self.stats.put_requests += 1
        self.daily_stats.put_requests += 1

        if self._cache.is_cached(obj_id):
            self.stats.put_overwrites += 1
            self.daily_stats.put_overwrites += 1
            # object already exists, this is an update

            #invalidate the cached instance first
            r = self._cache.remove_cached(obj_id)
            if r == None:
                raise Exception("object to overwrite was not removed! %r, size: %r" % (obj_id, size))

        # cache the (new) object
        # every new file has to go to the cache first, before being written to tape.
        # Therefore force this write, independent of the underlying caching strategy.
        self._cache.cache_object(obj_id, size, xtime, next_line, force=True, is_new=True)

        self.stats.put_requests += 1
        self.stats.bytes_written += size
        self.daily_stats.put_requests += 1
        self.daily_stats.bytes_written += size

    def get_object(self, obj_id, uid, size, xtime, next_line):
        is_cached = self._cache.get_cached(obj_id, size, xtime, next_line)
        if is_cached:
            self.stats.cache_hits += 1
            self.stats.cache_hits_bytes += size
            self.daily_stats.cache_hits += 1
            self.daily_stats.cache_hits_bytes += size
        else:
            # ask the system to get it from tape and put it to cache
            self.stats.cache_misses += 1
            self.stats.cache_misses_bytes += size
            self.daily_stats.cache_misses += 1
            self.daily_stats.cache_misses_bytes += size
            self._cache.cache_object(obj_id, size, xtime, next_line, force=False, is_new=False)
        self.stats.get_requests += 1
        self.stats.bytes_read += size
        self.daily_stats.get_requests += 1
        self.daily_stats.bytes_read += size

    def del_object(self, obj_id, uid, dtime):
        self._cache.remove_cached(obj_id)
        self.stats.del_requests += 1
        self.daily_stats.del_requests += 1

    def get_stats_total(self):
        s = dict()
        s["front"] = self.stats.to_dict()
        s["caches"] = self._cache.get_cache_stats_total()
        return s

    def get_stats_day(self):
        s = dict()
        s["front"] = self.daily_stats.to_dict()
        self.daily_stats.reset()

        s["caches"] = self._cache.get_cache_stats_day()

        return s

    def check_cache_sanity(self, line):
        return self._cache.check_sanity(line)
