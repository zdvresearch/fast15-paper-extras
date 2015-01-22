#!/usr/bin/env python

import sys
import json

import CacheStats
from RandomChoiceDict import RandomChoiceDict

CACHE_ATIME = 0
CACHE_SIZE = 1
       
class RandomCache():
   
    def __init__(self, cache_size, min_obj_size, max_obj_size):

        """
            cache_size in bytes.
        """
        self._max_size = cache_size
        self._used_size = 0

        self._cached_objects = RandomChoiceDict()

        self.stats = CacheStats.CacheStats("Fifo", cache_size)
        self.daily_stats = CacheStats.DailyCacheStats(cache_size)

    def get_cache_stats_total(self):
        self.stats.cache_used = self._used_size
        return self.stats.to_dict()

    def get_cache_stats_day(self):
        self.daily_stats.cache_used = self._used_size
        s = self.daily_stats.to_dict()
        self.daily_stats.reset()
        return s

    def get_num_cached_objects(self):
        return len(self._cached_objects)
    
    def is_cached(self, obj_id):
        return obj_id in self._cached_objects

    def is_remembered(self, obj_id):
        return self.is_cached(obj_id)

    def get_free_cache_bytes(self, size):
        return self._max_size - self._used_size
    
    def update_obj_size(self, obj_id, size, delta):
        pass

    def _evict_bytes(self, bytes, xtime):
        """
            evicts the last used objects and frees at least @bytes size.
        """
        if self.stats.first_eviction_ts == 0:
            self.stats.first_eviction_ts = xtime

        size_before = self._max_size - self._used_size
        if bytes > self._max_size:
            raise Exception("Cache too small.")

        # if verbose:
            # print ("_evict_bytes %d" % (bytes))
        
        evicted_bytes = 0
                       
        evicted_objects_cnt = 0
        while evicted_bytes < bytes:
            random_oid = self._cached_objects.random_key()

            freed_bytes = self._remove_cached(random_oid)
            evicted_bytes += freed_bytes

            # update stats
            self.stats.cached_objects_current -=1
            self.stats.evicted_objects += 1

            self.daily_stats.evicted_objects += 1

        size_after = self._max_size - self._used_size
        assert (size_after > size_before)

    def remove_cached(self, obj_id):
        if self.is_cached(obj_id):
            self.stats.deleted_objects += 1
            self.stats.cached_objects_current -= 1

            self.daily_stats.deleted_objects += 1

            return self._remove_cached(obj_id)
        return 0

    def _remove_cached(self, obj_id):
        if obj_id in self._cached_objects:
            obj = self._cached_objects.pop(obj_id)
            self._used_size -= obj[CACHE_SIZE]
            return obj[CACHE_SIZE]
        return 0

    def cache_object(self, obj_id, size, xtime, next_line=None, force=True, is_new=False):
        # if verbose:
            # print ("_cache_object %s size: %d available_cache: %d" % (obj_id, size, (self._max_size - self._used_size)))
        
        if self._used_size + size > self._max_size:
            # if verbose:
                # print ("_evict required for %d" % ((self._used_size + size) - self._max_size))
            self._evict_bytes(((self._used_size + size) - self._max_size), xtime)

        if size <= (self._max_size - self._used_size):
            self._cached_objects[obj_id] = (xtime, size)
            self._used_size += size

            self.stats.cached_objects_current += 1
            self.stats.cached_objects_total += 1
            self.stats.cached_bytes_written += size

            self.daily_stats.cached_objects += 1
            self.daily_stats.cached_bytes_written += size

        else:
            raise Exception("Error, cannot cache file. Size to large: %s %d" % (obj_id, size))

    def get_cached(self, obj_id, xtime, next_line=None):
        if obj_id in self._cached_objects:

            size = self._remove_cached(obj_id)
            self.cache_object(obj_id, size, xtime)

            self.stats.cache_hits += 1
            self.stats.cached_bytes_read += size

            self.daily_stats.cache_hits += 1
            self.daily_stats.cached_bytes_read += size
            return True

        self.stats.cache_misses += 1
        self.daily_stats.cache_misses += 1
        return False

    def rename(self, from_obj_id, to_obj_id):
        if from_obj_id in self._cached_objects:
            old = self._cached_objects.pop(from_obj_id)
            self._cached_objects[to_obj_id] = old

    def check_sanity(self):
        return True


def main(argv=None):
    cache = RandomCache(10000)

    cache.cache_object('a', 1000, 10)
    print (json.dumps(cache._cached_objects, indent=2))
    print ("====================================")
    
    cache.cache_object('b', 2000, 20)
    print (json.dumps(cache._cached_objects, indent=2))
    print ("====================================")
    
    cache.cache_object('c', 2000, 30)
    print (json.dumps(cache._cached_objects, indent=2))
    print ("====================================")
    
    cache.cache_object('d', 3000, 40)
    print (json.dumps(cache._cached_objects, indent=2))
    print ("====================================")
    print (json.dumps(cache._cached_objects, indent=2))

    cache._remove_cached('a')
    print (json.dumps(cache._cached_objects, indent=2))
    print ("====================================")
    
    cache._remove_cached('d')
    print (json.dumps(cache._cached_objects, indent=2))
    print ("====================================")

    cache.cache_object('e', 3000, 80)
    print (json.dumps(cache._cached_objects, indent=2))
    print ("====================================")
    
    cache.get_cached('b', 90)
    print (json.dumps(cache._cached_objects, indent=2))
    print ("====================================")
    
    
if __name__ == "__main__":
    sys.exit(main())