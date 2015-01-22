#!/usr/bin/env python

import sys

#sys.path.insert(0, '/usr/local/lib/python2.7/dist-packages/bintrees')

import CacheStats
from bintrees import AVLTree


CACHE_ATIME = 0
CACHE_SIZE = 1
CACHE_OBJ_ID = 2
CACHE_NEXT_TIME = 3

class BeladyCache():
   
    def __init__(self, cache_size, min_obj_size, max_obj_size):

        self._max_size = cache_size
        self._used_size = 0
        # dictionary: obj_id -> object with last and next caching time
        self._cached_objects = {}
        # AVL tree: next_time -> object with last and next caching time
        self._tree = AVLTree()
        self._oldest_obj_id = None
        self._freshest_obj_id = None

        self.stats = CacheStats.CacheStats("Belady", cache_size)
        self.daily_stats = CacheStats.DailyCacheStats(cache_size)

    def get_cache_stats_total(self):
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

    def get_free_cache_bytes(self):
        return self._max_size - self._used_size

    def update_obj_size(self, obj_id, size, delta):
        if obj_id in self._cached_objects:
            # update size of object in cache
            self._cached_objects[obj_id][CACHE_SIZE] = size
            # update size of object in tree
            next_time = self._cached_objects[obj_id][CACHE_NEXT_TIME] # ineffizient: zwei Zugriffe
            self._tree[next_time][CACHE_SIZE] = size
            # update size used in cache
            self._used_size += delta
            # TODO: Muesste man nicht noch pruefen, ob Cachegroesse ueberschritten?

    def _evict_bytes(self, bytes, xtime):
        if self.stats.first_eviction_ts == 0:
            self.stats.first_eviction_ts = xtime
        # remove objects from cache
        evicted_bytes = 0
        while evicted_bytes < bytes:
            # remove object with largest next_line_number from tree
            (next_line_number, obj) = self._tree.pop_max()
            # remove same object from cache
            evicted_bytes += self._remove_cached(obj[CACHE_OBJ_ID])
            # update stats
            self.stats.cached_objects_current -= 1
            self.stats.evicted_objects += 1
            self.daily_stats.evicted_objects += 1

    def remove_cached(self, obj_id):
        if self.is_cached(obj_id):
            self.stats.deleted_objects += 1
            self.stats.cached_objects_current -= 1
            self.daily_stats.deleted_objects += 1
            return self._remove_cached(obj_id)
        return None

    def _remove_cached(self, obj_id):
        if obj_id in self._cached_objects:
            # remove object from cache
            obj = self._cached_objects.pop(obj_id)
            # remove object from tree
            next_line_number = obj[CACHE_NEXT_TIME]
            self._tree.discard(next_line_number)
            # adapt size
            self._used_size -= obj[CACHE_SIZE]
            return obj[CACHE_SIZE]
        return 0

    def cache_object(self, obj_id, size, xtime, next_line_number, force=True, is_new=False):
        # do not cache object if next_line_number == -1
        if next_line_number == -1:
            return
        # add object to cache
        self._cached_objects[obj_id] = [xtime, size, obj_id, next_line_number]
        # add new object to tree
        self._tree[next_line_number] = [xtime, size, obj_id, next_line_number]
        # update size
        self._used_size += size
        # remove other objects from cache if necessary
        if self._used_size > self._max_size:
            bytes = self._used_size - self._max_size
            self._evict_bytes(bytes, next_line_number)
        # check whether cache is large enough
        if self._used_size > self._max_size:
            # remove new object
            self._cached_objects.pop(obj_id)
            self._tree.discard(next_line_number)
            raise Exception("Error, cannot cache file. Size to large: %s %d" % (obj_id, size))
        # update stats
        self.stats.cached_objects_current += 1
        self.stats.cached_objects_total += 1
        self.stats.cached_bytes_written += size
        self.daily_stats.cached_objects += 1
        self.daily_stats.cached_bytes_written += size

    def get_cached(self, obj_id, xtime, next_line):
        # GET
        if obj_id in self._cached_objects:
            # remove object from cache
            size = self._remove_cached(obj_id)
            # add object with new time to cache
            self.cache_object(obj_id, size, xtime, next_line)
            # update stats
            self.stats.cache_hits += 1
            self.stats.cached_bytes_read += size
            self.daily_stats.cache_hits += 1
            self.daily_stats.cached_bytes_read += size
            return True
        # update stats
        self.stats.cache_misses += 1
        self.daily_stats.cache_misses += 1
        return False

    def rename(self, from_obj_id, to_obj_id):
        # Belady cache stores from_obj only if to_obj is accessed (GET), possibly after a RENAME chain
        # Belady cache does not store to_obj because it will be overwritten by this function
        if self.is_cached(to_obj_id):
            raise Exception("Error in rename(...): File cached that is not needed.")
        if self.is_cached(from_obj_id):
            # retrieve object and store it under new ID
            obj = self._cached_objects.pop(from_obj_id)
            self._cached_objects[to_obj_id] = obj
            # update ID of object in tree
            next_line_number = obj[CACHE_NEXT_TIME]
            self._tree[next_line_number][CACHE_OBJ_ID] = to_obj_id



def main(argv=None):

    cache = BeladyCache(168884986026393600)
    """
    cache.cache_object('a', 1000, 10)
    print (json.dumps(cache._cached_objects, indent=2))
    print ("oldest_obj_id", cache._oldest_obj_id)
    print ("freshest_obj_id", cache._freshest_obj_id)
    print ("====================================")

    cache.cache_object('b', 2000, 20)
    print (json.dumps(cache._cached_objects, indent=2))
    print ("oldest_obj_id", cache._oldest_obj_id)
    print ("freshest_obj_id", cache._freshest_obj_id)
    print ("====================================")

    cache.cache_object('c', 2000, 30)
    print (json.dumps(cache._cached_objects, indent=2))
    print ("oldest_obj_id", cache._oldest_obj_id)
    print ("freshest_obj_id", cache._freshest_obj_id)
    print ("====================================")

    cache.cache_object('d', 3000, 40)
    print (json.dumps(cache._cached_objects, indent=2))
    print ("oldest_obj_id", cache._oldest_obj_id)
    print ("freshest_obj_id", cache._freshest_obj_id)
    print ("====================================")
    print (json.dumps(cache._cached_objects, indent=2))


    cache._remove_cached('a')
    print (json.dumps(cache._cached_objects, indent=2))
    print ("oldest_obj_id", cache._oldest_obj_id)
    print ("freshest_obj_id", cache._freshest_obj_id)
    print ("====================================")

    cache._remove_cached('d')
    print (json.dumps(cache._cached_objects, indent=2))
    print ("oldest_obj_id", cache._oldest_obj_id)
    print ("freshest_obj_id", cache._freshest_obj_id)
    print ("====================================")

    cache.cache_object('e', 3000, 80)
    print (json.dumps(cache._cached_objects, indent=2))
    print ("oldest_obj_id", cache._oldest_obj_id)
    print ("freshest_obj_id", cache._freshest_obj_id)
    print ("====================================")

    cache.get_cached('b', 90)
    print (json.dumps(cache._cached_objects, indent=2))
    print ("oldest_obj_id", cache._oldest_obj_id)
    print ("freshest_obj_id", cache._freshest_obj_id)
    print ("====================================")

    # test renaming.
    ## create 3 objects. rename the freshest, rename the oldest, rename the middle
    c2 = LRUCache(10000)
    c2.cache_object('old', 100, 1)
    c2.cache_object('middle', 100, 2)
    c2.cache_object('fresh', 100, 3)

    assert(c2._freshest_obj_id == "fresh")
    assert(c2._oldest_obj_id == "old")

    c2.rename("old", "new_old")
    assert(c2._oldest_obj_id == "new_old")

    c2.rename("fresh", "new_fresh")
    assert(c2._freshest_obj_id == "new_fresh")

    c2.rename("middle", "new_middle")
    assert(c2._cached_objects["new_middle"][CACHE_FRESHER_ID] == "new_fresh")
    assert(c2._cached_objects["new_middle"][CACHE_OLDER_ID] == "new_old")
    """
if __name__ == "__main__":
    sys.exit(main())