#!/usr/bin/env python

import sys
import json
import CacheStats

import AbstractCache

# cached objects gets a stub on first access, full data on second.
# stub has cached_size=0, full file has cached_size == real_size
CACHE_ATIME = 0
CACHE_CACHED_SIZE = 1
CACHE_REAL_SIZE = 2
CACHE_FRESHER_ID = 3
CACHE_OLDER_ID = 4

class LRU2NDCache(AbstractCache.AbstractCache):
    """
        LRU with second chance.
        On first request, create a stub entry in the cache, get data from tape, but do not cache to disk.
        On second request, get data from tape and store to cache.
        Empty stubs are evicted in the same manner as normal files.
    """
    def __init__(self, cache_size, min_obj_size, max_obj_size):

        """
            cache_size in bytes.
        """
        self._max_size = cache_size
        self._used_size = 0

        # obj_id -> last_atime
        self._cached_objects = {}
        
        self._oldest_obj_id = None
        self._freshest_obj_id = None

        self.stats = CacheStats.CacheStats("LRU2ND", cache_size)
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

    def is_stub(self, obj_id):
        if obj_id in self._cached_objects:
            if self._cached_objects[obj_id][CACHE_CACHED_SIZE] == 0:
                return True
        return False

    def get_free_cache_bytes(self, size):
        return self._max_size - self._used_size
    
    def update_obj_size(self, obj_id, size, delta):
        if obj_id in self._cached_objects:
            if self._cached_objects[obj_id][CACHE_CACHED_SIZE] > 0:
                # only update if it was cached previously
                self._cached_objects[obj_id][CACHE_CACHED_SIZE] = size
            self._cached_objects[obj_id][CACHE_REAL_SIZE] = size
            self._used_size += delta

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
            freed_bytes = self._remove_cached(self._oldest_obj_id)

            if freed_bytes == None:
                print("remove for evicted object failed! %r" % self._oldest_obj_id)
                sys.exit(1)

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
        return None

    def _remove_cached(self, obj_id):
        if self.is_cached(obj_id):
            obj = self._cached_objects.pop(obj_id)
            fresher_id = obj[CACHE_FRESHER_ID]
            older_id = obj[CACHE_OLDER_ID]

            # the oldest does not have an older id
            if older_id:
                if fresher_id:
                    self._cached_objects[older_id][CACHE_FRESHER_ID] = fresher_id
                else:
                    self._cached_objects[older_id][CACHE_FRESHER_ID] = None

            # the freshest does not have a fresher id
            if fresher_id:
                if older_id:
                    self._cached_objects[fresher_id][CACHE_OLDER_ID] = older_id
                else:
                    self._cached_objects[fresher_id][CACHE_OLDER_ID] = None

            # the oldest was removed
            if obj_id == self._oldest_obj_id:
                self._oldest_obj_id = fresher_id

            # the freshest was removed
            if obj_id == self._freshest_obj_id:
                self._freshest_obj_id = older_id

            self._used_size -= obj[CACHE_CACHED_SIZE]
            return obj[CACHE_CACHED_SIZE]

        return None

    def cache_object(self, obj_id, size, xtime, force=True):
        """
        :param obj_id:
        :param size:
        :param xtime:
        :param stub: false, if the file is actually cached, false on stub.
        :return:
        """
        # if verbose:
            # print ("_cache_object %s size: %d available_cache: %d" % (obj_id, size, (self._max_size - self._used_size)))

        if self.is_cached(obj_id):
            if self._cached_objects[obj_id][CACHE_CACHED_SIZE] == 0:
                has_stub = True
            else:
                raise Exception("ERROR: WRITING EXISTING ELEMENT!!! -> %r - ts: %r forced: %r" % (obj_id, xtime, force))
        else:
            has_stub = False


        # this either is a new file, or a second request for it. Then finally cache it.
        if force or has_stub:
            if self._used_size + size > self._max_size:
                # if verbose:
                    # print ("_evict required for %d" % ((self._used_size + size) - self._max_size))
                self._evict_bytes(((self._used_size + size) - self._max_size), xtime)

            if size <= (self._max_size - self._used_size):

                if has_stub:
                    self._cached_objects[obj_id][CACHE_CACHED_SIZE] = size
                    self.move_to_freshest(obj_id)
                else:
                    # force to write a full new entry
                    # newest object is always the freshest as has no fresher element.
                    self._cached_objects[obj_id] = [xtime, size, size, None, self._freshest_obj_id]

                    # if existing, update the next older entry
                    if self._freshest_obj_id != None:
                        self._cached_objects[self._freshest_obj_id][CACHE_FRESHER_ID] = obj_id
                    self._freshest_obj_id = obj_id

                    if self._oldest_obj_id == None:
                        # this is the first element and therefore the oldest as well.
                        self._oldest_obj_id = obj_id

                self._used_size += size

                self.stats.cached_objects_current += 1
                self.stats.cached_objects_total += 1
                self.stats.cached_bytes_written += size

                self.daily_stats.cached_objects += 1
                self.daily_stats.cached_bytes_written += size

            else:
                raise Exception("Error, cannot cache file. Size to large: %s %d" % (obj_id, size))

        else:
            # models the first cache request to a file that already exists. Just create a stub.
            # create stub file
            self._cached_objects[obj_id] = [xtime, 0, size, None, self._freshest_obj_id]

            # if existing, update the next older entry
            if self._freshest_obj_id != None:
                self._cached_objects[self._freshest_obj_id][CACHE_FRESHER_ID] = obj_id
            self._freshest_obj_id = obj_id

            if self._oldest_obj_id == None:
                # this is the first element and therefore the oldest as well.
                self._oldest_obj_id = obj_id

            self.stats.misc["cached_stubs"] += 1

    def move_to_freshest(self, obj_id):
        obj = self._cached_objects[obj_id]
        prev_freshest = self._freshest_obj_id

        # has fresher and older?
        older_id = obj[CACHE_OLDER_ID]
        fresher_id = obj[CACHE_FRESHER_ID]

        if obj_id == prev_freshest:
            # object already is the freshest
            return

        if older_id:
            # was taken from 'the middle' restore doubly linked list
            self._cached_objects[fresher_id][CACHE_OLDER_ID] = older_id
            self._cached_objects[older_id][CACHE_FRESHER_ID] = fresher_id

            self._freshest_obj_id = obj_id

            obj[CACHE_FRESHER_ID] = None
            obj[CACHE_OLDER_ID] = prev_freshest
            self._cached_objects[prev_freshest][CACHE_FRESHER_ID] = obj_id
            return

        # this was the oldest entry.
        if fresher_id:
            self._freshest_obj_id = obj_id

            obj[CACHE_FRESHER_ID] = None
            obj[CACHE_OLDER_ID] = prev_freshest
            self._cached_objects[prev_freshest][CACHE_FRESHER_ID] = obj_id

            self._cached_objects[fresher_id][CACHE_OLDER_ID] = None
            self._oldest_obj_id = fresher_id
            return

    def get_cached(self, obj_id, xtime):
        if self.is_cached(obj_id):
            self.move_to_freshest(obj_id)
            size = self._cached_objects[obj_id][CACHE_CACHED_SIZE]

            if size == 0:
                # this was a stub
                self.stats.cache_misses += 1
                self.daily_stats.cache_misses += 1
                return False

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
            obj = self._cached_objects.pop(from_obj_id)

            fresher_id = obj[CACHE_FRESHER_ID]
            older_id = obj[CACHE_OLDER_ID]

            # the oldest does not have an older id
            if older_id:
                self._cached_objects[older_id][CACHE_FRESHER_ID] = to_obj_id

            # the freshest does not have a fresher id
            if fresher_id:
                self._cached_objects[fresher_id][CACHE_OLDER_ID] = to_obj_id


            if self._freshest_obj_id == from_obj_id:
                self._freshest_obj_id = to_obj_id

            if self._oldest_obj_id == from_obj_id:
                self._oldest_obj_id = to_obj_id

            self._cached_objects[to_obj_id] = obj

    def check_sanity(self):
        return True

def main(argv=None):
    cache = LRUARCCache(10000)

    timer = 10
    cache.cache_object('a', 1000, timer, force=False)
    timer += 1
    assert(cache.is_stub('a'))

    assert (cache.get_cached('a', timer) is False)
    timer += 1

    print (json.dumps(cache._cached_objects, indent=2))
    print ("oldest_obj_id", cache._oldest_obj_id)
    print ("freshest_obj_id", cache._freshest_obj_id)
    print ("====================================")

    ## this is a second write request to an object, therefore it is cached after that.
    cache.cache_object('a', 1000, timer, force=False)
    timer += 1

    assert(cache.get_cached('a', timer) is True)
    timer += 1
    assert(cache.is_stub('a') is False)



    cache.cache_object('b', 1337, timer, force=True)
    timer += 1
    assert(cache.is_stub('b') is False)
    assert(cache.get_cached('b', timer) is True)

    print (json.dumps(cache._cached_objects, indent=2))
    print ("oldest_obj_id", cache._oldest_obj_id)
    print ("freshest_obj_id", cache._freshest_obj_id)
    print ("====================================")


    # cache.cache_object('b', 2000, 20)
    # print (json.dumps(cache._cached_objects, indent=2))
    # print ("oldest_obj_id", cache._oldest_obj_id)
    # print ("freshest_obj_id", cache._freshest_obj_id)
    # print ("====================================")
    #
    # cache.cache_object('c', 2000, 30)
    # print (json.dumps(cache._cached_objects, indent=2))
    # print ("oldest_obj_id", cache._oldest_obj_id)
    # print ("freshest_obj_id", cache._freshest_obj_id)
    # print ("====================================")
    #
    # cache.cache_object('d', 3000, 40)
    # print (json.dumps(cache._cached_objects, indent=2))
    # print ("oldest_obj_id", cache._oldest_obj_id)
    # print ("freshest_obj_id", cache._freshest_obj_id)
    # print ("====================================")
    # print (json.dumps(cache._cached_objects, indent=2))
    #
    #
    # cache.remove_cached('a')
    # print (json.dumps(cache._cached_objects, indent=2))
    # print ("oldest_obj_id", cache._oldest_obj_id)
    # print ("freshest_obj_id", cache._freshest_obj_id)
    # print ("====================================")
    #
    # cache.remove_cached('d')
    # print (json.dumps(cache._cached_objects, indent=2))
    # print ("oldest_obj_id", cache._oldest_obj_id)
    # print ("freshest_obj_id", cache._freshest_obj_id)
    # print ("====================================")
    #
    # cache.cache_object('e', 3000, 80)
    # print (json.dumps(cache._cached_objects, indent=2))
    # print ("oldest_obj_id", cache._oldest_obj_id)
    # print ("freshest_obj_id", cache._freshest_obj_id)
    # print ("====================================")
    #
    # cache.get_cached('b', 90)
    # print (json.dumps(cache._cached_objects, indent=2))
    # print ("oldest_obj_id", cache._oldest_obj_id)
    # print ("freshest_obj_id", cache._freshest_obj_id)
    # print ("====================================")
    #
if __name__ == "__main__":
    sys.exit(main())