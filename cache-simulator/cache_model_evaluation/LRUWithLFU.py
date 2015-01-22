#!/usr/bin/env python

import sys
import operator
from LRU import LRUCache
from LRU import CACHE_FRESHER_ID
from LRU import CACHE_SIZE


class LRUWithLFUCache(LRUCache):

    def __init__(self, cache_size, lookback_count):
        """
        Initalizes this LRUWithLFUCache
        """
        LRUCache.__init__(self, cache_size)
        self._lookback_count = lookback_count
        self._cache_frequencies = {}

    def cache_object(self, obj_id, size, xtime, next_line=None, force=True):
        # No, this goes horribly wrong because LRUCache uses
        # this method internally to put the obj_id on top of the stack
        # self._cache_frequencies[obj_id] = 0
        if obj_id not in self._cache_frequencies:
            self._cache_frequencies[obj_id] = 0
        LRUCache.cache_object(self, obj_id, size, xtime)

    def get_cached(self, obj_id, xtime, next_line=None):
        if obj_id in self._cache_frequencies:
            self._cache_frequencies[obj_id] += 1
        else:
            self._cache_frequencies[obj_id] = 1
        return LRUCache.get_cached(self, obj_id, xtime)

    def _evict_bytes(self, bytes_to_be_evicted, xtime):
        size_before = self._max_size - self._used_size
        if bytes_to_be_evicted > self._max_size:
            raise Exception("Cache too small.")

        evicted_bytes = 0
        windows = 1
        lookback_count = self._lookback_count * windows
        obj_ids_in_window = {}
        sum_sizes = 0
        i = 0
        window_sufficient = False

        while not window_sufficient:

            previous_obj_id = self._cached_objects[self._oldest_obj_id][CACHE_FRESHER_ID]
            obj_ids_in_window[self._oldest_obj_id] = self._cache_frequencies[self._oldest_obj_id]
            sum_sizes += self._cached_objects[self._oldest_obj_id][CACHE_SIZE]
            while previous_obj_id is not None and i < lookback_count:
                obj_ids_in_window[previous_obj_id] = self._cache_frequencies[previous_obj_id]
                sum_sizes += self._cached_objects[previous_obj_id][CACHE_SIZE]
                i += 1
                previous_obj_id = self._cached_objects[previous_obj_id][CACHE_FRESHER_ID]

            if sum_sizes >= bytes_to_be_evicted:
                window_sufficient = True
                sorted_obj_ids_in_window = sorted(obj_ids_in_window.keys())
                # print obj_ids_in_window
                # print sorted_obj_ids_in_window
                j = 0
                while evicted_bytes < bytes_to_be_evicted:
                    # remove obj_id in decreasing order of frequency
                    freed_bytes = self.remove_cached(sorted_obj_ids_in_window[j][0])
                    if freed_bytes == 0:
                        raise Exception("remove_cached() returned 0.")
                    evicted_bytes += freed_bytes
                    self._stats["evicted_objects"] += 1
                    j += 1
            else:
                windows += 1
                lookback_count = self._lookback_count * windows
                obj_ids_in_window = {}
                sum_sizes = 0
                i = 0

        size_after = self._max_size - self._used_size
        assert (size_after > size_before)

    def debug_print(self):
        """
        A debug function used to print the contents of the cache.
        """
        print ("---------")
        print ("num_cached_objects: %s" % self.get_num_cached_objects())
        print ("get_free_cache_bytes: %s" % self.get_free_cache_bytes(None))
        print ("oldest_obj_id: %s (%d)" % (self._oldest_obj_id, self._cache_frequencies[self._oldest_obj_id]))
        print ("freshest_obj_id: %s (%d)" % (self._freshest_obj_id, self._cache_frequencies[self._freshest_obj_id]))
        i = 0
        msg = "oldest -> %s (%d/%d)" % (self._oldest_obj_id, self._cache_frequencies[self._oldest_obj_id],
                                        self._cached_objects[self._oldest_obj_id][CACHE_SIZE])
        previous_obj_id = self._cached_objects[self._oldest_obj_id][CACHE_FRESHER_ID]
        while previous_obj_id is not None and i < self._lookback_count:
            msg = "%s -> %s (%d/%d)" % (msg, previous_obj_id, self._cache_frequencies[previous_obj_id],
                                        self._cached_objects[previous_obj_id][CACHE_SIZE])
            i += 1
            previous_obj_id = self._cached_objects[previous_obj_id][CACHE_FRESHER_ID]
        print "%s -> lookback_count reached" % msg
        print self._cached_objects

    def rename(self, from_obj_id, to_obj_id):

        if from_obj_id in self._cached_objects:

            old_freq = self._cache_frequencies.pop(from_obj_id)
            self._cache_frequencies.pop([to_obj_id] = old_freq

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

def main():
    cache = LRUWithLFUCache(10000, 2)

    cache.cache_object('a', 1000, 10)
    cache.cache_object('b', 2000, 20)
    cache.cache_object('c', 3000, 30)
    cache.cache_object('d', 4000, 40)

    cache.debug_print()

    cache.get_cached('a', 50)

    cache.debug_print()

    cache.get_cached('d', 60)

    cache.debug_print()

    cache.get_cached('b', 70)
    cache.get_cached('b', 80)
    cache.get_cached('b', 90)
    cache.get_cached('b', 100)

    cache.debug_print()

    cache.get_cached('a', 110)

    cache.debug_print()

    cache.get_cached('c', 120)
    cache.get_cached('c', 130)

    cache.debug_print()

    cache.cache_object('e', 5000, 140)

    cache.debug_print()

    cache.get_cached('c', 150)
    cache.get_cached('b', 160)

    cache.debug_print()

    cache.cache_object('f', 1000, 170)

    cache.debug_print()

    cache.cache_object('g', 1000, 170)
    cache.cache_object('h', 1000, 180)
    cache.cache_object('i', 1000, 190)
    cache.cache_object('j', 1000, 200)

    cache.debug_print()

    cache.cache_object('k', 7000, 220)

    cache.debug_print()

if __name__ == "__main__":
    sys.exit(main())