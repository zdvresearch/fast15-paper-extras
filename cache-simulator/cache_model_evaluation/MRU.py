#!/usr/bin/env python

import sys
import json
import random
import time
import os

import CacheStats

import LRU

CACHE_ATIME = 0
CACHE_SIZE = 1
CACHE_FRESHER_ID = 2
CACHE_OLDER_ID = 3
       
class MRUCache(LRU.LRUCache):
    """
    Most Recently Used (MRU): http://en.wikipedia.org/wiki/Cache_algorithms
    Discards, in contrast to LRU, the most recently used items first.
    In findings presented at the 11th VLDB conference, Chou and Dewitt noted that
    "When a file is being repeatedly scanned in a [Looping Sequential] reference pattern,
    MRU is the best replacement algorithm."[3] Subsequently other researchers presenting
    at the 22nd VLDB conference noted that for random access patterns and repeated scans
    over large datasets (sometimes known as cyclic access patterns) MRU cache algorithms
    have more hits than LRU due to their tendency to retain older data.
    [4] MRU algorithms are most useful in situations where the older an item is, the more likely it is to be accessed.
    """


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
            freed_bytes = self._remove_cached(self._freshest_obj_id)

            if freed_bytes == None:
                print("remove for evicted object failed! %r" % self._freshest_obj_id)
                sys.exit(1)

            evicted_bytes += freed_bytes

            # update stats
            self.stats.cached_objects_current -= 1
            self.stats.evicted_objects += 1

            self.daily_stats.evicted_objects += 1

        size_after = self._max_size - self._used_size
        assert (size_after > size_before)



def test_a():
    c = MRUCache(10000)

    ts = 1
    c.cache_object('a', 1000, ts)
    c.get_cached('a', ts)
    c.remove_cached('a')

    ts += 1
    c.cache_object('b', 1000, ts)
    c.remove_cached('b')


def test_b():
    cache_size = 10000
    cache = MRUCache(cache_size)

    cache.cache_object('a', 1000, 10)
    cache.cache_object('b', 2000, 11)
    cache.cache_object('c', 6000, 12)
    cache.cache_object('d', 4000, 13)
    cache.check_sanity()

    assert('d' == cache._freshest_obj_id)
    assert(cache.is_cached('a'))
    assert(cache.is_cached('b'))
    assert(cache.is_cached('d'))
    assert(cache.is_cached('c') == False)


def main(argv=None):
    test_a()
    test_b()

if __name__ == "__main__":
    sys.exit(main())
