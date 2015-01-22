#!/usr/bin/env python

import sys
import json
import random
import time
import os

import CacheStats

import AbstractCache

CACHE_ATIME = 0
CACHE_SIZE = 1
CACHE_FRESHER_ID = 2
CACHE_OLDER_ID = 3
       
class LRUCache(AbstractCache.AbstractCache):

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

        self.stats = CacheStats.CacheStats("LRU", cache_size)
        self.daily_stats = CacheStats.DailyCacheStats(cache_size)

        ts = int(time.time())


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
        if obj_id in self._cached_objects:
            return True
        return False

    def is_remembered(self, obj_id):
        return self.is_cached(obj_id)

    def get_free_cache_bytes(self):
        return self._max_size - self._used_size

    def update_obj_size(self, obj_id, size, delta):
        if obj_id in self._cached_objects:
            self._cached_objects[obj_id][CACHE_SIZE] = size
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
            self.stats.cached_objects_current -= 1
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

            self._used_size -= obj[CACHE_SIZE]
            return obj[CACHE_SIZE]

        return None



    def cache_object(self, obj_id, size, xtime, next_line=None, force=True, is_new=False):
        # already handled in calling function
        # if obj_id in self._cached_objects:
        #     self.remove_cached(obj_id)

        if self.is_cached(obj_id):
            raise Exception("ERROR: WRITING EXISTING ELEMENT!!! -> %r - ts: %r forced: %r" % (obj_id, xtime, force))


        if self._used_size + size > self._max_size:
            # if verbose:
                # print ("_evict required for %d" % ((self._used_size + size) - self._max_size))
            self._evict_bytes(((self._used_size + size) - self._max_size), xtime)

        if size <= (self._max_size - self._used_size):

            # newest object is always the freshest as has no fresher element.
            self._cached_objects[obj_id] = [xtime, size, None, self._freshest_obj_id]

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

    def get_cached(self, obj_id, xtime, next_line=None):
        if self.is_cached(obj_id):
            self.move_to_freshest(obj_id)
            size = self._cached_objects[obj_id][CACHE_SIZE]

            self.stats.cache_hits += 1
            self.stats.cached_bytes_read += size

            self.daily_stats.cache_hits += 1
            self.daily_stats.cached_bytes_read += size

            return True

        self.stats.cache_misses += 1
        self.daily_stats.cache_misses += 1
        return False

    def rename(self, from_obj_id, to_obj_id):
        if self.is_cached(to_obj_id):
            self._remove_cached(to_obj_id)

        if self.is_cached(from_obj_id):

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

        seen_elems = set()
        ptr = self._freshest_obj_id

        max_elems = len(self._cached_objects)

        if max_elems < 2:
            return True

        for x in range (max_elems-2):
            obj = self._cached_objects[ptr]
            if ptr in seen_elems:
                print ("erroneous duplicate cache entry => %r: %r" % (ptr, obj))
                self.dump_cache("erroneous duplicate cache entry => %r: %r" % (ptr, obj))
                return False
            else:
                seen_elems.add(ptr)

            if ptr == obj[CACHE_FRESHER_ID]:
                print ("erroneous cache entry => %r: %r" % (ptr, obj))
                self.dump_cache("erroneous cache entry => %r: %r" % (ptr, obj))
                return False

            elif ptr == obj[CACHE_OLDER_ID]:
                print ("erroneous cache entry => %r: %r" % (ptr, obj))
                self.dump_cache("erroneous cache entry => %r: %r" % (ptr, obj))
                return False

            ptr = obj[CACHE_OLDER_ID]
            if ptr == None:
                self.dump_cache("ptr == None")
                return False

        obj = self._cached_objects[ptr]
        ptr = obj[CACHE_OLDER_ID]

        if ptr != self._oldest_obj_id:
            print ("last element is not the oldest")
            self.dump_cache("last element is not the oldest")
            return False

        return True

    def dump_cache(self, reason):

        print("===================================")
        print("freshest: %r" % self._freshest_obj_id)

        print ("oldest: %r" % self._oldest_obj_id)
        print ("num_elems: %r" % len(self._cached_objects))

        ptr = self._freshest_obj_id

        print("============CACHE_DUMP===================")
        for obj_id, obj in self._cached_objects.items():
            print ("%r \t -> %r" % (obj_id, obj))

        print("============/CACHE_DUMP==================")

        for x in range(len(self._cached_objects)):
            obj = self._cached_objects[ptr]
            print ("%r \t %r->%r" % (x, ptr, obj))
            ptr = obj[CACHE_OLDER_ID]
        print("===================================")
        print("freshest: %r" % self._freshest_obj_id)
        print ("oldest: %r" % self._oldest_obj_id)
        print ("num_elems: %r" % len(self._cached_objects))
        print ("reason: %s "% reason)

        print("===================================")

        with open(os.path.join("/", "tmp", "dumpfile"), 'w') as ff:
            ff.write(json.dumps(self.get_cache_stats_day(), indent=4, sort_keys=True))
            ff.write("===================================\n")
            ff.write(json.dumps(self.get_cache_stats_total(), indent=4, sort_keys=True))
            ff.write("===================================\n")
            ff.write("freshest: %r\n" % self._freshest_obj_id)

            ff.write ("oldest: %r\n" % self._oldest_obj_id)
            ff.write ("num_elems: %r\n" % len(self._cached_objects))

            ptr = self._freshest_obj_id
            for x in range(len(self._cached_objects)):
                obj = self._cached_objects[ptr]
                ff.write("%r \t %r->%r\n" % (x, ptr, obj))
                ptr = obj[CACHE_OLDER_ID]
            ff.write("===================================\n")
            ff.write("freshest: %r\n" % self._freshest_obj_id)
            ff.write ("oldest: %r\n" % self._oldest_obj_id)
            ff.write ("num_elems: %r\n" % len(self._cached_objects))
            ff.write ("reason: %s "% reason)


class Timer():
    def __init__(self, s):
        self.s = s

    def __enter__(self):
        self.start = time.time()

    def __exit__(self, *args):
        print ("%s: %fs" % (self.s, (time.time() - self.start)))


def test_a():
    c = LRUCache(10000)

    ts = 1
    c.cache_object('a', 1000, ts)
    c.get_cached('a', ts)
    c.remove_cached('a')

    ts += 1
    c.cache_object('b', 1000, ts)
    ts += 1
    c.cache_object('b', 1000, ts)
    c.remove_cached('b')


def test_b():
    cache_size = 10000
    cache = LRUCache(cache_size)

    cache.cache_object('a', 1000, 10)
    print (json.dumps(cache._cached_objects, indent=2))
    assert('a' == cache._oldest_obj_id)
    assert('a' == cache._freshest_obj_id)

    cache.cache_object('b', 2000, 20)
    print (json.dumps(cache._cached_objects, indent=2))
    assert('a' == cache._oldest_obj_id)
    assert('b' == cache._freshest_obj_id)


    cache.cache_object('c', 2000, 30)
    print (json.dumps(cache._cached_objects, indent=2))
    assert('a' == cache._oldest_obj_id)
    assert('c' == cache._freshest_obj_id)

    print ("====================================")

    cache.cache_object('d', 3000, 40)
    print (json.dumps(cache._cached_objects, indent=2))
    assert('a' == cache._oldest_obj_id)
    assert('d' == cache._freshest_obj_id)
    print ("====================================")
    print (json.dumps(cache._cached_objects, indent=2))


    cache.check_sanity()

    cache._remove_cached('a')
    print (json.dumps(cache._cached_objects, indent=2))
    assert('b' == cache._oldest_obj_id)
    assert('d' == cache._freshest_obj_id)

    print ("====================================")

    cache._remove_cached('d')
    print (json.dumps(cache._cached_objects, indent=2))
    assert('b' == cache._oldest_obj_id)
    assert('c' == cache._freshest_obj_id)

    print ("====================================")

    cache.cache_object('e', 3000, 80)
    print (json.dumps(cache._cached_objects, indent=2))
    assert('b' == cache._oldest_obj_id)
    assert('e' == cache._freshest_obj_id)
    print ("oldest: %s, newest %s  "% (cache._oldest_obj_id, cache._freshest_obj_id))
    print ("====================================")

    cache.get_cached('b', 90)
    print (json.dumps(cache._cached_objects, indent=2))
    print ("oldest: %s, newest %s  "% (cache._oldest_obj_id, cache._freshest_obj_id))
    assert('c' == cache._oldest_obj_id)
    assert('b' == cache._freshest_obj_id)
    print ("====================================")

    cache.remove_cached('b')
    cache.remove_cached('c')
    cache.remove_cached('e')


    free_bytes = cache.get_free_cache_bytes()
    cached_objects = cache.get_num_cached_objects()
    assert(free_bytes == cache_size)
    assert(cached_objects == 0)
    assert(cache._oldest_obj_id is None)
    assert(cache._freshest_obj_id is None)


def test_c():
    ## do some load test:
    c3 = LRUCache(500000)

    with Timer("load test"):
        for ts in range(0, 1000000):

            if ts % 10000 == 0:
                print ("running: %r" % ts)
            fsize = random.randint(1, 1000)

            c3.cache_object('o-%r' % ts, fsize, ts)

            c3.check_sanity()

            if ts > 20:
                # on a 10% chance, request a previously cached object:
                if random.randint(0, 100) < 10:
                    c3.remove_cached('o-%r' % random.randint(0, ts-1))

                if random.randint(0,100) < 3:
                    if random.randint(0,100) < 50:
                        c3.remove_cached(c3._freshest_obj_id)
                    else:
                        c3.remove_cached(c3._oldest_obj_id)


                # on a 50% chance, request a previouusly cached object:
                if random.randint(0, 100) < 50:
                    c3.get_cached('o-%r' % random.randint(0, ts-1), ts)


        print(json.dumps(c3.get_cache_stats().to_dict()))

    ts += 1
    c3.cache_object('xxx' , 100, ts)
    c3.remove_cached('xxx')

    c3.check_sanity()

def test_d():
    """
    put and rename
    :return:
    """
    c2 = LRUCache(10000)
    c2.cache_object('old', 100, 1)
    c2.cache_object('middle', 100, 2)
    c2.cache_object('fresh', 100, 3)

    assert(c2._freshest_obj_id == "fresh")
    assert(c2._oldest_obj_id == "old")
    c2.check_sanity()
    c2.rename("old", "new_old")
    assert(c2._oldest_obj_id == "new_old")
    c2.check_sanity()
    c2.rename("fresh", "new_fresh")
    assert(c2._freshest_obj_id == "new_fresh")
    c2.check_sanity()
    c2.rename("middle", "new_middle")
    assert(c2._cached_objects["new_middle"][CACHE_FRESHER_ID] == "new_fresh")
    assert(c2._cached_objects["new_middle"][CACHE_OLDER_ID] == "new_old")
    c2.check_sanity()

def test_e():
    """
    put and rename
    :return:
    """
    c2 = LRUCache(10000)

    c2.cache_object("X", 50, 10)

    for x in [100, 200, 300, 400, 500, 600, 700]:
        c2.cache_object('a_%r' % x, x, x)

    assert(c2.check_sanity())


    # now get, remove and reput x
    c2.get_cached("X", 800)
    assert(c2.check_sanity())
    c2.remove_cached("X")
    assert(c2.check_sanity())
    c2.cache_object("X", 50, 900)
    c2.get_cached("X", 1000)
    assert(c2.check_sanity())

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
