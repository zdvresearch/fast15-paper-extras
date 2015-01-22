#!/usr/bin/env python

import sys
from collections import OrderedDict

import CacheStats
import AbstractCache

CACHE_ATIME = 0
CACHE_SIZE = 1

'''
Implementation based on "A Self-tuning, Low Overhead Replacement Cache" (by N. Megiddo and D. Modha)
ARC: Adaptive Replacement Cache
LRU: least recently used
MRU: most recently used
'''
class ARCCache(AbstractCache.AbstractCache):

    def __init__(self, cache_size, min_obj_size, max_obj_size):

        # L1 caches objects that have been seen exactly once recently
        # L2 caches objects that have been seen at least twice recently
        # the top parts of L1 and L2 contain the objects that are currently cached
        # we only need the top and bottom parts of lists L1 and L2
        self._top1 = OrderedDict()
        self._bottom1 = OrderedDict()
        self._top2 = OrderedDict()
        self._bottom2 = OrderedDict()
        # cache_size in bytes
        self._max_size = cache_size
        # used_size = top1_size + top2_size
        self._used_size = 0
        self._top1_size = 0
        self._bottom1_size = 0
        self._bottom2_size = 0
        # average object size in bytes
        #self._avg_obj_size = (max_obj_size + min_obj_size) / 2
        # cache specific parameter: target size for top1 (as fraction of cache size)
        self._p = 0.
        # in ARC cache p is incremented (or decremented) by 1 and p in {0, 1, ..., c} where c = cache size in pages;
        # since here p in {0, ..., 1}, p must be incremented (or decremented) by 1/c, but instead of cache size in
        # pages we use cache size in avg_objects
        #self._step = self._avg_obj_size / cache_size
        # stats
        self.stats = CacheStats.CacheStats("ARC", cache_size)
        self.daily_stats = CacheStats.DailyCacheStats(cache_size)

    def get_cache_stats_total(self):
        return self.stats.to_dict()

    def get_cache_stats_day(self):
        self.daily_stats.cache_used = self._used_size
        s = self.daily_stats.to_dict()
        self.daily_stats.reset()
        return s

    def get_num_cached_objects(self):
        return len(self._top1) + len(self._top2)

    def is_cached(self, obj_id):
        return obj_id in self._top1 or obj_id in self._top2

    def is_remembered(self, obj_id):
        return obj_id in self._bottom1 or obj_id in self._bottom2 or self.is_cached(obj_id)

    def get_free_cache_bytes(self):
        return self._max_size - self._used_size

    def update_obj_size(self, obj_id, size, delta):
        if obj_id in self._top1:
            self._top1[obj_id][CACHE_SIZE] = size
            self._used_size += delta
            self._top1_size += delta
        elif obj_id in self._top2:
            self._top2[obj_id][CACHE_SIZE] = size
            self._used_size += delta
        elif obj_id in self._bottom1:
            self._bottom1[obj_id][CACHE_SIZE] = size
        elif obj_id in self._bottom2:
            self._bottom2[obj_id][CACHE_SIZE] = size

    '''
    def _evict_bytes(self, bytes, xtime):
        if self.stats.first_eviction_ts == 0:
            self.stats.first_eviction_ts = xtime
        evicted_bytes = 0
        while evicted_bytes < bytes:
            evicted_bytes += self._remove_cached(self._oldest_obj_id)[CACHE_SIZE]
            # update stats
            self.stats.cached_objects_current -= 1
            self.stats.evicted_objects += 1
            self.daily_stats.evicted_objects += 1
    '''

    def remove_cached(self, obj_id):
        if self.is_cached(obj_id):
            # update stats
            self.stats.deleted_objects += 1
            self.stats.cached_objects_current -= 1
            self.daily_stats.deleted_objects += 1
            # remove cached object
            obj = self._remove_cached(obj_id)
            if obj is None:
                raise Exception("obj is None.")
            return obj[CACHE_SIZE]
        if self.is_remembered(obj_id):
            obj = self._remove_remembered(obj_id)
            return obj[CACHE_SIZE]
        return None

    def _remove_remembered(self, obj_id):
        # this function is not used by any eviction strategy
        if obj_id in self._bottom1:
            return self._remove_from_bottom1(obj_id)
        if obj_id in self._bottom2:
            return self._remove_from_bottom2(obj_id)
        return self._remove_cached(obj_id)

    def _count_occurrences(self, obj_id):
        # was only used for testing
        counter = 0
        if obj_id in self._bottom1:
            counter += 1
        if obj_id in self._bottom2:
            counter += 1
        if obj_id in self._top1:
            counter += 1
        if obj_id in self._top2:
            counter += 1
        return counter

    def _remove_cached(self, obj_id):
        # this function is not used by any eviction strategy
        if obj_id in self._top1:
            return self._remove_from_top1(obj_id)
        if obj_id in self._top2:
            return self._remove_from_top2(obj_id)
        return None

    def _remove_from_bottom1(self, obj_id):
        obj = self._bottom1.pop(obj_id)
        self._bottom1_size -= obj[CACHE_SIZE]
        return obj

    def _remove_from_bottom2(self, obj_id):
        obj = self._bottom2.pop(obj_id)
        self._bottom2_size -= obj[CACHE_SIZE]
        return obj

    def _remove_from_top1(self, obj_id):
        # this function is not used by any eviction strategy
        obj = self._top1.pop(obj_id)
        self._used_size -= obj[CACHE_SIZE]
        self._top1_size -= obj[CACHE_SIZE]
        return obj

    def _remove_from_top2(self, obj_id):
        # this function is not used by any eviction strategy
        obj = self._top2.pop(obj_id)
        self._used_size -= obj[CACHE_SIZE]
        return obj

    def _move_lru_from_top1_to_bottom1(self):
        if len(self._top1) == 0:
            # TODO: Kommt leider vor; Cache im Paper schlecht beschrieben?
            return None
        # this function is (only) used by eviction function _replace(...)
        # stats are updated in _replace(...)
        (lru_obj_id, lru_obj) = self._top1.popitem(False)
        size = lru_obj[CACHE_SIZE]
        self._bottom1[lru_obj_id] = lru_obj
        self._used_size -= size
        self._top1_size -= size
        self._bottom1_size += size
        return lru_obj

    # done
    def _move_lru_from_top2_to_bottom2(self):
        # this function is (only) used by eviction function _replace(...)
        # stats are updated in _replace(...)
        (lru_obj_id, lru_obj) = self._top2.popitem(False)
        size = lru_obj[CACHE_SIZE]
        self._bottom2[lru_obj_id] = lru_obj
        self._used_size -= size
        self._bottom2_size += size
        return lru_obj

    # done
    def _delete_lru_from_bottom1(self, rem_bytes):
        evicted_bytes = 0
        while evicted_bytes < rem_bytes and len(self._bottom1) > 0: # TODO: mglw. wird nicht genug Platz gemacht
            (lru_obj_id, lru_obj) = self._bottom1.popitem(False)
            self._bottom1_size -= lru_obj[CACHE_SIZE]
            evicted_bytes += lru_obj[CACHE_SIZE]

    # done
    def _delete_lru_from_bottom2(self, rem_bytes):
        evicted_bytes = 0
        while evicted_bytes < rem_bytes and len(self._bottom2) > 0: # TODO: mglw. wird nicht genug Platz gemacht
            (lru_obj_id, lru_obj) = self._bottom2.popitem(False)
            self._bottom2_size -= lru_obj[CACHE_SIZE]
            evicted_bytes += lru_obj[CACHE_SIZE]

    # done
    def _delete_lru_from_top1(self, rem_bytes, xtime):
        # this function is (only) used by eviction strategy in function cache_object(...)
        # stats are updated in cache_object(...)
        evicted_bytes = 0
        while evicted_bytes < rem_bytes:
            (lru_obj_id, lru_obj) = self._top1.popitem(False)
            self._top1_size -= lru_obj[CACHE_SIZE]
            self._used_size -= lru_obj[CACHE_SIZE]
            evicted_bytes += lru_obj[CACHE_SIZE]
            self._update_eviction_stats(xtime)

    def _add_object_to_top1(self, obj_id, obj):
        self._top1[obj_id] = obj
        self._top1_size += obj[CACHE_SIZE]
        self._used_size += obj[CACHE_SIZE]

    def _add_object_to_top2(self, obj_id, obj):
        self._top2[obj_id] = obj
        self._used_size += obj[CACHE_SIZE]

    # done
    def _replace(self, rem_bytes, xtime, new_obj_in_bottom2):
        # This function is called REPLACE in the paper "ARC: ..." by Megiddo and Modha
        # It moves one object (here: zero or more objects) from top1 (or top2) to bottom1 (or bottom2)
        # Note: p determines how many objects are stored in top1
        # ARC actually assumes that all objects have the same size, which requires a few changes:
        #  while loop only necessary because objects can have different size
        #  parameter p in [0, 1] is the fraction of the cache for top1: size_in_bytes(top1) < p * cache_size
        # TODO: Mglw. zwei Fkt. daraus machen; Objekte nur aus top1 bzw. top2 loeschen, wenn darin Platz benoetigt wird
        evicted_bytes = 0
        while evicted_bytes < rem_bytes:
            diff = self._top1_size - self._p * self._max_size
            if self._top1_size > 0 and (diff > 0 or (diff == 0 and new_obj_in_bottom2)):
                # delete LRU object in top1 and move it to MRU position in bottom1
                obj = self._move_lru_from_top1_to_bottom1()
                evicted_bytes += obj[CACHE_SIZE]
            elif len(self._top2) > 0: # TODO: Abfrage leider noetig; Cache im Paper schlecht beschrieben?
                # delete LRU object in top2 and move it to MRU position in bottom2
                obj = self._move_lru_from_top2_to_bottom2()
                evicted_bytes += obj[CACHE_SIZE]
            else:
                break
            # update stats
            self._update_eviction_stats(xtime)
        return evicted_bytes

    # done
    def _update_eviction_stats(self, xtime):
        self.stats.cached_objects_current -= 1
        self.stats.evicted_objects += 1
        self.daily_stats.evicted_objects += 1
        if self.stats.first_eviction_ts == 0:
            self.stats.first_eviction_ts = xtime

    def cache_object(self, obj_id, size, xtime, next_line=None, force=True, is_new=False):
        if self.is_cached(obj_id):
            # TODO: Die folgende Exception kommt vor, und ich weiss nicht weshalb
            #raise Exception("ERROR: WRITING EXISTING ELEMENT!!! -> %r - ts: %r forced: %r" % (obj_id, xtime, force))
            if obj_id in self._top1:
                self._top1.pop(obj_id)
            if obj_id in self._top2:
                self._top2.pop(obj_id)
            if self.is_cached(obj_id):
                raise Exception("ERROR: WRITING EXISTING ELEMENT!!! -> %r - ts: %r forced: %r" % (obj_id, xtime, force))
        if size > self._max_size:
            raise Exception("Cache too small.")
        rem_bytes = self._used_size + size - self._max_size
        if obj_id in self._bottom1:
            # adaptation of parameter p
            if(len(self._bottom1) >= len(self._bottom2)):
                #self._p = min(self._p + self._step, 1.)
                self._p = min(self._p + self._top1_size / len(self._top1), 1.)
            else:
                self._p = min(self._p + self._bottom2_size / self._bottom1_size, 1.)
            # evict as many objects from cache such that size(objects) >= size(obj)
            # (one could relax this dependent on how much space is really needed)
            self._replace(rem_bytes, xtime, False)
            # move object from bottom1 to MRU position of top2
            obj = self._remove_from_bottom1(obj_id)
            self._add_object_to_top2(obj_id, obj)
        elif obj_id in self._bottom2:
            # adaptation of parameter p
            if(len(self._bottom2) >= len(self._bottom1)):
                #self._p = max(self._p - self._step, 0.)
                self._p = max(self._p - self._top1_size / len(self._top1), 0.)
            else:
                self._p = max(self._p - self._bottom1_size / self._bottom2_size, 0.)
            # evict as many objects from cache such that size(objects) >= size(obj)
            # (one could relax this dependent on how much space is really needed)
            self._replace(rem_bytes, xtime, True)
            # move object from bottom2 to MRU position of top2
            obj = self._remove_from_bottom2(obj_id)
            self._add_object_to_top2(obj_id, obj)
        else:
            #if self._top1_size + self._bottom1_size == self._max_size:
            if self._top1_size + self._bottom1_size + size > self._max_size:
                if self._top1_size < self._max_size:
                    # evict as many objects from cache such that size(objects) >= size(obj)
                    # (one could relax this perhaps)
                    evicted_bytes = self._replace(rem_bytes, xtime, False)
                    # delete LRU objects in bottom1 (to make room for "replaced" objects)
                    # (this step should actually be executed before _replace(...))
                    self._delete_lru_from_bottom1(evicted_bytes)
                else:
                    # delete LRU objects in top1 (to make room for new object)
                    self._delete_lru_from_top1(rem_bytes, xtime)
            else:
                size_all = self._used_size + self._bottom1_size + self._bottom2_size
                if size_all + size > self._max_size:
                    # check whether there is enough space for the new object
                    if size_all + size > 2 * self._max_size:
                        # evict as many objects from cache such that size(objects) >= size(obj)
                        # (imprecise because a lesser amount of rem_bytes could be enough)
                        evicted_bytes = self._replace(rem_bytes, xtime, False)
                        # delete LRU objects in bottom2 (to make room for "replaced" objects)
                        # (this step should actually be executed before _replace(...))
                        self._delete_lru_from_bottom2(evicted_bytes)
                    else:
                        self._replace(rem_bytes, xtime, False)
            # put obj in MRU position in top1
            self._add_object_to_top1(obj_id, [xtime, size])
        # update stats (note: cache sizes are already updated)
        self.stats.cached_objects_current += 1
        self.stats.cached_objects_total += 1
        self.stats.cached_bytes_written += size
        self.daily_stats.cached_objects += 1
        self.daily_stats.cached_bytes_written += size



    # done
    def get_cached(self, obj_id, xtime, next_line=None):
        if self.is_cached(obj_id):
            size = 0
            # cache hit: obj_id is in top1 or top2
            # object remains in cache, but at a different position and always in top2
            if obj_id in self._top1:
                # move object from top1 to MRU position of top2
                obj = self._remove_from_top1(obj_id)
                self._add_object_to_top2(obj_id, obj)
                size = obj[CACHE_SIZE]
            else:
                # move object within top2 to MRU position (no need to update list sizes)
                obj = self._top2.pop(obj_id)
                self._top2[obj_id] = obj
                size = obj[CACHE_SIZE]
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

    # done
    def rename(self, from_obj_id, to_obj_id):
        if self.is_remembered(to_obj_id):
            r = self._remove_remembered(to_obj_id)
            if r == None:
                raise Exception("object not removed!")

        # retrieve object (if remembered) and store it under new ID (no need to update list sizes)
        if from_obj_id in self._top1:
            obj = self._top1.pop(from_obj_id)
            self._top1[to_obj_id] = obj
        elif from_obj_id in self._top2:
            obj = self._top2.pop(from_obj_id)
            self._top2[to_obj_id] = obj
        elif from_obj_id in self._bottom1:
            obj = self._bottom1.pop(from_obj_id)
            self._bottom1[to_obj_id] = obj
        elif from_obj_id in self._bottom2:
            obj = self._bottom2.pop(from_obj_id)
            self._bottom2[to_obj_id] = obj

    def check_sanity(self):
        c = self._max_size
        size_all = self._used_size + self._bottom1_size + self._bottom2_size
        # cache must not exceed max_size
        if self._used_size > c:
            return False
        # the sets have to be pairwise disjoint
        keys_top1 = set(self._top1.keys())
        keys_top2 = set(self._top2.keys())
        keys_bottom1 = set(self._bottom1.keys())
        keys_bottom2 = set(self._bottom2.keys())
        if len(keys_top1 & keys_top2) + len(keys_top1 & keys_bottom1) + len(keys_top1 & keys_bottom2) > 0:
            return False
        if len(keys_bottom1 & keys_bottom2) + len(keys_top2 & keys_bottom1) + len(keys_top2 & keys_bottom2) > 0:
            return False
        # if cache is not full, bottom1 and bottom2 must be empty
        if size_all < c:
            # bottom1 and bottom2 must be empty then
            # yet, since object sizes differ, the cache is usually not completely used
            #if len(self._bottom1_size) > 0 or len(self._bottom2_size) > 0: return False
            pass
        else:
            # top1 and top2 must be full then
            #if self._used_size != c: return False
            pass
        # TODO: check A4 on page 7 of "ARC: A Self-tuning ..." by Megiddo and Modha
        # TODO: think of other checks
        return True


def main(argv=None):
    c = ARCCache(10000, 0, 10000)

    c.cache_object('a', 1000, 1)
    c.get_cached('a', 1)
    c.cache_object('b', 1000, 2)
    c.cache_object('c', 1000, 3)
    if len(c._top1) != 2 or len(c._top2) != 1:
        raise Exception("Error: Should be two objects in top1, but only %r" % (c._top1))
    c.cache_object('d', 3000, 4)
    c.cache_object('e', 2000, 5)
    c.cache_object('f', 1000, 6)
    c.cache_object('g',  500, 7)
    c.cache_object('h', 1000, 8)
    if not('b' in c._bottom1 and 'a' in c._top2 and 'h' in c._top1 and len(c._top1) == 6):
        raise Exception("Error: b in bottom1? a in top2? h in top1? Six objects in top1? %r" % (c._top1))

    if c.is_remembered('f'):
        c._remove_cached('f')
    if c.is_cached('f'):
        raise Exception("Error: Element f should not be in the cache")
    c.cache_object('f', 1000, 9)

    if not(c.is_cached('d')):
        raise Exception("Error: Element d should be in the cache")
    if not(c.is_cached('f')):
        raise Exception("Error: Element d should be in the cache")
    if not(c.is_cached('a')):
        raise Exception("Error: Element d should be in the cache")

    print (c._top1)
    print (c._top2)
    print (c._bottom1)
    print (c._bottom2)


if __name__ == "__main__":
    sys.exit(main())
