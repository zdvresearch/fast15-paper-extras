__author__ = 'meatz'

from collections import defaultdict

class CacheStats():

    def __init__(self, cache_type, cache_size):
        self._cache_type = cache_type
        self._cache_size = cache_size

        self.cache_used = 0
        self.evicted_objects = 0
        self.deleted_objects = 0
        self.cached_objects_total = 0
        self.cached_objects_current = 0
        self.cached_bytes_written = 0
        self.cached_bytes_read = 0
        self.cache_hits = 0
        self.cache_misses = 0
        self.first_eviction_ts = 0

        self.misc = defaultdict(int)

    def to_dict(self):
        d = dict()
        d["cache_type"] = self._cache_type
        d["cache_size"] = self._cache_size
        d["cache_used"] = self.cache_used
        d["evicted_objects"] = self.evicted_objects
        d["deleted_objects"] = self.deleted_objects
        d["cached_objects_total"] = self.cached_objects_total
        d["cached_objects_current"] = self.cached_objects_current
        d["cached_bytes_written"] = self.cached_bytes_written
        d["cached_bytes_read"] = self.cached_bytes_read
        d["cache_hits"] = self.cache_hits
        d["cache_misses"] = self.cache_misses
        d["first_eviction_ts"] = self.first_eviction_ts

        if self.misc:
            d["misc"] = self.misc

        # some calculations:
        if d["cache_size"] > 0 and d["cache_used"] > 0:
            f = (1.0 * d["cache_used"]) / d["cache_size"]
            d["cache_fill_level"] = round(f, 5)
        else:
            d["cache_fill_level"] = 0

        if (d["cache_hits"] + d["cache_misses"]) > 0:
            chr = (1.0 * d["cache_hits"]) / (d["cache_hits"] + d["cache_misses"])
            d["cache_hit_ratio_requests"] = round(chr, 5)
        else:
            d["cache_hit_ratio_requests"] = 0

        return d

class DailyCacheStats():
    def __init__(self, cache_size):
        self._cache_size = cache_size
        self.cache_used = 0
        self.evicted_objects = 0
        self.deleted_objects = 0
        self.cached_objects = 0
        self.cached_bytes_written = 0
        self.cached_bytes_read = 0
        self.cache_hits = 0
        self.cache_misses = 0

        self.misc = defaultdict(int)

    def reset(self):
        self.cache_used = 0
        self.evicted_objects = 0
        self.deleted_objects = 0
        self.cached_objects = 0
        self.cached_bytes_written = 0
        self.cached_bytes_read = 0
        self.cache_hits = 0
        self.cache_misses = 0

        if self.misc:
            self.misc.clear()

    def to_dict(self):
        d = dict()
        d["cache_used"] = self.cache_used
        d["evicted_objects"] = self.evicted_objects
        d["deleted_objects"] = self.deleted_objects
        d["cached_objects"] = self.cached_objects
        d["cached_bytes_written"] = self.cached_bytes_written
        d["cached_bytes_read"] = self.cached_bytes_read
        d["cache_hits"] = self.cache_hits
        d["cache_misses"] = self.cache_misses

        if self.misc:
            d["misc"] = self.misc

        # some calculations:
        if self._cache_size > 0 and d["cache_used"] > 0:
            f = (1.0 * d["cache_used"]) / self._cache_size
            d["cache_fill_level"] = round(f, 5)
        else:
            d["cache_fill_level"] = 0

        if d["cache_hits"] + d["cache_misses"] > 0:
            chr = (1.0 * d["cache_hits"]) / (d["cache_hits"] + d["cache_misses"])
            d["cache_hit_ratio_requests"] = round(chr, 5)
        else:
            d["cache_hit_ratio_requests"] = 0

        return d

class StorageStats():
    def __init__(self):
        self.rename_requests = 0
        self.put_overwrites = 0
        self.put_requests = 0
        self.get_requests = 0
        self.del_requests = 0
        self.bytes_written = 0
        self.bytes_read = 0
        self.cache_hits = 0
        self.cache_misses = 0
        self.cache_hits_bytes = 0
        self.cache_misses_bytes = 0

        self.misc = defaultdict(int)

    def reset(self):
        self.rename_requests = 0
        self.put_overwrites = 0
        self.put_requests = 0
        self.get_requests = 0
        self.del_requests = 0
        self.bytes_written = 0
        self.bytes_read = 0
        self.cache_hits = 0
        self.cache_misses = 0
        self.cache_hits_bytes = 0
        self.cache_misses_bytes = 0

        if self.misc:
            self.misc.clear()

    def to_dict(self):
        d = dict()
        d["rename_requests"] = self.rename_requests
        d["put_overwrites"] = self.put_overwrites
        d["put_requests"] = self.put_requests
        d["get_requests"] = self.get_requests
        d["del_requests"] = self.del_requests
        d["bytes_written"] = self.bytes_written
        d["bytes_read"] = self.bytes_read
        d["cache_hits"] = self.cache_hits
        d["cache_misses"] = self.cache_misses
        d["cache_hits_bytes"] = self.cache_hits_bytes
        d["cache_misses_bytes"] = self.cache_misses_bytes

        if self.misc:
            d["misc"] = self.misc

        if d["get_requests"] > 0:
            chrhd = (1.0 * d["cache_hits"]) / d["get_requests"]
            d["cache_hit_ratio_requests"] = round(chrhd, 3)
        else:
            d["cache_hit_ratio_requests"] = 0

        if d["bytes_read"] > 0:
            chrrd = (1.0 * d["cache_hits_bytes"]) / d["bytes_read"]
            d["cache_hit_ratio_bytes"] = round(chrrd, 3)
        else:
            d["cache_hit_ratio_bytes"] = 0

        return d
