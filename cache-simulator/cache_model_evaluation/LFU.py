#!/usr/bin/env python
"""
LFU.py: A cache which implements the
least frequently used algorithm.
"""

__author__ = "Yves Weissig"
__email__ = "weissig@uni-mainz.de"
__status__ = "Development"

import random

from AbstractCache import AbstractCache


class LFUItem(object):
    
    def __init__(self, param_obj_id, param_atime, param_size):
        """
        Inits the item with the passed parameters.
        """
        self.obj_id = param_obj_id
        self.atime = param_atime
        self.size = param_size
        
    def __str__(self):
        return "%s / %s" % (self.atime, self.size)
        
    def __repr__(self):
        return "[%s, %s]" % (self.atime, self.size)


class LFUFrequency(object):
    
    def __init__(self, param_ref_counter):
        """
        Inits the frequency with the passed parameters.
        """
        self.items = {}
        self.ref_counter = param_ref_counter


class LFUCache(AbstractCache):
    """
    Represents a cache which uses the least frequently used algorithm.
    """
    
    # A dict which maps obj_id to its frequency
    lut = {}
    
    # A dict full of SimpleLFUFrequency's
    freq = {}
    
    # Size of this cache, in bytes
    cache_size = 10000000000000000000
    
    # The number of used bytes in this cache
    used_size = 0
    
    # A dict which contains all stats
    stats = {}
    
    def __init__(self, param_cache_size, min_obj_size, max_obj_size):
        """
        Just some boring boilerplate code to init the cache.
        """
        self.freq[0] = LFUFrequency(0)
        self.freq[1] = LFUFrequency(1)
        self.cache_size = param_cache_size
        self.stats["cache_size"] = param_cache_size
        self.stats["cache_size_bytes"] = param_cache_size
        self.stats["cache_size_kilobytes"] = param_cache_size / 1024
        self.stats["cache_size_megabytes"] = param_cache_size / 1024 / 1024
        self.stats["cache_size_gigabytes"] = param_cache_size / 1024 / 1024 / 1024
        self.stats["cache_type"] = "SimpleLFU"
        self.stats["evicted_objects"] = 0
        self.stats["cached_objects"] = 0
        self.stats["cached_bytes_written"] = 0
        # Needed for "backwards" compability with SimpleBuckets
        self._max_size = param_cache_size
        
    def get_stats(self):
        """
        Returns the statistical information.
        """
        return self.stats
    
    def get_num_cached_objects(self):
        """
        Returns the number of cached objects.
        """
        return len(self.lut)
    
    def is_cached(self, obj_id):
        """
        Returns if the object with the
        passed obj_id is cached or not.
        """
        return obj_id in self.lut

    def is_remembered(self, obj_id):
        return self.is_cached(obj_id)

    def get_free_cache_bytes(self, size):
        """
        Returns the number of free bytes in this cache.
        """
        return self.cache_size - self.used_size
    
    def update_obj_size(self, obj_id, size, delta):
        """
        Updates the size of an object in the cache.
        """
        
        # Sanity checks
        # assert(obj_id in self.lut)
        # assert(obj_id in self.freq[self.lut[obj_id]].items)
        if obj_id not in self.lut:
            # Makes no sense here, but SimpleLRU behaves the same
            #raise Exception("Unable to update size of object ('%s') which "
            #"is not cached!" % obj_id)
            return
        if obj_id not in self.freq[self.lut[obj_id]].items:
            raise Exception("Internal error during updating size of object "
                            "('%s'), the lut points to a wrong frequency bucket!" % obj_id)
        
        # Update size
        self.freq[self.lut[obj_id]].items[obj_id].size = size
        self.used_size += delta
        
        #self.sanity_check("update_obj_size")
    
    def remove_cached(self, obj_id):
        """
        Removes an object from the cache, returns the frequency it was
        used and the amount of freed bytes in the cache.
        """
        
        # Sanity checks
        # assert(obj_id in self.lut)
        # assert(obj_id in self.freq[self.lut[obj_id]].items)
        if obj_id not in self.lut:
            # raise Exception("Unable to remove an object ('%s') which "
            # "is not cached!" % obj_id)
            # This shouldn't raise an exception... because if we evict
            # the object and a second put is issued through the storage system
            # this would lead to an error here, although everything is fine.
            return 0
        if obj_id not in self.freq[self.lut[obj_id]].items:
            raise Exception("Internal error during removing the object ('%s'),"
                            " the lut points to a wrong frequency bucket!" % obj_id)
        
        # Free bytes and delete object in lut as well as in frequency bucket
        _freq = self.lut[obj_id]
        _size = self.freq[self.lut[obj_id]].items[obj_id].size
        self.used_size -= _size
        del self.freq[self.lut[obj_id]].items[obj_id]
        del self.lut[obj_id]
        
        #self.sanity_check("remove_cached")
        
        return _freq, _size
    
    def cache_object(self, obj_id, size, xtime, force=True):
        """
        Caches an object.
        """
        
        # Don't cache objects which are too big
        if size > self.cache_size:
            raise Exception("Object '%s' is too big for this cache!" % obj_id)
        
        # Evict objects if needed
        current_freq = 0
        i = 0
        #an_obj_id = None
        while self.used_size + size > self.cache_size:
            if (current_freq not in self.freq or 
                self.freq[current_freq] is None or 
                self.freq[current_freq].items is None or
                    len(self.freq[current_freq].items) == 0):
                current_freq += 1
            else:
                an_obj_id = random.choice(list(self.freq[current_freq].items.keys()))
                if i % 1000 == 0:
                    print ("Warning, evicted %d objects (us: %d, s: %d, cs: %d, freq: %d, an_obj_id: %s, in lut: %s)" %
                           (i, self.used_size, size, self.cache_size, current_freq, an_obj_id, an_obj_id in self.lut))
                self.remove_cached(an_obj_id)
                self.stats["evicted_objects"] += 1
            i += 1

        # Dirty, dirty fix... sometimes the object is already present
        if obj_id in self.lut:
            self.remove_cached(obj_id)
        
        # Put the object into the frequency bucket
        self.freq[1].items[obj_id] = LFUItem(obj_id, xtime, size)
        
        # Create a reference to the frequency bucket in the lut
        self.lut[obj_id] = 1
        
        # Set the used size of the cache
        self.used_size += size
        
        # Write statistics
        self.stats["cached_objects"] += 1
        self.stats["cached_bytes_written"] += size
        
        #self.sanity_check("cache_object")
    
    def get_cached(self, obj_id, xtime):
        """
        Retrieves an object from the cache.
        """
        if obj_id not in self.lut:
            return False
        if obj_id not in self.freq[self.lut[obj_id]].items:
            raise Exception("Internal error during retrieving the cached object"
                            " '%s', the lut points to a wrong frequency bucket!" % obj_id)
        (_freq, _size) = self.remove_cached(obj_id)
        _freq += 1
        if _freq not in self.freq or self.freq[_freq] is None:
            self.freq[_freq] = LFUFrequency(_freq)
        self.freq[_freq].items[obj_id] = LFUItem(obj_id, xtime, _size)
        self.lut[obj_id] = _freq
        self.used_size += _size
        
        #self.sanity_check("get_cached")
        
        return True
        
    def sanity_check(self, function):
        len_freq_items = 0
        for a_freq in self.freq.itervalues():
            len_freq_items += len(a_freq.items)
        if len(self.lut) != len_freq_items:
            print ("after function %s: %d != %d" % (function, len(self.lut), len_freq_items))
            exit(1)
        pass
        
    def debug_print(self):
        """
        A debug function used to print the contents of the cache.
        """
        print ("---------")
        print ("num_cached_objects: %s" % self.get_num_cached_objects())
        print ("get_free_cache_bytes: %s" % self.get_free_cache_bytes(None))
        for key, value in self.freq.items():
            print ("Frequency: %s" % key)
            print (value.items)

    def rename(self, from_obj_id, to_obj_id):

        if from_obj_id in self.lut:
            old = self.lut.pop(from_obj_id)
            self.lut[to_obj_id] = old

        for x, f in self.freq.items():
            if from_obj_id in f.items:
                old = f.items.pop(from_obj_id)
                old.obj_id = to_obj_id
                f.items[to_obj_id] = old

    def check_sanity(self):
        return True

# AbstractCache.register(LFUCache)
    
if __name__ == "__main__":
    
    # Replay of a small protocol
    tmp = LFUCache(2 * 1024)
    tmp.debug_print()
    tmp.cache_object("a", 1024, 0)
    tmp.cache_object("b", 512, 0)
    tmp.cache_object("c", 256, 0)
    tmp.debug_print()
    tmp.get_cached("a", 1)
    tmp.get_cached("a", 2)
    tmp.get_cached("b", 3)
    tmp.get_cached("a", 4)
    tmp.get_cached("a", 5)
    tmp.get_cached("a", 6)
    tmp.get_cached("b", 7)
    tmp.debug_print()
    tmp.cache_object("d", 512, 0)
    tmp.debug_print()
    tmp.get_cached("d", 7)
    tmp.get_cached("d", 8)
    tmp.get_cached("d", 9)
    tmp.get_cached("d", 10)
    tmp.debug_print()
    tmp.cache_object("e", 1024, 0)
    tmp.debug_print()