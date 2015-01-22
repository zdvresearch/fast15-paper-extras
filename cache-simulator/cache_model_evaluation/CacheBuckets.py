#!/usr/bin/env python
'''CacheBuckets.py: A cache with a configurable number of buckets.'''

__author__ = "Matthias Grawinkel"
__email__ = "grawinkel@uni-mainz.de"
__status__ = "Development"

__author__ = "Yves Weissig"
__email__ = "weissig@uni-mainz.de"
__status__ = "Development"

import sys

from LRU import LRUCache
from LFU import LFUCache
# from LRUWithLFU import LRUWithLFUCache
from Random import RandomCache
from Fifo import FifoCache
from LRU_2ND import LRU2NDCache
from MRU import MRUCache
from Belady import BeladyCache
from ARC import ARCCache
from SplitLRU import SplitLRUCache

class Bucket(object):
    '''Represents a bucket, identified by the minimum and
    maximum limit, a name and the attached LRU cache itself.'''    
    
    min = 0
    max = 99999999999999999999999999999
    cache = None
    
    def __init__(self, param_min, param_max, param_cache_size, param_name, param_cache_type):
        '''Inits this bucket with the passed parameters.'''
        self.min = param_min
        self.max = param_max
        self.cache = eval("%s(%s, %s, %s)" % (param_cache_type, param_cache_size, param_min, param_max))
        self.name = param_name

class CacheBuckets():
    '''Represents a cache with an configurable number of buckets.'''
    
    # Contains all buckets, key is minimum
    # value of the range is responsible for caching
    buckets = {}
    # Maps the obj_id's which are encountered to
    # the bucket they were stored in
    # mapping = {}
    # Indices of the configuration values
    CONF_MIN = 0
    CONF_MAX = 1
    CONF_SIZE = 2
    CONF_TYPE = 3
    
    def __init__(self, configuration):
        '''Inits this cache with a default set of five buckets. If a custom
        configuration is passed it expects it to be in a dict, whose key
        is the name of the bucket and its value consist of a list which
        identifies the minimum, maximum and cache size of the LRU cache, e.g.
        { "tiny": [0, 4*1024, 1024*1024*1024] } for a bucket "tiny" which
        spans from 0 bytes to 4 kilobytes and has a size of 1 gigabyte.'''

        if (type(configuration) is dict):
            print ("Configuration with %s buckets was passed." % len(configuration))
            for key, value in configuration.items():
                print ("Creating bucket '%s' with range of min: %d - max: %d, size %d and type %s" % (key, value[self.CONF_MIN], value[self.CONF_MAX], value[self.CONF_SIZE], value[self.CONF_TYPE]))
                self.buckets[value[self.CONF_MIN]] = Bucket(value[self.CONF_MIN], value[self.CONF_MAX], value[self.CONF_SIZE], key, value[self.CONF_TYPE])
        else:
            print ("Unable to read configuration..., are you sure this is a dict?")
        
    def get_bucket(self, size):
        '''Given a size this method returns
        the corresponding bucket for direct access.'''
        for key in sorted(self.buckets.keys()):
            value = self.buckets[key]
            if (key <= size and size < value.max): return (key, value)
    
    def get_cache_stats_total(self):
        '''Collects the statistical information
        of all buckets and returns them in a dict.'''
        stats = {}
        for key in sorted(self.buckets.keys()):
            stats[self.buckets[key].name] = self.buckets[key].cache.get_cache_stats_total()
        return stats

    def get_cache_stats_day(self):
        '''Collects the statistical information
        of all buckets and returns them in a dict.'''
        stats = dict()
        for key in sorted(self.buckets.keys()):
            stats[self.buckets[key].name] = self.buckets[key].cache.get_cache_stats_day()
        return stats


    def is_cached(self, obj_id):
        '''Returns if the object with the
        passed obj_id is cached or not.'''
        for bucket in self.buckets.values():
            if bucket.cache.is_cached(obj_id):
                return True
        return False

    def is_remembered(self, obj_id):
        '''Returns if the object with the
        passed obj_id is remembered or not.'''
        for bucket in self.buckets.values():
            if bucket.cache.is_remembered(obj_id):
                return True
        return False

    def get_free_cache_bytes(self, size):
        '''Returns the number of free bytes of
        the cache responsible for the given size.'''
        (bucket_id, bucket) = self.get_bucket(size)
        return bucket.cache.get_free_cache_bytes(size)
    
    def update_obj_size(self, obj_id, size, delta):
        '''Updates the object with the passed
        obj_id to a new size. If the objects size
        exceeds the buckets upper limit, it is moved
        to the repsonsible bucket.'''

        (new_bucket_id, new_bucket) = self.get_bucket(size)

        for old_bucket_id, old_bucket in self.buckets.items():
            #if old_bucket.cache.is_cached(obj_id):
            if old_bucket.cache.is_remembered(obj_id):
                if new_bucket_id == old_bucket_id:
                    old_bucket.cache.update_obj_size(obj_id, size, delta)
                else:
                    xtime = old_bucket.cache._cached_objects[obj_id][0]
                    old_bucket.cache.remove_cached(obj_id)
                    new_bucket.cache.cache_object(obj_id, size, xtime)
                # if not new_bucket.cache.check_sanity():
                #     print ("cache sanity check failed!")
                #     sys.exit(1)

    def remove_cached(self, obj_id):
        '''Removes the object with the passed obj_id
        from the cache, i.e. from the bucket or returns
        0 if the object is not in the cache.'''

        for bucket in self.buckets.values():
            #if bucket.cache.is_cached(obj_id):
            if bucket.cache.is_remembered(obj_id):
                ret = bucket.cache.remove_cached(obj_id)
                return ret
        return None

    def cache_object(self, obj_id, size, xtime, next_line, force=True, is_new=False):
        """
            Inserts the object with the passed obj_id to the cache.

            is_new is True, if the file is PUT for the first time.
            is_new is False, if the object is assumed to exist and is fetched from tape to disk.
        """
        (bucket_id, bucket) = self.get_bucket(size)
        if size < bucket.cache._max_size:
            bucket.cache.cache_object(obj_id, size, xtime, next_line, is_new=is_new)
        else:
            raise Exception("This item is too big for this cache: size of item: %s cache size: %s" % (size, bucket.cache._max_size))
    
    def get_cached(self, obj_id, size, xtime, next_line):
        """
        Simulates a retrieval of an object from the
        cache. If the object is not in the cache False is returned.
        :param obj_id:
        :param xtime:
        :return:
        """

        # under some circumstances, the log files are not sane.
        # a file may have been stored to another bucket, than the get requests read size would look for.
        for bucket in self.buckets.values():
            if bucket.cache.is_cached(obj_id):
                return bucket.cache.get_cached(obj_id, xtime, next_line)
        # the file is not yet cached, so trigger a miss
        (bucket_id, bucket) = self.get_bucket(size)
        return bucket.cache.get_cached(obj_id, xtime, next_line)


    def rename(self, from_obj_id, to_obj_id):
        if self.is_cached(from_obj_id):
            #if self.is_cached(to_obj_id):
            if self.is_remembered(to_obj_id):
                r = self.remove_cached(to_obj_id)
                if r is None:
                    print("ERROR during rename. existing object not removed.")
                    sys.exit(1)

            for bucket in self.buckets.values():
                #if bucket.cache.is_cached(from_obj_id):
                if bucket.cache.is_remembered(from_obj_id):
                    bucket.cache.rename(from_obj_id, to_obj_id)
                    return True
        return False

    def get_num_cached_objects(self):
        '''Returns the number of cached objects.'''
        num_cached_objects = 0
        for bucket_id, bucket in self.buckets.items():
            num_cached_objects += bucket.cache.get_num_cached_objects()
        return num_cached_objects
            
    def print_buckets(self):
        '''A debug function used to print the contents
        of each bucket individually.'''
        print ("---------")
        print ("num_cached_objects: %s" % self.get_num_cached_objects())
        for bucket_id in sorted(self.buckets.keys()):
            bucket = self.buckets[bucket_id]
            print("---------")
            print(bucket.name)
            print(bucket.cache.get_free_cache_bytes(None))
            print(bucket.cache.get_num_cached_objects())

    def check_sanity(self, line):
        for bucket_id, bucket in self.buckets.items():
            if not bucket.cache.check_sanity():
                return False
        return True

if __name__ == "__main__":

    tmp = CacheBuckets()

    # Sanity checks    
    (my_bucket_id, my_bucket) = tmp.get_bucket(0)
    print (my_bucket.name)
    (my_bucket_id, my_bucket) = tmp.get_bucket(1024)
    print (my_bucket.name)
    (my_bucket_id, my_bucket) = tmp.get_bucket(4*1024)
    print (my_bucket.name)
    (my_bucket_id, my_bucket) = tmp.get_bucket(8*1024)
    print (my_bucket.name)
    (my_bucket_id, my_bucket) = tmp.get_bucket(1024*1024)
    print (my_bucket.name)
    (my_bucket_id, my_bucket) = tmp.get_bucket(2*1024*1024)
    print (my_bucket.name)
    
    # Replay of a small protocol
    tmp.print_buckets()
    tmp.cache_object("a", 1000, 10)
    tmp.cache_object("c", 8000, 10)
    tmp.cache_object("d", 1024*1024, 10)
    tmp.cache_object("e", 1024*1024*1000, 10)
    tmp.cache_object("b", 4000, 10)
    tmp.cache_object("f", 1024*1024*1024*10, 10)
    tmp.cache_object("g", 100, 10)
    tmp.print_buckets()
    tmp.remove_cached("a")
    tmp.print_buckets()