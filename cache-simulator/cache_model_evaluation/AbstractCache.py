#!/usr/bin/env python
'''
AbstractCache.py: An abstract cache class which defines all methods a cache 
should implement in order to be used in 'evaluate_cache_models.py'.
'''

__author__ = "Matthias Grawinkel"
__email__ = "grawinkel@uni-mainz.de"
__status__ = "Production"

import abc

class AbstractCache(object):

    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def get_cache_stats_total(self):
        pass

    @abc.abstractmethod
    def get_cache_stats_day(self):
        pass


    @abc.abstractmethod
    def get_num_cached_objects(self):
        pass

    @abc.abstractmethod
    def is_cached(self, obj_id):
        pass

    @abc.abstractmethod
    def is_remembered(self, obj_id):
        pass

    @abc.abstractmethod
    def get_free_cache_bytes(self, size):
        pass

    @abc.abstractmethod
    def update_obj_size(self, obj_id, size, delta):
        pass

    @abc.abstractmethod
    def remove_cached(self, obj_id):
        pass

    @abc.abstractmethod
    def cache_object(self, obj_id, size, xtime, next_line, force=True, is_new=False):
        pass

    @abc.abstractmethod
    def get_cached(self, obj_id, xtime, next_line):
        pass

    @abc.abstractmethod
    def rename(self, from_obj_id, to_obj_id):
        pass

    @abc.abstractmethod
    def check_sanity(self):
        pass
