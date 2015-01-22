#!/usr/bin/env python

import unittest

from LFU import LFUCache

class TestSequenceFunctions(unittest.TestCase):

    def setUp(self):
        self.cache = LFUCache(2 * 1024)

    def test_insert(self):
        self.cache.cache_object("a", 1024, 0)
        self.assertTrue("a" in self.cache.lut)
        self.assertTrue("a" in self.cache.freq[1].items)
        self.cache.remove_cached("a")
        
    def test_remove(self):
        self.cache.cache_object("d", 1024, 0)
        self.cache.remove_cached("d")
        self.assertTrue("d" not in self.cache.lut)
        self.assertTrue("d" not in self.cache.freq[1].items)
        
    def test_remove_non_existing(self):
        self.assertRaises(Exception, self.cache.remove_cached, "zzz")
        
    def test_multiple_access(self):
        self.cache.cache_object("b", 1024, 10)
        self.cache.get_cached("b", 11)
        self.cache.get_cached("b", 12)
        self.cache.get_cached("b", 13)
        self.cache.get_cached("b", 14)
        self.cache.get_cached("b", 15)
        self.cache.get_cached("b", 16)
        self.cache.get_cached("b", 17)
        self.assertTrue("b" in self.cache.lut)
        self.assertTrue(self.cache.lut["b"] == 8, msg = "frequency is: %s" % self.cache.lut["b"])
        self.assertTrue("b" in self.cache.freq[8].items)
        self.cache.remove_cached("b")
        
    def test_non_existing(self):
        self.assertFalse(self.cache.is_cached("zzz"))
        
    def test_existing(self):
        self.cache.cache_object("c", 512, 0)
        self.assertTrue(self.cache.is_cached("c"))
        self.cache.remove_cached("c")
        
    def test_eviction(self):
        self.cache.cache_object("a1", 512, 0)
        self.cache.cache_object("a2", 512, 0)
        self.cache.get_cached("a2", 0)
        self.cache.cache_object("a3", 512, 0)
        self.cache.get_cached("a3", 0)
        self.cache.get_cached("a3", 0)
        self.cache.cache_object("a4", 512, 0)
        self.cache.get_cached("a4", 0)
        self.cache.get_cached("a4", 0)
        self.cache.get_cached("a4", 0)
        
        self.assertTrue(self.cache.lut["a1"] == 1, msg = "frequency is: %s" % self.cache.lut["a1"])
        self.assertTrue(self.cache.lut["a2"] == 2, msg = "frequency is: %s" % self.cache.lut["a2"])
        self.assertTrue(self.cache.lut["a3"] == 3, msg = "frequency is: %s" % self.cache.lut["a3"])
        self.assertTrue(self.cache.lut["a4"] == 4, msg = "frequency is: %s" % self.cache.lut["a4"])
        
        self.cache.cache_object("b1", 512, 0)
        self.assertTrue("b1" in self.cache.lut and "a1" not in self.cache.lut)
        for i in range(1, 10): self.cache.get_cached("b1", 10)
        
        self.cache.cache_object("b2", 512, 0)
        self.assertTrue("b2" in self.cache.lut and "a2" not in self.cache.lut)
        for i in range(1, 10): self.cache.get_cached("b2", 10)
        
        self.cache.cache_object("b3", 512, 0)
        self.assertTrue("b3" in self.cache.lut and "a3" not in self.cache.lut)
        for i in range(1, 10): self.cache.get_cached("b3", 10)
        
        self.cache.cache_object("b4", 512, 0)
        self.assertTrue("b4" in self.cache.lut and "a4" not in self.cache.lut)
        for i in range(1, 10): self.cache.get_cached("b4", 10)
        
        self.assertTrue(self.cache.lut["b1"] == 10, msg = "frequency is: %s" % self.cache.lut["b1"])
        self.assertTrue(self.cache.lut["b2"] == 10, msg = "frequency is: %s" % self.cache.lut["b2"])
        self.assertTrue(self.cache.lut["b3"] == 10, msg = "frequency is: %s" % self.cache.lut["b3"])
        self.assertTrue(self.cache.lut["b4"] == 10, msg = "frequency is: %s" % self.cache.lut["b4"])
        
        self.cache.remove_cached("b1")
        self.cache.remove_cached("b2")
        self.cache.remove_cached("b3")
        self.cache.remove_cached("b4")
        
    def test_massive_multiple_access(self):
        self.cache.cache_object("oo", 1024, 10)
        for i in range(1, 1000000): self.cache.get_cached("oo", 10)
        self.assertTrue(self.cache.lut["oo"] == 1000000, msg = "frequency is: %s" % self.cache.lut["oo"])
        self.cache.remove_cached("oo")
        
    def test_get_non_existing(self):
        self.assertFalse(self.cache.get_cached("uu", 0))
        
    def test_replacing(self):
        self.cache.cache_object("tt", 1024, 10)
        self.cache.cache_object("tt", 2048, 10)
        self.assertTrue(self.cache.freq[1].items["tt"].size == 2048)
        self.cache.remove_cached("tt")
        
    def test_too_big(self):
        self.assertRaises(Exception, self.cache.cache_object, ["ss", 2049, 0])

if __name__ == '__main__':
    unittest.main()