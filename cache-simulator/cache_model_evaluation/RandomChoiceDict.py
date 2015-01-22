__author__ = 'meatz'

from collections import MutableMapping
import random

class RandomChoiceDict(MutableMapping):
    """
    Dictionary-like object allowing efficient random selection.

    """
    def __init__(self):
        # Add code to initialize from existing dictionaries.
        self._keys = []
        self._values = []
        self._key_to_index = {}

    def __getitem__(self, key):
        return self._values[self._key_to_index[key]]

    def __setitem__(self, key, value):
        try:
            index = self._key_to_index[key]
        except KeyError:
            # Key doesn't exist; add a new one.
            index = len(self._keys)
            self._key_to_index[key] = index
            self._keys.append(key)
            self._values.append(value)
        else:
            # Key already exists; overwrite the value.
            self._values[index] = value

    def __delitem__(self, key):
        index = self._key_to_index.pop(key)
        # Remove *last* indexed element, then put
        # it back at position 'index' (overwriting the
        # one we're actually removing) if necessary.
        key, value = self._keys.pop(), self._values.pop()
        if index != len(self._key_to_index):
            self._keys[index] = key
            self._values[index] = value
            self._key_to_index[key] = index

    def __len__(self):
        return len(self._key_to_index)

    def __iter__(self):
        return iter(self._keys)

    def random_key(self):
        """Return a randomly chosen key."""
        if not self:
            raise KeyError("Empty collection")
        index = random.randrange(len(self))
        return self._keys[index]

    def popitem_random(self):
        key = self.random_key()
        value = self.pop(key)
        return key, value



d = RandomChoiceDict()
for x in range(10**6):  # populate with some values
    d[x] = x**2

d.popitem_random()  # remove and return random item
print (132545 in d)
print (d.popitem_random())
