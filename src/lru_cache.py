from collections import OrderedDict


class LRUCache:

    # initialising capacity
    def __init__(self, capacity: int):
        self.cache = OrderedDict()
        self.capacity = capacity

    def items(self):
        return self.cache.items()

    def update(self, key):
        self.cache.move_to_end(key)

    # we return the value of the key
    # that is queried in O(1) and return None if we
    # don't find the key in out dict / cache.
    # And also move the key to the end
    # to show that it was recently used.
    def get(self, key):
        value = self.cache.get(key, None)
        if value is not None:
            self.cache.move_to_end(key)
        return value

    # first, we add / update the key by conventional methods.
    # And also move the key to the end to show that it was recently used.
    # But here we will also check whether the length of our
    # ordered dictionary has exceeded our capacity,
    # If so we remove the first key (least recently used)
    def put(self, key, value):
        self.cache[key] = value
        self.cache.move_to_end(key)
        if len(self.cache) > self.capacity:
            return self.cache.popitem(last=False)