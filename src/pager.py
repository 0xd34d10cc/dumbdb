import contextlib

from lru_cache import LRUCache

page_size = 4096

class Pager:
    def __init__(self, io, capacity):
        self.io = io
        self.cache = LRUCache(capacity)

    def get(self, index):
        # first, check the cache
        page = self.cache.get(index)
        if page:
            return page

        # page not in cache, read from disk
        page = self.read(index)

        # put new page to cache, write evicted page to disk
        evicted = self.cache.put(index, page)
        if evicted:
            i, p = evicted
            self.write(i, p)

        return page

    def put(self, index, page):
        assert len(page) == page_size
        cached = self.cache.get(index)
        if page is cached:
            return

        self.cache.put(index, page)

    @contextlib.contextmanager
    def modify(self, index):
        page = self.get(index)
        yield page
        self.cache.update(index)

    def read(self, index):
        self.io.seek(page_size * index)
        page = self.io.read(page_size)
        page = bytearray(page_size) if len(page) == 0 else bytearray(page)
        assert len(page) == page_size
        return page

    def write(self, index, page):
        assert len(page) == page_size
        self.io.seek(page_size * index)
        n = self.io.write(page)
        assert n == page_size

    def read_at(self, file_offset, n):
        page_num = file_offset // page_size
        off = file_offset % page_size
        page = self.get(page_num)
        if off + n <= page_size:
            return page[off:off+n]
        else:
            next_page = self.get(page_num + 1)
            return page[off:] + next_page[:n - (page_size - off)]

    def write_at(self, file_offset, data):
        page_num = file_offset // page_size
        off = file_offset % page_size
        with self.modify(page_num) as page:
            if off + len(data) <= page_size:
                page[off:off+len(data)] = data
            else:
                mid = page_size - off
                page[off:] = data[:mid]
                with self.modify(page_num + 1) as next_page:
                    next_page[:len(data) - mid] = data[mid:]

    def close(self):
        for index, page in self.cache.items():
            self.write(index, page)

        self.io.flush()
        self.io.close()
