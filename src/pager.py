import contextlib

from lru_cache import LRUCache

page_size = 4096
header_size = 4

class Page:
    n_rows_size = 4
    data_offset = n_rows_size

    def __init__(self, schema, data):
        self.n_rows = int.from_bytes(data[0:self.n_rows_size], 'little')
        self.schema = schema
        self.data = data
        self.max_rows = schema.rows_per_page(header_size, page_size)

    def get_row(self, index) -> tuple:
        if self.n_rows - 1 < index:
            return None
        offset = self.data_offset + self.schema.row_size() * index
        needed_slice = self.data[offset:offset+self.schema.row_size()]
        return self.schema.unpack(needed_slice)

    def insert_row(self, row) -> bool:
        if self.n_rows == self.max_rows:
            return False
        offset = self.data_offset + self.schema.row_size() * self.n_rows
        bin_row = self.schema.pack(row)
        self.data[offset:offset + self.schema.row_size()] = bin_row
        self.n_rows += 1
        return True

    def get_data(self) -> bytearray:
        binary_n_rows = self.n_rows.to_bytes(self.n_rows_size, 'little')
        self.data[:self.n_rows_size] = binary_n_rows
        return self.data


class Pager:
    def __init__(self, io, schema, cached_pages):
        self.io = io
        self.cache = LRUCache(cached_pages)
        self.schema = schema

    def get(self, page_id):
        # first, check the cache
        page = self.cache.get(page_id)
        if page:
            return page

        # page not in cache, read from disk
        page = self.read(page_id)
        page = Page(self.schema, page)
        # put new page to cache, write evicted page to disk
        evicted = self.cache.put(page_id, page)
        if evicted:
            i, p = evicted
            self.write(i, p.get_data())

        return page

    def put(self, page_id, page):
        assert len(page) == page_size
        cached = self.cache.get(page_id)
        if page is cached:
            return

        self.cache.put(page_id, page)

    @contextlib.contextmanager
    def modify(self, page_id):
        page = self.get(page_id)
        yield page
        try:
            self.cache.update(page_id)
        except KeyError:
            self.cache.put(page_id, page)

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

    def flush(self):
        # TODO: write only dirty pages
        for page_id, page in self.cache.items():
            self.write(page_id, page.get_data())

        self.io.flush()

    def close(self):
        self.flush()
        self.io.close()
