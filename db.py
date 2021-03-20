import struct
import functools
import io
import contextlib
from dataclasses import dataclass, field

from lark import Lark, LarkError, Transformer, v_args

from lru_cache import LRUCache

int_size = 4
str_size = 16
cached_pages = 128
page_size = 4096

row_size = int_size + str_size + str_size
row_fmt = f'<i{str_size}s{str_size}s'

metadata_size = int_size
metadata_fmt = f'<i'


@dataclass
class Row:
    id: int
    username: str
    email: str

    def pack(self):
        return struct.pack(row_fmt, self.id, self.username.encode('ascii'), self.email.encode('ascii'))

    def unpack(data):
        id, username, email = struct.unpack(row_fmt, data)
        return Row(id, username.decode('ascii').rstrip('\0'), email.decode('ascii').rstrip('\0'))


@dataclass
class Metadata:
    n_rows: int

    def pack(self):
        return struct.pack(metadata_fmt, self.n_rows)

    def unpack(data):
        return Metadata(*struct.unpack(metadata_fmt, data))


@dataclass
class Pager:
    io: object  # usually a file

    def __init__(self, io, capacity=128):
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
        self.put(index, page)

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

    def close(self):
        for index, page in self.lru_cache.items():
            self.write_to_disk(index, page)

        self.io.flush()
        self.io.close()


@dataclass
class Database:
    pager: Pager
    metadata: Metadata

    def __init__(self, pager=None):
        if pager is None:
            pager = Pager(io.BytesIO())

        page = pager.get(0)
        metadata = Metadata.unpack(page[:metadata_size])

        self.pager = pager
        self.metadata = metadata

    def close(self):
        page = self.pager.get(0)
        page[:metadata_size] = self.metadata.pack()
        self.pager.put(0, page)
        self.pager.close()

    def execute(self, query):
        if type(query) is Insert:
            return self.insert(query.row)

        if type(query) is Select:
            return self.select()

        assert False, f'Unknown query: {query}'

    def row_offset(self, index):
        return metadata_size + index * row_size

    def read_at(self, file_offset, n):
        page_num = file_offset // page_size
        off = file_offset % page_size
        page = self.pager.get(page_num)
        if off + n <= page_size:
            return page[off:off+n]
        else:
            next_page = self.pager.get(page_num + 1)
            return page[off:] + next_page[:n - (page_size - off)]

    def write_at(self, file_offset, data):
        page_num = file_offset // page_size
        off = file_offset % page_size
        with self.pager.modify(page_num) as page:
            if off + len(data) <= page_size:
                page[off:off+len(data)] = data
            else:
                mid = page_size - off
                page[off:] = data[:mid]
                with self.pager.modify(page_num + 1) as next_page:
                    next_page[:len(data) - mid] = data[mid:]

    def insert(self, row):
        offset = self.row_offset(self.metadata.n_rows)
        self.write_at(offset, row.pack())
        self.metadata.n_rows += 1

    def select(self):
        rows = []
        for i in range(self.metadata.n_rows):
            offset = self.row_offset(i)
            r = Row.unpack(self.read_at(offset, row_size))
            rows.append(r)
        return rows


@dataclass
class Select:
    pass


@dataclass
class Insert:
    row: Row


grammar = '''
    ?start: query

    ?query: select | insert
    select: "select" "*"         -> select
    insert: "insert" num str str -> insert

    num: SIGNED_INT     -> num
    str: ESCAPED_STRING -> str


    %import common.WS
    %import common.ESCAPED_STRING
    %import common.SIGNED_INT
    %ignore WS
'''


@v_args(inline=True)
class QueryTransformer(Transformer):
    num = int

    def str(self, s):
        return s.strip('"')

    def select(self):
        return Select()

    def insert(self, id, username, email):
        return Insert(Row(id, username, email))


parser = Lark(grammar, parser='lalr', transformer=QueryTransformer())
parse = parser.parse


def repl():
    pager = Pager(io=open('data.bin', 'r+b'))
    db = Database(pager=pager)

    with contextlib.closing(db) as db:
        while True:
            query = input('db > ').strip()
            if query.startswith('.'):
                if query == '.exit':
                    return

                print(f'Unrecognized command: {query}')
                continue

            try:
                query = parse(query)
            except LarkError as e:
                print(e)
                continue

            print(db.execute(query))


def test_insert_and_select():
    db = Database()
    r1 = Row(123, 'alloe', 'arbue')
    db.insert(r1)
    assert db.select() == [r1]

    r2 = Row(456, 'pog', 'kekw')
    db.insert(r2)
    assert db.select() == [r1, r2]


def test_insert_array():
    db = Database()
    rows = [Row(i, str(i), str(i ** 2)) for i in range(1000)]
    for row in rows:
        db.insert(row)

    assert db.select() == rows


if __name__ == '__main__':
    try:
        repl()
    except KeyboardInterrupt:
        print('^C')
