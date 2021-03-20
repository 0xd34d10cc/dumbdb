import struct
import functools
import io
import contextlib
from dataclasses import dataclass, field

from lark import Lark, LarkError, Transformer, v_args

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

    def __init__(self, io):
        self.io = io
        # TODO: proper cache with reuse of pages
        self.read_at = functools.lru_cache(maxsize=cached_pages)(self.read_at)

    def read_at(self, index):
        self.io.seek(page_size * index)
        page = self.io.read(page_size)
        if len(page) == 0:  # new page
            return bytearray(page_size)
        assert len(page) == page_size
        return bytearray(page)

    def write_at(self, index, page):
        assert len(page) == page_size
        # TODO: use cache
        self.io.seek(page_size * index)
        n = self.io.write(page)
        assert n == page_size

    def close(self):
        self.io.flush()
        self.io.close()


@dataclass
class Database:
    pager: Pager
    metadata: Metadata

    def __init__(self, pager=None):
        if pager is None:
            pager = Pager(io.BytesIO())

        page = pager.read_at(0)
        metadata = Metadata.unpack(page[:metadata_size])

        self.pager = pager
        self.metadata = metadata

    def close(self):
        page = self.pager.read_at(0)
        page[:metadata_size] = self.metadata.pack()
        self.pager.write_at(0, page)
        self.pager.close()

    def execute(self, query):
        if type(query) is Insert:
            return self.insert(query.row)

        if type(query) is Select:
            return self.select()

        assert False, f'Unknown query: {query}'

    def location(self, index):
        offset = index * row_size + metadata_size
        page_num = offset // page_size
        offset -= page_size * page_num
        return page_num, offset

    def insert(self, row):
        page_num, offset = self.location(self.metadata.n_rows)
        page = self.pager.read_at(page_num)
        # TODO: handle case when row is split between pages
        page[offset:offset + row_size] = row.pack()
        self.pager.write_at(page_num, page)
        self.metadata.n_rows += 1

    def select(self):
        rows = []
        for i in range(self.metadata.n_rows):
            page_num, offset = self.location(i)
            page = self.pager.read_at(page_num)
            r = Row.unpack(page[offset:offset + row_size])
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


if __name__ == '__main__':
    try:
        repl()
    except KeyboardInterrupt:
        print('^C')
