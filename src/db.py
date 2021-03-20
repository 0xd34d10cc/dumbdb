import struct
import functools
import io
import contextlib
from dataclasses import dataclass, field

import query
from pager import Pager, page_size

int_size = 4
str_size = 16
cached_pages = 128

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
class Database:
    def __init__(self, pager=None):
        if pager is None:
            pager = Pager(io.BytesIO(), capacity=cached_pages)

        page = pager.get(0)
        metadata = Metadata.unpack(page[:metadata_size])

        self.pager = pager
        self.metadata = metadata

    def close(self):
        with self.pager.modify(0) as page:
            page[:metadata_size] = self.metadata.pack()
        self.pager.close()

    def execute(self, q):
        if type(q) is query.Insert:
            return self.insert(q.values)

        if type(q) is query.Select:
            return self.select()

        assert False, f'Unknown query: {q}'

    def insert(self, values):
        offset = self.row_offset(self.metadata.n_rows)
        self.pager.write_at(offset, Row(*values).pack())
        self.metadata.n_rows += 1

    def select(self):
        rows = []
        for i in range(self.metadata.n_rows):
            offset = self.row_offset(i)
            r = Row.unpack(self.pager.read_at(offset, row_size))
            rows.append((r.id, r.username, r.email))
        return rows

    def row_offset(self, index):
        return metadata_size + index * row_size


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
    with contextlib.closing(Database()) as db:
        r1 = (123, 'alloe', 'arbue')
        db.execute(query.Insert(r1))
        assert db.execute(query.Select()) == [r1]

        r2 = (456, 'pog', 'kekw')
        db.execute(query.Insert(r2))
        assert db.execute(query.Select()) == [r1, r2]


def test_insert_array():
    with contextlib.closing(Database()) as db:
        rows = [(i, str(i), str(i ** 2)) for i in range(1000)]
        for row in rows:
            db.execute(query.Insert(row))

        assert db.execute(query.Select()) == rows


if __name__ == '__main__':
    try:
        repl()
    except KeyboardInterrupt:
        print('^C')
