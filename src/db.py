import io
import contextlib

import query
from pager import Pager
from table import Table, Int, String


class Database:
    pager: Pager
    table: Table
    n_rows: int

    def __init__(self, table=None, pager=None):
        if table is None:
            table = Table([
                ('id', Int),
                ('username', String(16)),
                ('email', String(16))
            ])

        if pager is None:
            pager = Pager(io.BytesIO(), capacity=128)

        page = pager.get(0)
        n_rows = int.from_bytes(page[:4], 'little')

        self.pager = pager
        self.table = table
        self.n_rows = n_rows

    def close(self):
        with self.pager.modify(0) as page:
            page[:4] = int.to_bytes(self.n_rows, 4, 'little')
        self.pager.close()

    def execute(self, q):
        if type(q) is query.Insert:
            return self.insert(q.values)

        if type(q) is query.Select:
            return self.select()

        assert False, f'Unknown query: {q}'

    def insert(self, values):
        offset = self.row_offset(self.n_rows)
        self.pager.write_at(offset, self.table.pack(values))
        self.n_rows += 1

    def select(self):
        rows = []
        row_size = self.table.row_size()
        for i in range(self.n_rows):
            offset = self.row_offset(i)
            data = self.pager.read_at(offset, row_size)
            r = self.table.unpack(data)
            rows.append(r)
        return rows

    def row_offset(self, index):
        return 4 + index * self.table.row_size()



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
