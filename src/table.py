import io
import contextlib

import query
from pager import Pager, page_size, header_size
from schema import Schema, Int, String


class Table:
    pager: Pager
    schema: Schema
    n_rows: int

    def __init__(self, schema, pager):
        self.pager = pager
        self.schema = schema
        self.n_rows = pager.get(0).n_rows

    def flush(self):
        with self.pager.modify(0) as page:
            page.n_rows = self.n_rows
        self.pager.flush()

    def close(self):
        self.flush()
        self.pager.close()

    def execute(self, q):
        if type(q) is query.Insert:
            return self.insert(q.values)

        if type(q) is query.Select:
            return self.select()

        assert False, f'Unknown query: {q}'

    def insert(self, values):
        page_id, _ = self.row_id(self.n_rows)

        page = self.pager.get(page_id)
        if not page.insert_row(values):
             page = self.pager.get(page_id+1)
             assert page.insert_row(values)

        self.n_rows += 1

    def select(self):
        rows = []
        for i in range(self.n_rows):
            page_id, row_id = self.row_id(i)
            page = self.pager.get(page_id)
            row = page.get_row(row_id)
            rows.append(row)
        return rows

    def row_id(self, index) -> (int, int):
        per_size = self.schema.rows_per_page(header_size, page_size)
        page_id = index // per_size
        row_id = index % per_size

        # first page is for metadata only, so we start with index 1
        return page_id + 1, row_id

    def row_offset(self, index):
        return 4 + index * self.schema.row_size()