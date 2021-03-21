import contextlib
import os

from lark import LarkError
from tabulate import tabulate


import query
from table import Table
from pager import Pager
from schema import Schema, Int, String


def repl():
    schema = Schema([
        ('id', Int),
        ('username', String(16)),
        ('email', String(16))
    ])

    if not os.path.exists('data.bin'):
        open('data.bin', 'ab').close()

    file = open('data.bin', 'r+b')
    pager = Pager(file, schema, cached_pages=128)
    table = Table(schema, pager)

    with contextlib.closing(table) as table:
        while True:
            q = input('db > ').strip()
            if q.startswith('.'):
                if q == '.exit':
                    return

                print(f'Unrecognized command: {q}')
                continue

            try:
                q = query.parse(q)
            except LarkError as e:
                print(e)
                continue

            rows = table.execute(q)
            if rows is not None:
                print(tabulate(rows, headers=[name for name, type in table.schema.fields]))


if __name__ == '__main__':
    try:
        repl()
    except KeyboardInterrupt:
        print('^C')
