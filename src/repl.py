import contextlib

from lark import LarkError
from tabulate import tabulate

import query
from table import Table
from pager import Pager


def repl():
    pager = Pager(io=open('data.bin', 'r+b'), capacity=128)
    table = Table(pager=pager)

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
