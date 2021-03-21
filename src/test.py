import io
import contextlib

import pytest

import query
from table import Table
from pager import Pager
from schema import Schema, Int, String


r1 = (123, 'alloe', 'arbue')
r2 = (456, 'pog', 'kekw')

@pytest.fixture
def schema():
    return Schema([
        ('id', Int),
        ('username', String(16)),
        ('email', String(16))
    ])

@pytest.fixture
def table(schema):
    pager = Pager(io.BytesIO(), schema, cached_pages=128)
    return contextlib.closing(Table(schema, pager))

def test_insert_and_select(table):
    with table as db:
        db.execute(query.Insert(r1))
        assert db.execute(query.Select()) == [r1]

        db.execute(query.Insert(r2))
        assert db.execute(query.Select()) == [r1, r2]

def test_insert_array(table):
    with table as db:
        rows = [(i, str(i), str(i ** 2)) for i in range(1000)]
        for row in rows:
            db.execute(query.Insert(row))

        assert db.execute(query.Select()) == rows

def test_insert_and_select_with_close(schema):
    file = io.BytesIO()
    pager = Pager(file, schema, cached_pages=128)
    buffer = None

    with contextlib.closing(Table(schema, pager)) as db:
        db.execute(query.Insert(r1))
        assert db.execute(query.Select()) == [r1]

        db.execute(query.Insert(r2))
        assert db.execute(query.Select()) == [r1, r2]
        db.flush()
        buffer = file.getvalue()

    pager = Pager(io.BytesIO(buffer), schema, cached_pages=128)
    with contextlib.closing(Table(schema, pager)) as db:
        assert db.execute(query.Select()) == [r1, r2]