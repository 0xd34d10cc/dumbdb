import struct

from dataclasses import dataclass


class Int:
    def fmt():
        return 'i'

    def encode(value):
        assert type(value) is int
        return value

    def decode(value):
        assert type(value) is int
        return value


@dataclass
class String:
    size: int

    def fmt(self):
        return f'{self.size}s'

    def encode(self, value):
        assert type(value) is str
        assert len(value) <= self.size
        return value.encode('ascii')

    def decode(self, value):
        assert type(value) is bytes
        assert len(value) <= self.size
        return value.decode('ascii').rstrip('\0')


class Schema:
    fields: list # (name, type)
    fmt: str
    record_size: int

    def __init__(self, fields):
        self.fields = fields
        self.fmt = '<' + ''.join(field.fmt() for (name, field) in fields)
        self.record_size = struct.calcsize(self.fmt)

    def row_size(self):
        return self.record_size

    def rows_per_page(self, header_size, page_size):
        return (page_size - header_size) // self.row_size()

    def pack(self, values):
        assert len(values) == len(self.fields)
        values = tuple(field.encode(value) for (name, field), value in zip(self.fields, values))
        return struct.pack(self.fmt, *values)

    def unpack(self, data):
        values = struct.unpack(self.fmt, data)
        assert len(values) == len(self.fields)
        return tuple(field.decode(value) for (name, field), value in zip(self.fields, values))
