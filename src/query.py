from dataclasses import dataclass
from lark import Lark, LarkError, Transformer, v_args

from schema import Schema, Int, String


@dataclass
class Select:
    pass

@dataclass
class Insert:
    values: tuple

@dataclass
class Create:
    name: str
    fields: list


grammar = '''
    ?start: query

    ?query: select | insert | create
    select: "select" "*"                -> select
    insert: "insert" value ("," value)* -> insert
    ?value: num | str
    create: "create" "table" name schema -> create
    schema: "(" field ("," field)*   ")" -> schema
    field: name type                     -> field

    ?type: int | varchar
    int: "int"                  -> int
    varchar: "varchar(" num ")" -> varchar

    name: CNAME         -> name
    num: SIGNED_INT     -> num
    str: ESCAPED_STRING -> str

    %import common.WS
    %import common.ESCAPED_STRING
    %import common.CNAME
    %import common.SIGNED_INT
    %ignore WS
'''


@v_args(inline=True)
class QueryTransformer(Transformer):
    num = int
    name = str

    def create(self, name, fields):
        return Create(name, fields)

    def schema(self, *fields):
        return list(fields)

    def field(self, name, type):
        return (name, type)

    def int(self):
        return Int

    def varchar(self, n):
        return String(n)

    def str(self, s):
        return s.strip('"')

    def select(self):
        return Select()

    def insert(self, *values):
        return Insert(values)


parser = Lark(grammar, parser='lalr', transformer=QueryTransformer())
parse = parser.parse
