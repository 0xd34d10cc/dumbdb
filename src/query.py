from dataclasses import dataclass
from lark import Lark, LarkError, Transformer, v_args


@dataclass
class Select:
    pass


@dataclass
class Insert:
    values: tuple


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
