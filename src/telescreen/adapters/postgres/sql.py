from telescreen.domain.table import TableName


def quote_ident(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def quote_table(table: TableName) -> str:
    return f"{quote_ident(table.schema)}.{quote_ident(table.name)}"
