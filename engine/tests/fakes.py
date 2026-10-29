import re


class FakeSql:
    """In-memory Sql. `responses` is a list of (regex, rows | callable(stmt, args) | Exception)."""

    def __init__(self, responses=None, existing_tables=()):
        self.statements = []
        self.responses = list(responses or [])
        self.tables = {}
        self.existing = set(existing_tables)

    def _answer(self, stmt, args):
        m = re.fullmatch(r"\s*SELECT \* FROM (\S+)\s*", stmt)
        if m and m.group(1) in self.tables:
            return [dict(r) for r in self.tables[m.group(1)]]
        for pattern, resp in self.responses:
            if re.search(pattern, stmt, re.S):
                if isinstance(resp, Exception):
                    raise resp
                return resp(stmt, args) if callable(resp) else [dict(r) for r in resp]
        return []

    def query(self, stmt, args=None):
        self.statements.append((stmt, dict(args or {})))
        return self._answer(stmt, args or {})

    def execute(self, stmt, args=None):
        self.statements.append((stmt, dict(args or {})))
        self._answer(stmt, args or {})

    def table_exists(self, name):
        return name in self.existing or name in self.tables

    def insert_rows(self, table, rows):
        self.tables.setdefault(table, []).extend(dict(r) for r in rows)

    def merge_rows(self, table, rows, keys, only_if=None):
        current = self.tables.setdefault(table, [])
        for row in rows:
            for i, old in enumerate(current):
                if all(old.get(k) == row.get(k) for k in keys):
                    if only_if is None or only_if.check(old, row):
                        current[i] = dict(row)
                    break
            else:
                current.append(dict(row))

    def ran(self, pattern):
        return [s for s, _ in self.statements if re.search(pattern, s, re.S)]
