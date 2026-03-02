"""
db.py — Database adapter.
Wraps both SQLite and psycopg2 behind a unified interface that mirrors
SQLite's connection.execute() API so app.py works unchanged.
"""
import os, sqlite3

DATABASE_URL = os.environ.get("DATABASE_URL")


class Row(dict):
    """Dict that also supports row["col"] and row.col access."""
    def __getitem__(self, key):
        try:
            return super().__getitem__(key)
        except KeyError:
            # Try case-insensitive lookup
            lower = {k.lower(): v for k, v in self.items()}
            return lower[key.lower()]


class UnifiedCursor:
    """Cursor that normalises psycopg2 RealDictRow → Row."""
    def __init__(self, cur, db_type):
        self._cur = cur
        self._db_type = db_type

    def execute(self, sql, params=()):
        if self._db_type == "sqlite":
            sql = sql.replace("%s", "?")
        self._cur.execute(sql, params)
        return self

    def fetchone(self):
        r = self._cur.fetchone()
        if r is None:
            return None
        return Row(dict(r))

    def fetchall(self):
        rows = self._cur.fetchall()
        return [Row(dict(r)) for r in rows]

    def close(self):
        self._cur.close()

    @property
    def description(self):
        return self._cur.description

    def __iter__(self):
        for r in self._cur:
            yield Row(dict(r))


class UnifiedConn:
    """Connection wrapper with .execute() that works like SQLite."""
    def __init__(self, conn, db_type):
        self._conn = conn
        self._db_type = db_type

    def cursor(self):
        return UnifiedCursor(self._conn.cursor(), self._db_type)

    def execute(self, sql, params=()):
        cur = self.cursor()
        cur.execute(sql, params)
        return cur

    def commit(self):
        self._conn.commit()

    def close(self):
        self._conn.close()

    def __getattr__(self, name):
        return getattr(self._conn, name)


def get_db():
    url = os.environ.get("DATABASE_URL")
    if url:
        import psycopg2, psycopg2.extras
        url = url.replace("postgres://", "postgresql://", 1)
        raw = psycopg2.connect(url, cursor_factory=psycopg2.extras.RealDictCursor)
        raw.autocommit = False
        return UnifiedConn(raw, "pg")
    else:
        raw = sqlite3.connect(os.path.join(os.path.dirname(__file__), "tennis.db"))
        raw.row_factory = sqlite3.Row
        return UnifiedConn(raw, "sqlite")
