"""
db.py — Database adapter.
Uses PostgreSQL in production (DATABASE_URL env var), SQLite locally.
"""
import os
import sqlite3

DATABASE_URL = os.environ.get("DATABASE_URL")


def get_conn():
    if DATABASE_URL:
        import psycopg2
        import psycopg2.extras
        url = DATABASE_URL.replace("postgres://", "postgresql://", 1)
        conn = psycopg2.connect(url, cursor_factory=psycopg2.extras.RealDictCursor)
        return conn, "pg"
    else:
        conn = sqlite3.connect(os.path.join(os.path.dirname(__file__), "tennis.db"))
        conn.row_factory = sqlite3.Row
        return conn, "sqlite"


def placeholder(db_type):
    """Return the right param placeholder for the DB type."""
    return "%s" if db_type == "pg" else "?"


def fetchall(cur):
    rows = cur.fetchall()
    if not rows:
        return []
    # Normalize psycopg2 RealDictRow to plain dicts
    if hasattr(rows[0], '_asdict'):
        return [dict(r) for r in rows]
    return [dict(r) for r in rows]
