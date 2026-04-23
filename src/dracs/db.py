import sqlite3
from contextlib import contextmanager
from typing import List, Tuple


@contextmanager
def get_db_connection(dbpath):
    """
    Context manager for database connections.
    Ensures connections are properly closed.
    """
    conn = sqlite3.connect(dbpath)
    try:
        yield conn
    finally:
        conn.close()


def query_by_service_tag(dbpath: str, service_tag: str) -> List[Tuple]:
    """
    Helper function to query systems by service tag.
    Returns list of matching records.
    """
    with get_db_connection(dbpath) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT * FROM systems
            WHERE svc_tag = ?
        """,
            (service_tag,),
        )
        return cursor.fetchall()


def query_by_hostname(dbpath: str, hostname: str) -> List[Tuple]:
    """
    Helper function to query systems by hostname.
    Returns list of matching records.
    """
    with get_db_connection(dbpath) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT * FROM systems
            WHERE name = ?
        """,
            (hostname,),
        )
        return cursor.fetchall()


def upsert_system(
    dbpath: str,
    svc_tag: str,
    name: str,
    model: str,
    idrac_version: str,
    bios_version: str,
    exp_date: str,
    exp_epoch: int,
) -> None:
    """
    Helper function to insert or replace a system record.
    """
    with get_db_connection(dbpath) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT OR REPLACE INTO systems
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
            (svc_tag, name, model, idrac_version, bios_version, exp_date, exp_epoch),
        )
        conn.commit()


def db_initialize(dbpath: str) -> None:
    """
    Initializes the SQLite database. Creates the 'systems' table if it does not
    already exist in the specified file path.
    """
    with get_db_connection(dbpath) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS systems (
                svc_tag TEXT PRIMARY KEY,
                name TEXT,
                model TEXT,
                idrac_version TEXT,
                bios_version TEXT,
                exp_date TEXT,
                exp_epoch INTEGER
            )
        """)
        conn.commit()
    return
