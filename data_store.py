import sqlite3
from dataclasses import dataclass

DATABASE_NAME = "data.db"
QUIET = True

def audit(*args, **kwargs):
    if not QUIET:
        print(*args, **kwargs)

@dataclass
class File:

    session_id: str
    file_name: str
    file_size: str
    timestamp: str
    file_hash: str


@dataclass
class Error:

    session_id: str
    file_name: str
    timestamp: str
    exception_msg: str
    recoverable: bool


CREATE_DEF = {
    "errors": "(session_id TEXT NOT NULL, file_name TEXT NOT NULL, timestamp TEXT, exception_msg TEXT, receoverable INT DEFAULT 0)",
    "files": "(session_id TEXT NOT NULL, file_name TEXT NOT NULL, file_size INTEGER NOT NULL, timestamp TEXT, file_hash TEXT )",
}


class DataStore:

    database_name: str
    db = None
    cur = None

    def __init__(self, database_name: str = DATABASE_NAME):
        self.database_name = database_name
        self.db = sqlite3.connect(database_name)
        self.cur = self.db.cursor()

    def _execute_query(self, stmt: str):
        try:
            audit(stmt)
            res = self.cur.execute(stmt)
            # log should go here
        except Exception as e:
            raise e

        try:
            if not stmt.upper().startswith("select"):
                self.db.commit()
            # log / metrics should go here
        except Exception as e:
            raise e
        return res

    def detect_table(self, table_name: str):
        stmt = f'''SELECT COUNT(*) FROM main.sqlite_schema WHERE tbl_name == "{table_name}"'''
        data = self._execute_query(stmt).fetchone()
        if data and data[0] == 1:
            return True
        else:
            return False

    def create_table(self, table_name: str):
        stmt = f"""CREATE TABLE {table_name} {CREATE_DEF[table_name]}"""
        return self._execute_query(stmt)

    def create_schema(self):
        if not self.detect_table("files"):
            self.create_table("files")
        if not self.detect_table("errors"):
            self.create_table("errors")
        return None

    def insert_error(self, error: Error) -> bool:
        stmt = f"""INSERT INTO errors VALUES
        ({error.session_id}, {error.file_name}, {error.timestamp}, {error.exception_msg}, {error.recoverable})"""
        return self._execute_query(stmt)

    def insert_file(self, file: File) -> bool:
        stmt = f"""INSERT INTO files VALUES
        ({file.session_id}, {file.file_name}, {file.file_size}, {file.timestamp}, {file.file_hash})"""
        return self._execute_query(stmt)

    def update_count(self, file: File):
        stmt = f'''SELECT * FROM files  
        WHERE session_id = "{file.session_id}" and file_name = "{file.file_name}"'''
        return self._execute_query(stmt)

    def format_content_table(self, table_name):
        stmt = f"""SELECT * FROM {table_name}"""
        res = self._execute_query(stmt)
        for r in res.fetchall():
            yield r
