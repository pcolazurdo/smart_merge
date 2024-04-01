import sqlite3
from dataclasses import dataclass

DATABASE_NAME = "data.db"
QUIET = True


def audit(*args, **kwargs):
    if not QUIET:
        print(*args, **kwargs)


@dataclass
class FileRecord:

    session_id: str
    file_name: str
    file_size: str
    timestamp: str
    file_hash: str


@dataclass
class ErrorRecord:

    session_id: str
    file_name: str
    timestamp: str
    exception_msg: str
    recoverable: bool


CREATE_DEF = {
    "errors": """(
        session_id TEXT NOT NULL, 
        file_name TEXT NOT NULL, 
        timestamp TEXT, 
        exception_msg TEXT, 
        receoverable INT DEFAULT 0
    )""",
    "files": """(
        session_id TEXT NOT NULL, 
        file_name TEXT NOT NULL, 
        file_size INTEGER NOT NULL, 
        timestamp TEXT, 
        file_hash TEXT
    )""",
}


class DataStore:

    database_name: str
    db: sqlite3.Connection
    cur: sqlite3.Cursor
    audit: callable

    def set_audit_handler(self, func):
        self.audit = func

    def __init__(self, database_name: str = DATABASE_NAME):
        self.database_name = database_name
        self.db = sqlite3.connect(database_name)
        self.cur = self.db.cursor()
        self.audit = audit
        self.create_schema_if_needed()

    def execute_query(self, stmt: str):
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
        data = self.execute_query(stmt).fetchone()
        if data and data[0] == 1:
            return True
        else:
            return False

    def create_table(self, table_name: str):
        stmt = f"""CREATE TABLE {table_name} {CREATE_DEF[table_name]}"""
        return self.execute_query(stmt)

    def create_schema_if_needed(self) -> int:
        count = 0
        if not self.detect_table("files"):
            self.create_table("files")
            count += 1
        if not self.detect_table("errors"):
            self.create_table("errors")
            count += 1
        return count

    def insert_error(self, error: ErrorRecord) -> bool:
        stmt = f"""INSERT INTO errors VALUES
        ("{error.session_id}", "{error.file_name}", "{error.timestamp}", "{error.exception_msg}", "{error.recoverable}")"""
        return self.execute_query(stmt)

    def insert_file(self, file: FileRecord) -> bool:
        stmt = f"""INSERT INTO files VALUES
        ("{file.session_id}", "{file.file_name}", {file.file_size}, "{file.timestamp}", "{file.file_hash}")"""
        return self.execute_query(stmt)

    def update_count(self, file: FileRecord):
        stmt = f'''SELECT * FROM files  
        WHERE session_id = "{file.session_id}" and file_name = "{file.file_name}"'''
        return self.execute_query(stmt)


    def create_query_stmt(self, table_name:str, **kwargs):
        stmt = f"""SELECT * FROM {table_name}"""
        stmt_where_clause = []
        for k, v in kwargs.items():
            stmt_where_clause.append(f'{k} == \"{v}\"')

        if len(stmt_where_clause) > 0:
            stmt_where = ' AND '.join(stmt_where_clause)
            stmt = f'{stmt} WHERE {stmt_where}'

        return stmt


    def get_records(self, table_name: str, session_id: str=None, file_name:str= None):
        stmt = f"""SELECT * FROM {table_name}"""
        stmt_where_clause = []
        if session_id:
            stmt_where_clause.append(f'session_id == \"{session_id}\"')
        if file_name:
            stmt_where_clause.append(f'file_name == \"{file_name}\"')
        
        if len(stmt_where_clause) > 0:
            stmt_where = ' AND '.join(stmt_where_clause)
            stmt = f'{stmt} WHERE {stmt_where}'

        res = self.execute_query(stmt)
        for r in res.fetchall():
            yield r

    def format_content_table(self, table_name):
        stmt = f"""SELECT * FROM {table_name}"""
        res = self.execute_query(stmt)
        for r in res.fetchall():
            yield r