import sqlite3
from dataclasses import dataclass
from typing import List
import logging
import os

from stats import Metrics, function_counter, function_timer

# DATABASE_NAME = "data.db"
DATABASE_NAME = "file::memory:?cache=shared"
QUIET = True

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv("MERGELOGGING", "INFO"))

metrics = Metrics()


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
        PRIMARY KEY (session_id, file_size, file_hash, file_name)
    )""",
}


@dataclass
class DataQuery:

    select_clause: str = None
    from_clause: str = None
    where_clause: str | List = None
    group_clause: str = None
    having_clause: str | List = None
    order_clause: str = None
    limit_clause: int = None

    def format_query_in_clause(self, column_name, item_list: List) -> str:
        query = ""
        if len(item_list) > 0:
            t = ", ".join([f"'{item}'" for item in item_list])
            item_list_clause = f"({t})"
            query = f"""{column_name} in {item_list_clause}"""
        return query

    def format_query(self) -> str:
        # assert type(select_clause) == str and len(select_clause) > 0, select_clause
        # assert type(from_clause) == str and len(from_clause) > 0, from_clause
        # assert where_clause == None or type(where_clause) == str and len(where_clause) > 0, where_clause
        # assert group_clause == None or type(group_clause) == str and len(group_clause) > 0, group_clause
        # assert having_clause == None or type(having_clause) == str and len(having_clause) > 0, having_clause
        # assert limit_clause == None or type(limit_clause) == int and limit_clause > 0, limit_clause

        stmt = f"""SELECT {self.select_clause} FROM {self.from_clause}"""
        if self.where_clause:
            assert "list" in str(type(self.where_clause)) or "str" in str(
                type(self.where_clause)
            )
            _where_clause = self.where_clause
            if "list" in str(type(self.where_clause)):
                _where_clause = " AND ".join(self.where_clause)
            stmt = f"""{stmt}
WHERE {_where_clause}"""
        if self.group_clause:
            stmt = f"""{stmt}
GROUP BY {self.group_clause}"""
        if self.having_clause:
            _having_clause = self.having_clause
            if "list" in str(type(self.having_clause)):
                _having_clause = " AND ".join(self.having_clause)
            stmt = f"""{stmt}
HAVING {_having_clause}"""
        if self.order_clause:
            stmt = f"""{stmt}
ORDER BY {self.order_clause}"""
        if self.limit_clause:
            stmt = f"""{stmt}
LIMIT {self.limit_clause}"""
        self.stmt = stmt.strip()
        return self.stmt


class DataStore:

    database_name: str
    db: sqlite3.Connection
    cur: sqlite3.Cursor
    audit: callable
    header_description: List = None

    def set_audit_handler(self, func):
        self.audit = func

    def __init__(self, database_name: str = DATABASE_NAME):
        self.database_name = database_name
        self.db = sqlite3.connect(database_name)
        self.cur = self.db.cursor()
        self.audit = audit
        self.create_schema_if_needed()

    def get_observability(self):
        return metrics

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
        return self._execute_query(stmt)

    @function_counter(metrics)
    @function_timer(metrics)
    def insert_file(self, file: FileRecord) -> bool:
        stmt = f"""INSERT INTO files VALUES
        ("{file.session_id}", "{file.file_name}", {file.file_size}, "{file.timestamp}", "{file.file_hash}")"""
        return self._execute_query(stmt)

    @function_counter(metrics)
    @function_timer(metrics)
    def check_file_exists(self, file: FileRecord) -> str:
        stmt = f"""SELECT file_name FROM files"""
        stmt_where_clause = []
        stmt_where_clause.append(f'session_id == "{file.session_id}"')
        stmt_where_clause.append(f'file_size == "{file.file_size}"')
        stmt_where_clause.append(f'file_hash == "{file.file_hash}"')
        stmt_where = " AND ".join(stmt_where_clause)
        stmt = f"{stmt} WHERE {stmt_where}"

        res = self._execute_query(stmt)
        res_list = res.fetchall()
        if len(res_list) > 1:
            raise Exception(
                f"We've found more than 1 record matching {file.session_id} {file.file_name} {file.file_size} {file.file_hash} "
            )
        elif len(res_list) == 1:
            return res_list[0][0]
        else:
            return None

    def check_and_insert_file(self, file: FileRecord) -> str:
        res = self.check_file_exists(file)
        if not res:
            self.insert_file(file)
            return None
        else:
            return res

    def update_count(self, file: FileRecord):
        stmt = f'''SELECT * FROM files  
        WHERE session_id = "{file.session_id}" and file_name = "{file.file_name}"'''
        return self._execute_query(stmt)

    def create_query_stmt(self, table_name: str, **kwargs):
        stmt = f"""SELECT * FROM {table_name}"""
        stmt_where_clause = []
        for k, v in kwargs.items():
            stmt_where_clause.append(f'{k} == "{v}"')

        if len(stmt_where_clause) > 0:
            stmt_where = " AND ".join(stmt_where_clause)
            stmt = f"{stmt} WHERE {stmt_where}"

        return stmt

    def get_records(
        self, table_name: str, session_id: str = None, file_name: str = None
    ):
        stmt = f"""SELECT * FROM {table_name}"""
        stmt_where_clause = []
        if session_id:
            stmt_where_clause.append(f'session_id == "{session_id}"')
        if file_name:
            stmt_where_clause.append(f'file_name == "{file_name}"')

        if len(stmt_where_clause) > 0:
            stmt_where = " AND ".join(stmt_where_clause)
            stmt = f"{stmt} WHERE {stmt_where}"

        res = self._execute_query(stmt)
        for r in res.fetchall():
            yield r

    def format_content_table(self, table_name):
        stmt = f"""SELECT * FROM {table_name}"""
        res = self._execute_query(stmt)
        for r in res.fetchall():
            yield r

    def exec_query(self, dq: DataQuery):
        res = self._execute_query(dq.format_query())
        self.header_description = list(map(lambda x: x[0], self.cur.description))
        return res.fetchall()

    def headers(self):
        return self.header_description


@dataclass
class MemoryDataStore:

    database_name: str
    db: dict
    cur: None
    audit: callable
    header_description: List = None

    def set_audit_handler(self, func):
        self.audit = func

    def __init__(self, database_name: str = DATABASE_NAME):
        self.database_name = database_name
        self.db = dict()
        self.cur = None
        self.audit = audit
        self.create_schema_if_needed()

    def get_observability(self):
        return metrics

    def _execute_query(self, stmt: str):
        raise Exception(f"_execute_query Not Implemented in {type(self).__name__}")

    def detect_table(self, table_name: str):
        if table_name in self.db:
            return True
        return False

    def create_table(self, table_name: str):
        self.db[table_name] = dict()
        return True

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
        key = f"{error.session_id}#{error.file_name}"  # , "{error.timestamp}", "{error.exception_msg}", "{error.recoverable}")"""
        self.db["errors"][key] = {
            "timestamp": error.timestamp,
            "exception_msg": error.exception_msg,
            "recoverable": error.recoverable,
        }
        return True

    @function_counter(metrics)
    @function_timer(metrics)
    def insert_file(self, file: FileRecord) -> bool:
        key = f"{file.session_id}#{file.file_size}#{file.file_hash}"
        self.db["files"][key] = {
            "file_name": file.file_name,
            "timestamp": file.timestamp,
        }
        return True

    @function_counter(metrics)
    @function_timer(metrics)
    def check_file_exists(self, file: FileRecord) -> str:
        key = f"{file.session_id}#{file.file_size}#{file.file_hash}"
        if key in self.db["files"]:
            return self.db["files"][key]["file_name"]
        return None

    def check_and_insert_file(self, file: FileRecord) -> str:
        res = self.check_file_exists(file)
        if not res:
            self.insert_file(file)
            return None
        else:
            return res

    def update_count(self, file: FileRecord):
        raise Exception(f"update_count Not Implemented in {type(self).__name__}")

    def create_query_stmt(self, table_name: str, **kwargs):
        raise Exception(f"create_query_stmt Not Implemented in {type(self).__name__}")

    def get_records(
        self, table_name: str, session_id: str = None, file_name: str = None
    ):
        raise Exception(f"get_records Not Implemented in {type(self).__name__}")

    def format_content_table(self, table_name):
        raise Exception(
            f"format_content_table Not Implemented in {type(self).__name__}"
        )

    def exec_query(self, dq: DataQuery):
        raise Exception(f"exec_query Not Implemented in {type(self).__name__}")

    def headers(self):
        raise Exception(f"headers Not Implemented in {type(self).__name__}")
