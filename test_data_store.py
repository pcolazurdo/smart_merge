from data_store import DataStore, FileRecord, ErrorRecord, DataQuery
import uuid
from typing import Any
import pytest

TEMP_IN_MEMORY = True


@pytest.fixture
def open_default_db() -> DataStore:
    ds = DataStore()
    return ds


@pytest.fixture
def new_database_name() -> str:
    if TEMP_IN_MEMORY:
        db_temp_name = ":memory:"
    return db_temp_name


def test_create_schema_on_new_db(new_database_name):
    ds = DataStore(new_database_name)
    assert  "data_store.DataStore" in str(type(ds)) 


def test_create_schema_on_new_default():
    ds = DataStore()
    assert ds.create_schema_if_needed() == 0


def test_read_existing_files():
    ds = DataStore()
    count = 0
    for i in ds.format_content_table("files"):
        count += 1
    assert count > 0


def test_create_new_file(open_default_db: DataStore):
    ds = open_default_db
    session_id = str(uuid.uuid4())
    file_name = "new_file_name.txt"
    f = FileRecord(session_id, file_name, 0, "20240101120000.00000", str(uuid.uuid4()))

    count_before = 0
    for i in ds.format_content_table("files"):
        count_before += 1

    ds.insert_file(f)
    count_after = 0
    for i in ds.format_content_table("files"):
        count_after += 1
    assert count_after > 0
    assert count_after > count_before


def test_create_new_error(open_default_db: DataStore):
    ds = open_default_db
    session_id = str(uuid.uuid4())
    file_name = "new_file_name.txt"
    f = ErrorRecord(
        session_id,
        file_name,
        "20240101120000.00000",
        "[Exception] Exception Message",
        0,
    )

    count_before = 0
    for i in ds.format_content_table("errors"):
        count_before += 1

    ds.insert_error(f)
    count_after = 0
    for i in ds.format_content_table("errors"):
        count_after += 1

    assert count_after > 0, "there should be at least 1 record after the insertion"
    assert (
        count_after > count_before
    ), "there should be more records than before the insertion"


def test_get_one_exact_record(open_default_db):
    ds = open_default_db
    session_id = str(uuid.uuid4())
    file_name = "new_file_name.txt"
    f = ErrorRecord(
        session_id,
        file_name,
        "20240101120000.00000",
        "[Exception] Exception Message",
        0,
    )
    ds.insert_error(f)
    res = ds.get_records("errors", session_id, file_name)
    count = 0
    for i in res:
        count += 1
        assert i[0] == session_id
        assert i[1] == file_name
    assert (
        count == 1
    ), "There should be only one record with the same name and session_id"


def test_get_at_least_one_record(open_default_db):
    ds = open_default_db
    session_id = str(uuid.uuid4())
    file_name = "new_file_name.txt"
    f = ErrorRecord(
        session_id,
        file_name,
        "20240101120000.00000",
        "[Exception] Exception Message",
        0,
    )
    ds.insert_error(f)
    res = ds.get_records("errors", session_id)
    count = 0
    for i in res:
        count += 1
        assert i[0] == session_id
    assert count > 0, "There should be at least one record with the exact session_id"


def test_query_creation(open_default_db):
    ds = open_default_db
    ret = ds.create_query_stmt("errors", session_id=1, file_name="pp")

    assert (
        ret == 'SELECT * FROM errors WHERE session_id == "1" AND file_name == "pp"'
    ), ret


def test_query_1(open_default_db):
    ds = open_default_db
    session_ids = [1, 2]
    s = ', '.join([f'"{sess}"' for sess in session_ids])
    sessions_list = f'[{s}]'
    stmt = f"select * from files where session_id in {sessions_list}"
    
    with pytest.raises(Exception): 
        ds.execute_query(stmt) 


def test_query_builder_inner_join(open_default_db):
    STMT1="""SELECT file_hash, file_size, COUNT(*) as cnt FROM files
WHERE session_id in ('1', '2') AND file_hash != "UNDER THRESHOLD"
GROUP BY file_size, file_hash
HAVING cnt > 1"""
    STMT2="""SELECT f.file_hash, f.file_size, f.file_name FROM files AS f INNER JOIN (SELECT file_hash, file_size, COUNT(*) as cnt FROM files
WHERE session_id in ('1', '2') AND file_hash != "UNDER THRESHOLD"
GROUP BY file_size, file_hash
HAVING cnt > 1) AS q ON f.file_hash == q.file_hash AND f.file_size == q.file_size
ORDER BY f.file_hash, f.file_size"""
    
    ds = open_default_db
    session_ids = [1, 2]
    
    data_query1 = DataQuery()
    data_query1.select_clause = 'file_hash, file_size, COUNT(*) as cnt'
    data_query1.from_clause = 'files'
    data_query1.where_clause = [f'{data_query1.format_query_in_clause('session_id', session_ids)}']
    data_query1.where_clause.append(f'file_hash != "UNDER THRESHOLD"')
    data_query1.group_clause = 'file_size, file_hash'
    data_query1.having_clause = 'cnt > 1'
    query1 = data_query1.format_query()

    data_query2 = DataQuery()
    data_query2.select_clause = 'f.file_hash, f.file_size, f.file_name'
    data_query2.from_clause = f'''files AS f INNER JOIN ({query1}) AS q ON f.file_hash == q.file_hash AND f.file_size == q.file_size'''
    data_query2.order_clause = 'f.file_hash, f.file_size'
    query2 = data_query2.format_query()
    
    assert query1 == STMT1
    assert query2 == STMT2

    # assert query1 == 0, query1

