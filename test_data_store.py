from data_store import DataStore, FileRecord, ErrorRecord
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
    assert ds.create_schema() == 2


def test_create_schema_on_new_default():
    ds = DataStore()
    assert ds.create_schema() == 0


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
