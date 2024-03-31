import hashlib
import tempfile
from pathlib import Path
from typing import Any

import pytest

import scan
from config import ScanConfig

DEBUG = False # if true, tempdirectories aren't cleaned up for further investigation.

def cleanup_directory(temp_directory):
    temp_directory.cleanup()

@pytest.fixture
def initialise_directories():
    src = tempfile.TemporaryDirectory(delete=not DEBUG)
    return src
    

@pytest.fixture
def create_source_file(initialise_directories):
    target_directory = Path(initialise_directories.name).resolve()
    file = 'known_hash.txt'
    path = target_directory / file
    with path.open(mode="x") as f:
        data = '#' * 1024
        f.write(data)
    return path


def test_hash_file(create_source_file):
    config = ScanConfig()
    config.SIZE_THRESHOLD = 1000
    scan.set_config(config)
    file_name = create_source_file

    hash = scan.hash_file(file_name)
    assert file_name != None, file_name
    assert hash == 'ccca4d28d9b929c1a429eadad7ab0d6d', hash


