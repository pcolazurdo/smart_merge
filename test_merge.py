import pytest
import tempfile
from pathlib import Path
import hashlib
from typing import Any

from config import MergeConfig
import merge

DEBUG = True

def hash(string_to_hash):
    if len(string_to_hash) > 0:
        result = hashlib.md5(string_to_hash.encode()).hexdigest()
    else:
        result = ''
    return result

def initialise_directories():
    src = tempfile.TemporaryDirectory(delete=not DEBUG)
    dst = tempfile.TemporaryDirectory(delete=not DEBUG)
    return src, dst

def cleanup_directory(temp_directory):
    temp_directory.cleanup()

def get_file_size(cycle=True):
    file_sizes = [0, 1, 2, 3, 4]
    count = 0
    while cycle:
        count = count +1 if count < len(file_sizes)-1 else 0
        yield file_sizes[count]

def create_source_files(target_directory, immutable_path_length=0):
    hash_table = {}
    file_names = [
        'file1', 
        'file2.txt', 
        'file3.tar.gz', 
        'file space.txt', 
        'file space. txt', 
        'file space. .txt', 
        'file7.txt.txt', 
        '.-file8.txt', 
        '~file9.txt'
    ]
    file_size = get_file_size(cycle=True)
    for file in file_names:
        path = target_directory / file
        key_name = '/'.join(path.parts[immutable_path_length:])
        assert key_name not in hash_table
        with path.open(mode="x") as f:
            data = '#' * next(file_size)
            f.write(data)
            hash_table[key_name] = hash(data)
            
            # print(data, file=f)
    
    return hash_table

def create_source_directories(temp_directory):
    assert "str" in str(type(temp_directory))
    
    hash_table = {}
    
    DIR1 = 'dir1'
    DIR2 = 'dir2'
    DIR3 = 'dir3'
    DIR4 = 'dir4'
    DIR5 = 'dir5'
    DIREMPTY1 = 'dirempty1'
    DIREMPTY2 = 'dirempty2'

    # src/
    # src/dir1
    # src/dir1/file1-10 
    # src/dirempty2
    # src/dir2
    # src/dir2/dir3
    # src/dir2/dir3/file1-10
    # src/dir4
    # src/dir4/file1-10 (5KB)
    # src/dir4/dir5
    # src/dir4/dir5/file1-10 (3KB)
    # src/dir4/dirempty2

    src_path = Path(temp_directory).resolve()
    temp_directory_path_depth = len(src_path.parts)
    dir1_path = src_path / DIR1
    dir2_path = src_path / DIR2
    dir3_path = src_path / DIR2 / DIR3
    dir4_path = src_path / DIR4
    dir5_path = src_path / DIR4 / DIR5
    dirempty1_path = src_path / DIREMPTY1
    dirempty2_path = src_path / DIR4 / DIREMPTY2
    dir1_path.mkdir()
    dir2_path.mkdir()
    dir3_path.mkdir()
    dir4_path.mkdir()
    dir5_path.mkdir()
    dirempty1_path.mkdir()
    dirempty2_path.mkdir()

    hash_table = hash_table | create_source_files(dir1_path, immutable_path_length=temp_directory_path_depth)
    # hash_table = hash_table | create_source_files(dir2_path)
    hash_table = hash_table | create_source_files(dir3_path, immutable_path_length=temp_directory_path_depth)
    hash_table = hash_table | create_source_files(dir4_path, immutable_path_length=temp_directory_path_depth)
    hash_table = hash_table | create_source_files(dir5_path, immutable_path_length=temp_directory_path_depth)

    return hash_table

@pytest.fixture
def create_src_empty_dst():
    src, dst = initialise_directories()
    hash_table = create_source_directories(src.name)
    # print(hash_table)
    yield { 
        'src': src.name, 
        'dst': dst.name, 
        'ht': hash_table
    }

    # cleanup_directory(src)
    # cleanup_directory(dst)

def create_src_and_dst():
    src, dst = initialise_directories()
    hash_table = create_source_directories(src.name)
    _ = create_source_directories(dst.name)
    # print(hash_table)
    yield { 
        'src': src.name, 
        'dst': dst.name, 
        'ht': hash_table
    }


def analyze_structure(directory_to_compare, hash_table):
    assert "str" in str(type(directory_to_compare))
    dir_path = Path(directory_to_compare).resolve()
    dir_count = 0
    file_count = 0
    for root, dirs, files in dir_path.walk(top_down=True):
        for i in dirs:
            dir_count += 1
        for i in files:
            file_count += 1
    for k,v in hash_table.items():
        file_path = Path(directory_to_compare/ dir_path / k)
        with file_path.open("r") as f:
            content = f.read()
            # print("content", file_path, len(content), f'k: {k} / v: {v}')
            assert v == hash(content)
    return dir_count, file_count


def test_merge_copy(create_src_empty_dst: dict[str, Any]):
    data = create_src_empty_dst
    assert len(data['ht']) == 36
    assert data['src'] != data['dst'] 
    
def test_merge_copy_no_delete_empty_target(create_src_empty_dst: dict[str, Any]):
    data = create_src_empty_dst
    config = MergeConfig()
    config.DO_DELETE = False
    config.DO_COPY = True
    config.DO_MKDIR = True
    config.DO_CLEANUP_SOURCE = False
    config.IGNORE_DOT_UNDERSCORE_FILES = False
    config.DO_QUIET = True
    config.DO_STATS = False
    merge.set_config(config)
    merge.reset_stats()
    
    merge.tree_walk(data['src'], data['dst'])
    merge.clean_up(data['src'])

    dir_count_src, file_count_src = analyze_structure(data['src'], data['ht'])
    assert dir_count_src == 7
    assert file_count_src == 36
    
    dir_count_dst, file_count_dst = analyze_structure(data['dst'], data['ht'])

    assert dir_count_dst == 5
    assert file_count_dst == 36

    stats = merge.get_stats()
    assert stats.processed_files_count == 36
    assert stats.copied_files_count == 36
    assert stats.deleted_files_count == 36
    assert stats.ignored_files_count == 0
    assert stats.duplicated_files_count == 0

def test_merge_twice_no_delete_empty_target(create_src_empty_dst: dict[str, Any]):
    data = create_src_empty_dst
    config = MergeConfig()
    config.DO_DELETE = False
    config.DO_COPY = True
    config.DO_MKDIR = True
    config.DO_CLEANUP_SOURCE = False
    config.IGNORE_DOT_UNDERSCORE_FILES = False
    config.DO_QUIET = True
    config.DO_STATS = False
    merge.set_config(config)
    merge.reset_stats()
    
    merge.tree_walk(data['src'], data['dst'])
    merge.clean_up(data['src'])

    merge.tree_walk(data['src'], data['dst'])
    merge.clean_up(data['src'])

    dir_count_src, file_count_src = analyze_structure(data['src'], data['ht'])
    assert dir_count_src == 7
    assert file_count_src == 36
    
    dir_count_dst, file_count_dst = analyze_structure(data['dst'], data['ht'])

    assert dir_count_dst == 5
    assert file_count_dst == 36

    stats = merge.get_stats()
    assert stats.processed_files_count == 72
    assert stats.copied_files_count == 36
    assert stats.deleted_files_count == 72
    assert stats.ignored_files_count == 36
    assert stats.duplicated_files_count == 0
    

def test_merge_copy_delete_empty_target(create_src_empty_dst: dict[str, Any]):
    data = create_src_empty_dst
    config = MergeConfig()
    config.DO_DELETE = True
    config.DO_COPY = True
    config.DO_MKDIR = True
    config.DO_CLEANUP_SOURCE = True
    config.IGNORE_DOT_UNDERSCORE_FILES = False
    config.DO_QUIET = True
    config.DO_STATS = False
    merge.set_config(config)
    merge.reset_stats()
    
    merge.tree_walk(data['src'], data['dst'])
    merge.clean_up(data['src'])

    dir_count_src, file_count_src = analyze_structure(data['src'], {})
    assert dir_count_src == 0, f'{data['src']} should be empty and is not'
    assert file_count_src == 0
    

    dir_count_dst, file_count_dst = analyze_structure(data['dst'], data['ht'])

    assert dir_count_dst == 5
    assert file_count_dst == 36

    stats = merge.get_stats()
    assert stats.processed_files_count == 36
    assert stats.copied_files_count == 36
    assert stats.deleted_files_count == 36
    assert stats.ignored_files_count == 0
    assert stats.duplicated_files_count == 0


# def test_merge_copy_no_delete_empty_dir(create_structure: dict[str, Any]):
#     data = create_structure
#     config = MergeConfig()
#     config.DO_DELETE = False
#     config.DO_COPY = True
#     config.DO_MKDIR = True
#     config.DO_CLEANUP_SOURCE = False
#     config.IGNORE_DOT_UNDERSCORE_FILES = False
#     config.DO_QUIET = True
#     config.DO_STATS = False
#     merge.set_config(config)
    
#     merge.tree_walk(data['src'], data['dst'])

#     dir_count_src, file_count_src = analyze_structure(data['src'], data['ht'])
#     assert dir_count_src == 7
#     assert file_count_src == 36
    

#     dir_count_dst, file_count_dst = analyze_structure(data['dst'], data['ht'])

#     assert dir_count_dst == 5
#     assert file_count_dst == 36