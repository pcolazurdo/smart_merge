import pytest
import tempfile
from pathlib import Path
import hashlib
from typing import Any

from config import MergeConfig
import merge

DEBUG = True # if true, tempdirectories aren't cleaned up for further investigation.

FILE_NAMES_LIST = [
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

FILE_SIZES_LIST = [0, 1, 2, 3, 4]
SOURCE_DIRECTORIES = 7
NON_EMPTY_DIRECTORIES = 5
DIRECTORIES_CONTAINING_FILES = 4
EXPECTED_FILES = len(FILE_NAMES_LIST) * DIRECTORIES_CONTAINING_FILES

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
    file_sizes = FILE_SIZES_LIST
    count = 0
    while cycle:
        count = count +1 if count < len(file_sizes)-1 else 0
        yield file_sizes[count]

def create_source_files(target_directory, immutable_path_length=0):
    hash_table = {}
    file_names = FILE_NAMES_LIST
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
    assert len(data['ht']) == 36, f'there should be 36 hashes from the source directory'
    assert data['src'] != data['dst'], f'src and dst folders should be different'
    
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

    # check that source is untouched
    dir_count_src, file_count_src = analyze_structure(data['src'], data['ht'])
    assert dir_count_src == SOURCE_DIRECTORIES, 'the stats are miscounting the number of operations'
    assert file_count_src == EXPECTED_FILES, 'the stats are miscounting the number of operations'
    
    # check that destination has the right copies of the files
    dir_count_dst, file_count_dst = analyze_structure(data['dst'], data['ht'])
    assert dir_count_dst == NON_EMPTY_DIRECTORIES, 'the stats are miscounting the number of operations'
    assert file_count_dst == EXPECTED_FILES, 'the stats are miscounting the number of operations'

    # check that stats are correct
    stats = merge.get_stats()
    assert stats.processed_files_count == EXPECTED_FILES, 'the stats are miscounting the number of operations'
    assert stats.copied_files_count == EXPECTED_FILES, 'the stats are miscounting the number of operations'
    assert stats.deleted_files_count == EXPECTED_FILES, 'the stats are miscounting the number of operations'
    assert stats.ignored_files_count == 0, 'the stats are miscounting the number of operations'
    assert stats.duplicated_files_count == 0, 'the stats are miscounting the number of operations'

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

    # check that source is untouched the first time
    dir_count_src, file_count_src = analyze_structure(data['src'], data['ht'])
    assert dir_count_src == SOURCE_DIRECTORIES
    assert file_count_src == EXPECTED_FILES

    # check that destination has all the files the first time
    dir_count_src, file_count_src = analyze_structure(data['src'], data['ht'])
    assert dir_count_src == SOURCE_DIRECTORIES
    assert file_count_src == EXPECTED_FILES

    stats = merge.get_stats()
    assert stats.processed_files_count == EXPECTED_FILES, 'the stats are miscounting the number of operations'
    assert stats.copied_files_count == EXPECTED_FILES, 'the stats are miscounting the number of operations'
    assert stats.deleted_files_count == EXPECTED_FILES, 'the stats are miscounting the number of operations'
    assert stats.ignored_files_count == 0, 'the stats are miscounting the number of operations'
    assert stats.duplicated_files_count == 0, 'the stats are miscounting the number of operations'

    merge.reset_stats()
    merge.tree_walk(data['src'], data['dst'])
    merge.clean_up(data['src'])

    # check that source is untouched the first time
    dir_count_src, file_count_src = analyze_structure(data['src'], data['ht'])
    assert dir_count_src == SOURCE_DIRECTORIES
    assert file_count_src == EXPECTED_FILES
    
    # check that destination has all files untouched but that there wasn't any new copy
    dir_count_dst, file_count_dst = analyze_structure(data['dst'], data['ht'])
    assert dir_count_dst == NON_EMPTY_DIRECTORIES
    assert file_count_dst == EXPECTED_FILES

    stats = merge.get_stats()
    assert stats.processed_files_count == EXPECTED_FILES, 'the stats are miscounting the number of operations'
    assert stats.copied_files_count == 0, 'the stats are miscounting the number of operations'
    assert stats.deleted_files_count == EXPECTED_FILES, 'the stats are miscounting the number of operations'
    assert stats.ignored_files_count == EXPECTED_FILES, 'the stats are miscounting the number of operations'
    assert stats.duplicated_files_count == 0, 'the stats are miscounting the number of operations'
    

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
    assert dir_count_src == 0, f'{data['src']} there should not be any directories left but there are some - check folder'
    assert file_count_src == 0, f'{data['src']} there should not be any files left but there are some - check folder'
    
    dir_count_dst, file_count_dst = analyze_structure(data['dst'], data['ht'])
    assert dir_count_dst == NON_EMPTY_DIRECTORIES, f'{data['dst']} there should be 5 folders from the 7 configured (as the tool does not copy empty folders) - check folder'
    assert file_count_dst == EXPECTED_FILES, f'{data['dst']} there should be 36 files (4*9) - check folder'

    stats = merge.get_stats()
    assert stats.processed_files_count == EXPECTED_FILES, 'the stats are miscounting the number of operations'
    assert stats.copied_files_count == EXPECTED_FILES, 'the stats are miscounting the number of operations'
    assert stats.deleted_files_count == EXPECTED_FILES, 'the stats are miscounting the number of operations'
    assert stats.ignored_files_count == 0, 'the stats are miscounting the number of operations'
    assert stats.duplicated_files_count == 0, 'the stats are miscounting the number of operations'


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