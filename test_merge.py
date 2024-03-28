import tempfile
from pathlib import Path

def initialise_directories():

    src = tempfile.TemporaryDirectory()
    dst = tempfile.TemporaryDirectory()

    return src, dst

def cleanup_directory(temp_directory):
    temp_directory.cleanup()

def create_source_files(target_directory):
    FILE1 = 'file1'
    FILE2 = 'file2.txt'
    FILE3 = 'file3.tar.gz'
    FILE4 = 'file space.txt'
    FILE5 = 'file space. txt'
    FILE6 = 'file space. .txt'
    FILE7 = 'file7.txt.txt'
    FILE8 = '.-file8.txt'
    FILE9 = '\~file9.txt'


def create_source_directories(temp_directory):
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
    dir1_path = temp_directory / DIR1
    dir2_path = temp_directory / DIR2
    dir3_path = temp_directory / DIR2 / DIR3
    dir4_path = temp_directory / DIR4
    dir5_path = temp_directory / DIR4 / DIR5
    dirempty1_path = temp_directory / DIREMPTY1
    dirempty2_path = temp_directory / DIR4 / DIREMPTY2
    src_path.mkdir(dir1_path)
    src_path.mkdir(dir2_path)
    src_path.mkdir(dir3_path)
    src_path.mkdir(dir4_path)
    src_path.mkdir(dir5_path)
    src_path.mkdir(dirempty1_path)
    src_path.mkdir(dirempty2_path)




    
