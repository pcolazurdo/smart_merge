import argparse
import os
import sys
import traceback
import uuid
from filecmp import cmp
from functools import cache, wraps
from pathlib import Path
from shutil import copy2
from time import sleep

from config import ScanConfig
from stats import ProcessStats
from data_store import DataStore, FileRecord, ErrorRecord
import logging

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv('MERGELOGGING', 'INFO')) 


try:
    from alive_progress import alive_bar
except ImportError as e:
    from utils import alive_bar

stats = ProcessStats()
config = ScanConfig()
ds = DataStore(config.DATASTORE)


def reset_stats():
    global stats
    stats = ProcessStats()


def set_config(new_config: ScanConfig):
    global config
    config = new_config


def print_or_quiet(*args, **kwargs):
    if not config.DO_QUIET:
        print(*args, **kwargs)


def get_session_id():
    sid = str(uuid.uuid4())
    print(f"Generating new session id: {sid}")
    return sid


def audit_exceptions(e):
    with open(config.AUDIT_LOG_FILE, "a") as f:
        f.write(f"{e}\n")


def handle_exception(func):
    @wraps(func)
    def handle_exception_inner(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except FileNotFoundError as e:
            if "._" in str(e):
                if config.IGNORE_DOT_UNDERSCORE_FILES:
                    print_or_quiet(
                        f"ignoring FileNotFoundError exception produced by a ._* file: {e}"
                    )
            if config.LOG_FILE_NOT_FOUND_ERRORS:
                audit_exceptions(e)
            return None
        except Exception as e:
            if config.DO_SUPRESS_UNKNOWN_EXCEPTIONS:
                audit_exceptions(traceback.format_exc())
                pass
            else:
                raise e

    return handle_exception_inner

@handle_exception
def calc_size(file_name):
    assert "Path" in str(type(file_name))
    if (
        file_name.is_file()
    ):  # this is an extra fs call but due to handling exception it is better to do this, except performance is critical
        return file_name.stat().st_size
    else:
        return 0

@handle_exception
def hash_file(file_name: str):
    assert "Path" in str(type(file_name))
    hash_function = config.HASH_FUNCTION()

    with file_name.open('rb') as f:
        while True:
            data = f.read(config.BUF_SIZE)
            if not data:
                break
            hash_function.update(data)
    
    return hash_function.hexdigest()

def save_data(file_name: Path) -> bool:
    assert "Path" in str(type(file_name))
    file_size = calc_size(file_name)
    if file_size > config.SIZE_THRESHOLD:
        hash = hash_file(file_name)
    else:
        hash = config.UNDER_THRESHOLD_TEXT
    
    file_record = FileRecord(config.SESSION_ID, str(file_name), file_size, config.TIMESTAMP, hash)
    ds.insert_file(file_record)
    return file_record


def ignore_file(source_file):
    assert "Path" in str(type(source_file))
    print_or_quiet(f"Would ignore {source_file}")


def walk_error(e):
    print_or_quiet("my Error", e)


def tree_walk(source_dir):
    assert "str" in str(type(source_dir))
    s = Path(source_dir).resolve()
    source_depth = len(s.parts)
    with alive_bar() as bar:
        for root, dirs, files in s.walk(top_down=True, on_error=walk_error):
            for f in files:
                save_data(root / f)
                bar()


def get_stats():
    return stats


def run(args):
    source = args.source

    if not Path(source).resolve().is_dir():
        print(f"Source: {source} - is not a directory. Aborting")
        sys.exit(1)

    print(f"Source: {source} - files will be deleted from this directory after copied")
    print (config.show_config())

    with alive_bar(total=config.SECURITY_TIMEOUT) as bar:
        i = 0
        while i < config.SECURITY_TIMEOUT:
            bar()
            sleep(1)
            i += 1

    print(f"Scanning ...")

    tree_walk(source)

    print(
        f"""Session Id (in case you want to file new files was): {config.SESSION_ID}."""
    )

    if config.DO_STATS:
        stats.print_stats()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="scan",
        description="Scan of directories",
        epilog="Use carefully",
    )
    parser.add_argument("source")
    parser.add_argument(
        "-i", "--ignore", action="store_false", dest="IGNORE_DOT_UNDERSCORE_FILES"
    )
    parser.add_argument("-q", "--quiet", action="store_false", dest="DO_QUIET")
    parser.add_argument("-s", "--stats", action="store_false", dest="DO_STATS")
    parser.add_argument(
        "-t", "--timeout", action="store", type=int, dest="SECURITY_TIMEOUT", default=30
    )
    args = parser.parse_args()
    print(args.source, args)
    config.SESSION_ID = get_session_id()
    config.IGNORE_DOT_UNDERSCORE_FILES = args.IGNORE_DOT_UNDERSCORE_FILES
    config.DO_QUIET = args.DO_QUIET
    config.DO_STATS = args.DO_STATS
    config.SECURITY_TIMEOUT = max(5, args.SECURITY_TIMEOUT)  # in seconds
    config.LOG_FILE_NOT_FOUND_ERRORS = True
    config.AUDIT_LOG_FILE = f"{os.getcwd()}/AUDIT_LOG_FILE-{config.SESSION_ID}.log"
    config.DO_SUPRESS_UNKNOWN_EXCEPTIONS = True

    run(args)
