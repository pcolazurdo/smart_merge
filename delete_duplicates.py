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
import mimetypes
from datetime import datetime

from config import ScanConfig
from stats import ProcessStats, Metrics, function_counter, function_timer
from data_store import MemoryDataStore, FileRecord, ErrorRecord
import logging

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv("MERGELOGGING", "INFO"))

mimetypes.init()


def get_extensions_for_type(general_type):
    for ext in mimetypes.types_map:
        if mimetypes.types_map[ext].split("/")[0] == general_type:
            yield ext


VIDEO = list(get_extensions_for_type("video"))
AUDIO = list(get_extensions_for_type("audio"))
IMAGE = list(get_extensions_for_type("image"))
COMPRESSED = [".zip", ".gz", ".7z", ".gzip", ".tar", ".cpio", ".rar"]
OTHERS = []

CHECK_EXTENSIONS = []
CHECK_EXTENSIONS.extend(VIDEO)
CHECK_EXTENSIONS.extend(AUDIO)
CHECK_EXTENSIONS.extend(IMAGE)
CHECK_EXTENSIONS.extend(COMPRESSED)

try:
    from alive_progress import alive_bar
except ImportError as e:
    from utils import alive_bar

stats = ProcessStats()
metrics = Metrics()
config = ScanConfig()
ds = MemoryDataStore(config.DATASTORE)


script_file = open("script.sh", "w")


@function_counter(metrics)
def write_to_file(line):
    script_file.write(f"{line}\n")


@function_counter(metrics)
def reset_stats():
    global stats
    stats = ProcessStats()


@function_counter(metrics)
def set_config(new_config: ScanConfig):
    global config
    config = new_config


@function_counter(metrics)
def print_or_quiet(*args, **kwargs):
    if not config.DO_QUIET:
        print(*args, **kwargs)


@function_counter(metrics)
def get_session_id():
    sid = str(uuid.uuid4())
    print(f"Generating new session id: {sid}")
    return sid


@function_counter(metrics)
def audit_exceptions(e):
    with open(config.AUDIT_LOG_FILE, "a") as f:
        f.write(f"{e}\n")


@function_counter(metrics)
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
@function_counter(metrics)
def calc_size(file_name):
    assert "Path" in str(type(file_name))
    if (
        file_name.is_file()
    ):  # this is an extra fs call but due to handling exception it is better to do this, except performance is critical
        return file_name.stat().st_size
    else:
        return 0


@handle_exception
@function_counter(metrics)
def hash_file(file_name: str):
    assert "Path" in str(type(file_name))
    hash_function = config.HASH_FUNCTION()

    with file_name.open("rb") as f:
        while True:
            data = f.read(config.BUF_SIZE)
            if not data:
                break
            hash_function.update(data)

    return hash_function.hexdigest()


@function_counter(metrics)
def is_duplicated(file_name: Path) -> str:
    assert "Path" in str(type(file_name))
    file_size = calc_size(file_name)
    if file_size > config.SIZE_THRESHOLD:
        hash = hash_file(file_name)
    else:
        hash = config.UNDER_THRESHOLD_TEXT

    file_record = FileRecord(
        config.SESSION_ID, str(file_name), file_size, config.TIMESTAMP, hash
    )
    begin = datetime.now()
    existing_file = ds.check_and_insert_file(file_record)
    time_taken = datetime.now() - begin
    metrics.timer(f"#check_and_insert_file_timer", time_taken.total_seconds() * 1000)
    return existing_file


@function_counter(metrics)
def ignore_file(source_file):
    assert "Path" in str(type(source_file))
    print_or_quiet(f"Would ignore {source_file}")


@function_counter(metrics)
def walk_error(e):
    print_or_quiet("my Error", e)


@function_counter(metrics)
def delete_file(source_file: Path, duplicated_file: Path):
    if config.PHYSICAL_DELETE:
        try:
            # source_file.unlink()
            print(f"rm {str(source_file)}")
        except Exception as e:
            if config.DO_SUPRESS_UNKNOWN_EXCEPTIONS:
                audit_exceptions(traceback.format_exc())
                pass
            else:
                raise e
    else:
        write_to_file(f"rm {str(source_file)} #{str(duplicated_file)}")


@function_counter(metrics)
@function_timer(metrics)
def should_ignore(file_name: Path):
    # ignore if file is not in approved extensions
    if file_name.suffix.lower() not in CHECK_EXTENSIONS:
        return True

    # ignore if file is inside a .git directory
    if str(file_name).find(".git") > 0:
        return True

    return False


@function_counter(metrics)
def tree_walk(source_dir):
    assert "str" in str(type(source_dir))
    s = Path(source_dir).resolve()
    source_depth = len(s.parts)
    with alive_bar() as bar:
        for root, dirs, files in s.walk(top_down=True, on_error=walk_error):
            for f in files:
                if not should_ignore(root / f):
                    duplicated = is_duplicated(root / f)
                    if duplicated:
                        delete_file(root / f, duplicated)
                bar()


@function_counter(metrics)
def get_stats():
    return stats


@function_counter(metrics)
@function_timer(metrics)
def run(args):
    source = args.source

    if not Path(source).resolve().is_dir():
        print(f"Source: {source} - is not a directory. Aborting")
        sys.exit(1)

    print(f"Source: {source} - files will be deleted from this directory after copied")
    print(config.show_config())

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
        prog="delete_duplicates",
        description="Scan Directories and Deletes files if the file has already been found",
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
    parser.add_argument(
        "-p", "--physical", action="store", type=int, dest="PHYSICAL_DELETE"
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
    # config.PHYSICAL_DELETE = True

    run(args)

    print(str(metrics))
    print("Total should_ignore-timer (ms):", metrics.get_accum("should_ignore-timer"))
    print(
        "Total #check_and_insert_file_timer (ms):",
        metrics.get_accum("#check_and_insert_file_timer"),
    )

    print(str(ds.get_observability()))
