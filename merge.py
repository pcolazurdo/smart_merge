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

from config import MergeConfig
from stats import ProcessStats

try:
    from alive_progress import alive_bar
except ImportError as e:
    from utils import alive_bar

stats = ProcessStats()
config = MergeConfig()


def reset_stats():
    global stats
    stats = ProcessStats()


def set_config(new_config: MergeConfig):
    global config
    config = new_config


def print_or_quiet(*args, **kwargs):
    if not config.DO_QUIET:
        print(*args, **kwargs)


def generation_session_id():
    sid = str(uuid.uuid4())[0:8]
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
def delete_file(file_name):
    if config.DO_DELETE:
        # print_or_quiet(f'Would delete {file_name}')
        # try:
        file_name.unlink()
    # except Exception as e:
    # raise Exception(f"Error unlyinking {file_name} : {e}")
    else:
        print_or_quiet(f'rm "{file_name}"')
    pass


@handle_exception
def copy_file(source_file, destination_file):
    create_directory(destination_file.parent)
    if config.DO_COPY:
        # print_or_quiet(f'Would copy {source_file} -> {destination_file}')
        # try:
        return copy2(source_file, destination_file)
    # except Exception as e:
    # raise Exception(f"Error copying {source_file} -> {destination_file}: {e}")
    else:
        print_or_quiet(f'cp "{source_file}" "{destination_file}"')
        return destination_file


@cache
@handle_exception
def create_directory(directory_path_name):
    assert "Path" in str(type(directory_path_name))
    if not directory_path_name.is_dir():
        if config.DO_MKDIR:
            # print_or_quiet(f"Would create Directory: {directory_path_name}")
            # try:
            directory_path_name.mkdir(parents=True, exist_ok=True)
        # except Exception as e:
        # raise Exception(f"Error mkdiring {directory_path_name}: {e}")
        else:
            print_or_quiet(f"mkdir -p {directory_path_name}")


@handle_exception
def file_issame(source_file, destination_file):
    assert "Path" in str(type(source_file))
    assert "Path" in str(type(destination_file))
    if config.DO_COMPARE:
        result = cmp(source_file, destination_file, shallow=config.DO_SHALLOW)
        if result == None:  # There was an exception
            result = False
    else:
        result = True
        
    return result


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
def clean_up(source_dir):
    assert "str" in str(type(source_dir))
    s = Path(source_dir).resolve()
    for root, dirs, files in s.walk(top_down=False, on_error=walk_error):
        for d in dirs:
            t = s / root / d
            if config.DO_CLEANUP_SOURCE:
                t.rmdir()
            else:
                print_or_quiet(f'rm -r "{t}"')


def ignore_file(source_file):
    assert "Path" in str(type(source_file))
    audit_exceptions(f"Ignored file: {source_file}")
    print_or_quiet(f"Would ignore {source_file}")


def generate_filename(destination_file):
    assert "Path" in str(type(destination_file))
    global session_id
    parent_path = destination_file.parent
    new_file_name = f"{destination_file.stem}-{session_id}{destination_file.suffix}"
    possible_destination_name = Path(parent_path / new_file_name)
    print_or_quiet(
        f"Original name {destination_file} ## Possible new name: {possible_destination_name}"
    )

    # new file with uuid name also exists
    if possible_destination_name.is_file():
        session_id = generation_session_id()
        possible_destination_name = generate_filename(destination_file)

    return possible_destination_name


def merge_file(source_file, destination_file):
    assert "Path" in str(type(source_file))
    assert "Path" in str(type(destination_file))
    source_size = calc_size(source_file)
    stats.processed(source_size)
    if config.DO_IGNORE:
        if config.IGNORE_PATH in source_file:
            ignore_file(source_file)
            stats.ignored(source_size)
    else:
        if Path(destination_file).is_file():
            if not file_issame(source_file, destination_file):
                # file exists but is different
                destination_file = generate_filename(destination_file)
                if len(str(copy_file(source_file, destination_file))) <= 0:
                    raise Exception(
                        f"copy_file {source_file} -> {destination_file} failed!"
                    )
                else:
                    stats.duplicated(source_size)
            else:
                ignore_file(source_file)
                stats.ignored(source_size)
        else:
            if len(str(copy_file(source_file, destination_file))) <= 0:
                raise Exception(f"copy_file {source_file} -> {destination_file} failed!")
            else:
                stats.copied(source_size)
    delete_file(source_file)
    stats.deleted(
        source_size
    )  # TODO: stats.deleted should be inside the delete command


def walk_error(e):
    print_or_quiet("my Error", e)


def tree_walk(source_dir, destination_dir):
    assert "str" in str(type(source_dir))
    assert "str" in str(type(destination_dir))
    s = Path(source_dir).resolve()
    t = Path(destination_dir).resolve()
    source_depth = len(s.parts)
    with alive_bar() as bar:
        for root, dirs, files in s.walk(top_down=True, on_error=walk_error):
            for f in files:
                target_dir = t.joinpath(*root.parts[source_depth:])
                merge_file(root / f, target_dir / f)
                bar()


def get_stats():
    return stats


def run(args):
    source = args.source
    destination = args.destination

    if not Path(source).resolve().is_dir():
        print(f"Source: {source} - is not a directory. Aborting")
        sys.exit(1)

    if not Path(destination).resolve().is_dir():
        print(f"Destination: {destination} - is not a directory. Aborting")
        sys.exit(1)

    print(f"Source: {source} - files will be deleted from this directory after copied")
    print(f"Destination: {destination}")
    print (config.show_config())

    with alive_bar(total=config.SECURITY_TIMEOUT) as bar:
        i = 0
        while i < config.SECURITY_TIMEOUT:
            bar()
            sleep(1)
            i += 1

    print(f"Merging ...")

    tree_walk(source, destination)
    clean_up(source)

    print(
        f"""Session Id (in case you want to file new files was): {session_id}. For example you can do
          fd \"\\-{session_id}\" {destination}"""
    )

    if config.DO_STATS:
        stats.print_stats()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="merge",
        description="Smart merge of directories",
        epilog="Use carefully",
    )
    parser.add_argument("source")
    parser.add_argument("destination")
    parser.add_argument("-c", "--copy", action="store_false", dest="DO_COPY")
    parser.add_argument("-d", "--delete", action="store_true", dest="DO_DELETE")
    parser.add_argument(
        "-i", "--ignore", action="store_false", dest="IGNORE_DOT_UNDERSCORE_FILES"
    )
    parser.add_argument(
        "-p", "--compare", action="store_false", dest="DO_COMPARE"
    )
    parser.add_argument("-q", "--quiet", action="store_false", dest="DO_QUIET")
    parser.add_argument("-s", "--stats", action="store_false", dest="DO_STATS")
    parser.add_argument(
        "-t", "--timeout", action="store", type=int, dest="SECURITY_TIMEOUT", default=30
    )
    parser.add_argument(
        "-w", "--shallow", action="store_true", dest="DO_SHALLOW")

    args = parser.parse_args()
    print(args.source, args.destination, args)
    session_id = generation_session_id()
    config.DO_DELETE = args.DO_DELETE
    config.DO_COMPARE = args.DO_COMPARE
    config.DO_COPY = args.DO_COPY
    config.DO_MKDIR = True
    config.DO_CLEANUP_SOURCE = True & config.DO_DELETE
    config.IGNORE_DOT_UNDERSCORE_FILES = args.IGNORE_DOT_UNDERSCORE_FILES
    config.DO_QUIET = args.DO_QUIET
    config.DO_STATS = args.DO_STATS
    config.SECURITY_TIMEOUT = max(5, args.SECURITY_TIMEOUT)  # in seconds
    config.DO_SHALLOW = args.DO_SHALLOW
    config.LOG_FILE_NOT_FOUND_ERRORS = True
    config.AUDIT_LOG_FILE = f"{os.getcwd()}/AUDIT_LOG_FILE-{session_id}.log"
    config.DO_SUPRESS_UNKNOWN_EXCEPTIONS = True
    config.DO_IGNORE = True
    config.IGNORE_PATH = "$RECYCLE.BIN"

    run(args)
