from dataclasses import dataclass
import os
import logging
from functools import wraps
from datetime import datetime

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv("MERGELOGGING", "INFO"))


def sizeof_fmt(num, suffix="B"):
    for unit in ("", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"):
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Yi{suffix}"


@dataclass
class ProcessStats:
    processed_files_size: int
    deleted_files_size: int
    copied_files_size: int
    duplicated_files_size: int
    deleted_files_count: int
    ignored_files_count: int
    copied_files_count: int
    duplicated_files_count: int

    def __init__(self):
        self.processed_files_size = 0
        self.ignored_files_size = 0
        self.deleted_files_size = 0
        self.copied_files_size = 0
        self.duplicated_files_size = 0

        self.processed_files_count = 0
        self.ignored_files_count = 0
        self.deleted_files_count = 0
        self.copied_files_count = 0
        self.duplicated_files_count = 0

    def deleted(self, size: int):
        self.deleted_files_count += 1
        self.deleted_files_size += size

    def processed(self, size: int):
        self.processed_files_count += 1
        self.processed_files_size += size

    def ignored(self, size: int):
        self.ignored_files_count += 1
        self.ignored_files_size += size

    def deleted(self, size: int):
        self.deleted_files_count += 1
        self.deleted_files_size += size

    def copied(self, size: int):
        self.copied_files_count += 1
        self.copied_files_size += size

    def duplicated(self, size: int):
        self.duplicated_files_count += 1
        self.duplicated_files_size += size

    def print_stats(self):
        total_msg = f"""
A total of {self.processed_files_count} files were processed from the source. 
The total size of the source directory was: {sizeof_fmt(self.processed_files_size)}"""
        delete_msg = f"""
A total of {self.deleted_files_count} files were deleted from the source. 
Size of deleted files: {sizeof_fmt(self.deleted_files_size)}"""
        ignored_msg = f"""
A total of {self.ignored_files_count} files were ignored as duplicated. 
Size of ignored files: {sizeof_fmt(self.ignored_files_size)}"""
        copy_msg = f"""
A total of {self.copied_files_count} files were copied. 
Size of copied files: {sizeof_fmt(self.copied_files_size)}"""
        duplicated_msg = f"""
A total of {self.duplicated_files_count} files existed on the target and were copied with an alternative name. 
Size of duplicated files: {sizeof_fmt(self.duplicated_files_size)}"""
        print("***********************************************")
        print(total_msg)
        print(copy_msg)
        print(delete_msg)
        print(ignored_msg)
        print(duplicated_msg)
        print("***********************************************")


class Metric(object):
    _name: str = ""
    _count: int = 0
    _value: float = 0
    _accum: float = 0

    def __init__(self, name):
        self._name = name
        self._count = 0
        self._value = 0

    def inc(self):
        self._count += 1
        self._value += 1
        return self._value

    def dec(self):
        self._count += 1
        self._value -= 1
        return self._value

    def add(self, value):
        self._count += 1
        self._value += value
        return self._value

    def timer(self, value):
        self._count += 1
        a = 1 / self._count
        b = 1 - a
        self._value = a * value + b * self._value
        self._accum += value
        return self._value

    def get(self):
        return self._value

    def get_accum(self):
        return self._accum

    def avg(self):
        if self._count > 0:
            return self._value / self._count
        else:
            return None


class Metrics(object):
    _metric_list: dict

    def __init__(self):
        self._metric_list = {}

    def __str__(self):
        output = ""
        for k, v in self.metrics():
            output = output + f"{k} = {self.get(k)}\n"
        return output

    def add(self, metric_name, metric_value):
        if metric_name not in self._metric_list:
            self._metric_list[metric_name] = Metric(metric_name)

        return self._metric_list[metric_name].add(metric_value)

    def inc(self, metric_name):
        if metric_name not in self._metric_list:
            self._metric_list[metric_name] = Metric(metric_name)

        return self._metric_list[metric_name].inc()

    def dec(self, metric_name):
        if metric_name not in self._metric_list:
            self._metric_list[metric_name] = Metric(metric_name)

        return self._metric_list[metric_name].dec()

    def timer(self, metric_name, metric_value):
        if metric_name not in self._metric_list:
            self._metric_list[metric_name] = Metric(metric_name)

        return self._metric_list[metric_name].timer(metric_value)

    def get_accum(self, metric_name):
        if metric_name not in self._metric_list:
            self._metric_list[metric_name] = Metric(metric_name)

        return self._metric_list[metric_name].get_accum()

    def avg(self, metric_name):
        if metric_name not in self._metric_list:
            self._metric_list[metric_name] = Metric(metric_name)

        return self._metric_list[metric_name].avg()

    def get(self, metric_name):
        if metric_name not in self._metric_list:
            self._metric_list[metric_name] = Metric(metric_name)

        return self._metric_list[metric_name].get()

    def metrics(self):
        for k in self._metric_list:
            yield k, self._metric_list[k].get()


def function_counter(metrics_holder):
    def wrap(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            metrics_holder.inc(func.__qualname__)
            return func(*args, **kwargs)

        return wrapper

    return wrap


def function_timer(metrics_holder):
    def wrap(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            begin = datetime.now()
            return_value = func(*args, **kwargs)
            time_taken = datetime.now() - begin
            metrics_holder.timer(
                f"{func.__qualname__}-timer", time_taken.total_seconds() * 1000
            )
            return return_value

        return wrapper

    return wrap
