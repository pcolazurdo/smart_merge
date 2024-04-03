import os
from dataclasses import dataclass
import hashlib
from datetime import datetime


class MergeConfig():
    DO_COMPARE: bool
    DO_COPY: bool
    DO_DELETE: bool
    DO_MKDIR: bool
    DO_CLEANUP_SOURCE: bool
    IGNORE_DOT_UNDERSCORE_FILES: bool
    DO_QUIET: bool
    DO_STATS: bool 
    SECURITY_TIMEOUT: int
    DO_SHALLOW: bool
    DO_IGNORE: bool
    IGNORE_PATH: str
    
    LOG_FILE_NOT_FOUND_ERRORS: bool
    AUDIT_LOG_FILE: str
    DO_SUPRESS_UNKNOWN_EXCEPTIONS: bool
    
    # init with safe values
    def __init__(self):
        self.DO_COMPARE = True
        self.DO_COPY = False
        self.DO_DELETE = False
        self.DO_MKDIR = False
        self.DO_CLEANUP_SOURCE = False
        self.IGNORE_DOT_UNDERSCORE_FILES = False
        self.DO_QUIET = True
        self.DO_STATS = True
        self.SECURITY_TIMEOUT = 60
        self.DO_SHALLOW = False
        self.DO_IGNORE = False
        self.IGNORE_PATH = ""
        
        self.LOG_FILE_NOT_FOUND_ERRORS = False
        self.AUDIT_LOG_FILE= f'{os.getcwd()}/AUDIT_LOG_FILE.log' 
        self.DO_SUPRESS_UNKNOWN_EXCEPTIONS = False

    def show_config(self):
        config_formatted = f"""
We will execute with the following options:
* Run quietly: {self.DO_QUIET}
* Copy source files to destination: {self.DO_COPY}
* Compare files before copying: {self.DO_COMPARE}
  * Do a shallow comparison (compare only metadata): {self.DO_SHALLOW}
* Create subdirectories in target if they don't exist: {self.DO_MKDIR}
* Delete source files after processing: {self.DO_DELETE}
* Delete subdirectories on source after processing: {self.DO_CLEANUP_SOURCE}
* Ignore ._* files (special MAC files): {self.IGNORE_DOT_UNDERSCORE_FILES}
* Log Files not found: {self.LOG_FILE_NOT_FOUND_ERRORS}
  * Continue even with unknown file handling exceptions: {self.DO_SUPRESS_UNKNOWN_EXCEPTIONS}"
  * Log File: {self.AUDIT_LOG_FILE}

* Show Stats: {self.DO_STATS}

* Wait {self.SECURITY_TIMEOUT} seconds before continuining for safety reasons
"""
        return config_formatted
    
class ScanConfig():
    IGNORE_DOT_UNDERSCORE_FILES: bool
    DO_QUIET: bool
    DO_STATS: bool 
    SECURITY_TIMEOUT: int
    DATASTORE:str
    SESSION_ID: str

    LOG_FILE_NOT_FOUND_ERRORS: bool
    AUDIT_LOG_FILE: str
    DO_SUPRESS_UNKNOWN_EXCEPTIONS: bool

    BUF_SIZE: int
    SIZE_THRESHOLD: int
    HASH_FUNCTION: callable
    TIMESTAMP: str
    UNDER_THRESHOLD_TEXT: str
    
    # init with safe values
    def __init__(self):
        self.IGNORE_DOT_UNDERSCORE_FILES = False
        self.DO_QUIET = True
        self.DO_STATS = True
        self.SECURITY_TIMEOUT = 60        
        self.LOG_FILE_NOT_FOUND_ERRORS = False
        self.AUDIT_LOG_FILE= f'{os.getcwd()}/AUDIT_LOG_FILE.log' 
        self.DO_SUPRESS_UNKNOWN_EXCEPTIONS = False
        self.BUF_SIZE = 65536  # lets read stuff in 64kb chunks!
        self.SIZE_THRESHOLD = 65536  # read stuff in 64kb chunks!
        self.HASH_FUNCTION = hashlib.md5
        self.DATASTORE = 'datastore.db'
        self.TIMESTAMP = datetime.now().isoformat(timespec='microseconds')
        self.SESSION_ID = ''
        self.UNDER_THRESHOLD_TEXT = "UNDER THRESHOLD"

    def show_config(self):
        config_formatted = f"""
We will execute with the following options:
* Run quietly: {self.DO_QUIET}
* Ignore ._* files (special MAC files): {self.IGNORE_DOT_UNDERSCORE_FILES}
* Log Files not found: {self.LOG_FILE_NOT_FOUND_ERRORS}
  * Continue even with unknown file handling exceptions: {self.DO_SUPRESS_UNKNOWN_EXCEPTIONS}"
  * Log File: {self.AUDIT_LOG_FILE}

* Show Stats: {self.DO_STATS}

* Wait {self.SECURITY_TIMEOUT} seconds before continuining for safety reasons
"""
        return config_formatted

class DataConfig():
    DATASTORE:str
    UNDER_THRESHOLD_TEXT: str
    DRY_RUN: bool
    
    # init with safe values
    def __init__(self):
        self.DATASTORE = 'datastore.db'
        self.UNDER_THRESHOLD_TEXT = "UNDER THRESHOLD"
        self.DRY_RUN = True

    def show_config(self):
        config_formatted = f"""
        DRY_RUN: {self.DRY_RUN}
        DataStore: {self.DATASTORE}
        """
        return config_formatted