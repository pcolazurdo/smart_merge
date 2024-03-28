from dataclasses import dataclass
import os

class MergeConfig():
    DO_DELETE: bool
    DO_COPY: bool
    DO_MKDIR: bool
    DO_CLEANUP_SOURCE: bool
    IGNORE_DOT_UNDERSCORE_FILES: bool
    DO_QUIET: bool
    DO_STATS: bool 
    SECURITY_TIMEOUT: int
    DO_SHALLOW: bool
    DO_NOT_COMPARE: bool
    LOG_FILE_NOT_FOUND_ERRORS: bool
    FILE_NOT_FOUND_ERRORS_LOG: str
    
    # init with safe values
    def __init__(self):
        self.DO_DELETE = False
        self.DO_COPY = False
        self.DO_MKDIR = False
        self.DO_CLEANUP_SOURCE = False
        self.IGNORE_DOT_UNDERSCORE_FILES = False
        self.DO_QUIET = True
        self.DO_STATS = True
        self.SECURITY_TIMEOUT = 60
        self.DO_SHALLOW = False
        self.DO_NOT_COMPARE = False
        self.LOG_FILE_NOT_FOUND_ERRORS = False
        self.FILE_NOT_FOUND_ERRORS_LOG= f'{os.getcwd()}/file_not_found.log' 
