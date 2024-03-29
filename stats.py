from dataclasses import dataclass


def sizeof_fmt(num, suffix="B"):
    for unit in ("", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"):
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Yi{suffix}"

@dataclass
class ProcessStats():
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
        print('***********************************************')
        print(total_msg)
        print(copy_msg)
        print(delete_msg)
        print(ignored_msg)
        print(duplicated_msg)
        print('***********************************************')