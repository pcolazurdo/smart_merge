from stats import ProcessStats

def test_create():
    stats = ProcessStats()
    assert stats.processed_files_count == 0
    assert stats.ignored_files_count == 0
    assert stats.duplicated_files_count == 0
    assert stats.copied_files_count == 0
    assert stats.deleted_files_count == 0
    assert stats.copied_files_size == 0
    assert stats.processed_files_size == 0
    assert stats.ignored_files_size == 0
    assert stats.duplicated_files_size == 0
    assert stats.deleted_files_size == 0


def test_processed():
    stats = ProcessStats()
    stats.processed(1000)
    assert stats.processed_files_count == 1
    assert stats.ignored_files_count == 0
    assert stats.duplicated_files_count == 0
    assert stats.copied_files_count == 0
    assert stats.deleted_files_count == 0
    assert stats.copied_files_size == 0
    assert stats.processed_files_size == 1000
    assert stats.ignored_files_size == 0
    assert stats.duplicated_files_size == 0
    assert stats.ignored_files_size == 0
    stats.processed(1000)
    assert stats.processed_files_count == 2
    assert stats.ignored_files_count == 0
    assert stats.duplicated_files_count == 0
    assert stats.copied_files_count == 0
    assert stats.deleted_files_count == 0
    assert stats.copied_files_size == 0
    assert stats.processed_files_size == 2000
    assert stats.ignored_files_size == 0
    assert stats.duplicated_files_size == 0
    assert stats.ignored_files_size == 0
    assert stats.deleted_files_size == 0

def test_deleted():
    stats = ProcessStats()
    stats.deleted(1000)
    assert stats.processed_files_count == 0
    assert stats.ignored_files_count == 0
    assert stats.duplicated_files_count == 0
    assert stats.copied_files_count == 0
    assert stats.deleted_files_count == 1
    assert stats.copied_files_size == 0
    assert stats.processed_files_size == 0
    assert stats.ignored_files_size == 0
    assert stats.duplicated_files_size == 0
    assert stats.ignored_files_size == 0
    assert stats.deleted_files_size == 1000
    stats.deleted(1000)
    assert stats.processed_files_count == 0
    assert stats.ignored_files_count == 0
    assert stats.duplicated_files_count == 0
    assert stats.copied_files_count == 0
    assert stats.deleted_files_count == 2
    assert stats.copied_files_size == 0
    assert stats.processed_files_size == 0
    assert stats.ignored_files_size == 0
    assert stats.duplicated_files_size == 0
    assert stats.ignored_files_size == 0
    assert stats.deleted_files_size == 2000

def test_ignored():
    stats = ProcessStats()
    stats.ignored(1000)
    assert stats.processed_files_count == 0
    assert stats.ignored_files_count == 1
    assert stats.duplicated_files_count == 0
    assert stats.copied_files_count == 0
    assert stats.deleted_files_count == 0
    assert stats.copied_files_size == 0
    assert stats.processed_files_size == 0
    assert stats.duplicated_files_size == 0
    assert stats.ignored_files_size == 1000
    assert stats.deleted_files_size == 0
    stats.ignored(1000)
    assert stats.processed_files_count == 0
    assert stats.ignored_files_count == 2
    assert stats.duplicated_files_count == 0
    assert stats.copied_files_count == 0
    assert stats.deleted_files_count == 0
    assert stats.copied_files_size == 0
    assert stats.processed_files_size == 0
    assert stats.ignored_files_size == 2000
    assert stats.duplicated_files_size == 0
    assert stats.deleted_files_size == 0

def test_duplicated():
    stats = ProcessStats()
    stats.duplicated(1000)
    assert stats.processed_files_count == 0
    assert stats.ignored_files_count == 0
    assert stats.duplicated_files_count == 1
    assert stats.copied_files_count == 0
    assert stats.deleted_files_count == 0
    assert stats.copied_files_size == 0
    assert stats.processed_files_size == 0
    assert stats.ignored_files_size == 0
    assert stats.duplicated_files_size == 1000
    assert stats.ignored_files_size == 0
    stats.duplicated(1000)
    assert stats.processed_files_count == 0
    assert stats.ignored_files_count == 0
    assert stats.duplicated_files_count == 2
    assert stats.copied_files_count == 0
    assert stats.deleted_files_count == 0
    assert stats.copied_files_size == 0
    assert stats.processed_files_size == 0
    assert stats.ignored_files_size == 0
    assert stats.duplicated_files_size == 2000
    assert stats.ignored_files_size == 0

def test_copied():
    stats = ProcessStats()
    stats.copied(1000)
    assert stats.processed_files_count == 0
    assert stats.ignored_files_count == 0
    assert stats.duplicated_files_count == 0
    assert stats.copied_files_count == 1
    assert stats.deleted_files_count == 0
    assert stats.copied_files_size == 1000
    assert stats.processed_files_size == 0
    assert stats.ignored_files_size == 0
    assert stats.duplicated_files_size == 0
    assert stats.ignored_files_size == 0
    stats.copied(1000)
    assert stats.processed_files_count == 0
    assert stats.ignored_files_count == 0
    assert stats.duplicated_files_count == 0
    assert stats.copied_files_count == 2
    assert stats.deleted_files_count == 0
    assert stats.copied_files_size == 2000
    assert stats.processed_files_size == 0
    assert stats.ignored_files_size == 0
    assert stats.duplicated_files_size == 0
    assert stats.ignored_files_size == 0