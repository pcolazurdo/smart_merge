import argparse
import os
import sys
import traceback
import uuid
from pathlib import Path

import pandas as pd

from config import ScanConfig
from data_store import DataStore, FileRecord, ErrorRecord, DataQuery

config = ScanConfig()
ds = DataStore(config.DATASTORE)

def set_config(new_config: ScanConfig):
    global config
    config = new_config


def print_or_quiet(*args, **kwargs):
    if not config.DO_QUIET:
        print(*args, **kwargs)

def list_sessions(query: DataQuery):
    query.select_clause = 'DISTINCT session_id'
    query.from_clause = 'files'
    return query

def list_duplicated(query: DataQuery):
    query_inner = DataQuery()
    query_inner.select_clause = 'file_hash, file_size, COUNT(*) as cnt'
    query_inner.from_clause = 'files'
    if len(session_ids) > 0:
        query_inner.where_clause = [f'{query_inner.format_query_in_clause('session_id', session_ids)}']
    else:
        query_inner.where_clause = []
    query_inner.where_clause.append(f'file_hash != "{config.UNDER_THRESHOLD_TEXT}"')
    query_inner.group_clause = 'file_size, file_hash'
    query_inner.having_clause = 'cnt > 1'

    query.select_clause = 'f.file_hash, f.file_size, f.file_name'
    query.from_clause = f'''files AS f INNER JOIN ({query_inner.format_query()}) AS q ON f.file_hash == q.file_hash AND f.file_size == q.file_size'''
    query.order_clause = 'f.file_hash, f.file_size'
    return query

def list_duplicatedpaths(query: DataQuery):
    query.select_clause = 'file_name, COUNT(*) as cnt'
    query.from_clause = 'files'
    if len(session_ids) > 0:
        query.where_clause = f'{query.format_query_in_clause('session_id', session_ids)}'
    else:
        query.where_clause = None
    query.group_clause = 'file_size, file_hash'
    query.having_clause = 'cnt > 1'
    return query

def list_files(query: DataQuery):
    query.select_clause = '*'
    query.from_clause = 'files'
    if len(session_ids) > 0:
        query.where_clause = [f'{query_inner.format_query_in_clause('session_id', session_ids)}']
    else:
        query.where_clause = []
    query.where_clause.append(f'file_hash != "{config.UNDER_THRESHOLD_TEXT}"')
    return query

def count_sessions(query: DataQuery):
    query.select_clause = 'COUNT(DISTINCT session_id)'
    query.from_clause = 'files'
    return query

def count_files(query: DataQuery):
    query.select_clause = '*'
    query.from_clause = 'files'
    query.where_clause = [f'{query_inner.format_query_in_clause('session_id', session_ids)}']
    return query

def run(args):
    task = args.task
    target = args.target
    session_ids = args.sessions

    query = DataQuery()
    if task == 'list':
        if target == 'sessions':
            query = list_sessions(query)
        if target == 'duplicatedpaths':
            query = list_duplicatedpaths(query)
        if target == 'duplicated':
            query = list_duplicated(query)
        if target == 'files':
            query = list_files(query)
    if task == 'count':
        if target == 'sessions':
            query = count_sessions(query)
        if target == 'files':
            query = count_files(query)
            
    if query:
        try:
            # print(query)
            results = ds.exec_query(query).fetchall()
            # result = pd.read_sql_query(query, ds.db).drop_duplicates(keep=False)
            # print(result)
        except Exception as e:
            print(query)
            print(e)
    else:
        print("Nothing to show")
        return

    if task == 'list':
        if target == 'duplicated':
            df = pd.DataFrame.from_records(results, columns=ds.headers())
            print(df)
            return 
    
    for i in results:
        print(i)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="data",
        description="Lets you handle the file data store",
        epilog="Use carefully",
    )
    parser.add_argument("task")
    parser.add_argument("target")
    parser.add_argument("-s", "--session", action= 'append', dest='sessions', default=[])
    args = parser.parse_args()
    # print(args.task, args.target, args)
    
    run(args)
