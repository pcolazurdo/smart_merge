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

def run(args):
    task = args.task
    target = args.target
    session_ids = args.sessions

    query = DataQuery()
    if task == 'list':
        if target == 'sessions':
            query.select_clause = 'DISTINCT session_id'
            query.from_clause = 'files'
        if target == 'duplicatedpaths':
            query.select_clause = 'file_name, COUNT(*) as cnt'
            query.from_clause = 'files'
            if len(session_ids) > 0:
                query.where_clause = f'{query.format_query_in_clause('session_id', session_ids)}'
            else:
                query.where_clause = None
            query.group_clause = 'file_size, file_hash'
            query.having_clause = 'cnt > 1'
        if target == 'duplicated':
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
        if target == 'files':
            query.select_clause = '*'
            query.from_clause = 'files'
            if len(session_ids) > 0:
                query.where_clause = [f'{query_inner.format_query_in_clause('session_id', session_ids)}']
            else:
                query.where_clause = []
            query.where_clause.append(f'file_hash != "{config.UNDER_THRESHOLD_TEXT}"')
            
    if task == 'count':
        if target == 'sessions':
            query.select_clause = 'COUNT(DISTINCT session_id)'
            query.from_clause ='files'
        if target == 'files':
            query.select_clause = '*' 
            query.from_clause = 'files'
            query.where_clause = [f'{query_inner.format_query_in_clause('session_id', session_ids)}']
        
    if query:
        try:
            # print(query)
            result = ds.exec_query(query)
            # result = pd.read_sql_query(query, ds.db).drop_duplicates(keep=False)
            # print(result)
        except Exception as e:
            print(query)
            print(e)
        for i in result:
            print (i)



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
