import argparse
import os
import sys
import traceback
import uuid
from pathlib import Path
from typing import List, Any

import pandas as pd

from config import DataConfig
from data_store import DataStore, FileRecord, ErrorRecord, DataQuery

config = DataConfig()

def set_config(new_config: DataConfig):
    global config
    config = new_config


def print_or_quiet(*args, **kwargs):
    if config.DRY_RUN:
        print(*args, **kwargs)

def list_sessions(query: DataQuery):
    query.select_clause = 'DISTINCT session_id'
    query.from_clause = 'files'
    return query

def list_duplicated(query: DataQuery, session_ids):
    query_inner = DataQuery()
    query_inner.select_clause = 'file_hash as file_hash, file_size as file_size, COUNT(*) as cnt'
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

def list_duplicatedpaths(query: DataQuery, session_ids):
    query.select_clause = 'file_name, COUNT(*) as cnt'
    query.from_clause = 'files'
    if len(session_ids) > 0:
        query.where_clause = f'{query.format_query_in_clause('session_id', session_ids)}'
    else:
        query.where_clause = None
    query.group_clause = 'file_size, file_hash'
    query.having_clause = 'cnt > 1'
    return query

def list_files(query: DataQuery, session_ids):
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

def count_files(query: DataQuery, session_ids):
    query.select_clause = '*'
    query.from_clause = 'files'
    query.where_clause = [f'{query.format_query_in_clause('session_id', session_ids)}']
    return query

# it filters all records for which there is only 1 combination of the same file_size, file_hash
def delete_non_duplicated(df: pd.DataFrame):
    assert type(df) == pd.DataFrame
    non_duplicated_records = df.groupby(['file_size', 'file_hash'])['file_name'].filter(lambda x: len(x) == 1)
    return non_duplicated_records

# it filters all records which the file_name contains some of the texts in the exclusion list
def filter_df(df_orig, include_list, exclude_list):
    assert type(df_orig) == pd.DataFrame
    df = df_orig
    for i in exclude_list:
        print_or_quiet(f"Excluding: {i}")
        df = df[~df['file_name'].str.contains(i.strip())]
    return df

# depending on the DRY_RUN setting this will create a list of files that are duplicated 
# For each combination of file_hash, file_size, it will mark the first file_name (lexicographically ordered) as duplicated = False
# and the rest as duplicated = True and then it will print the list. If DRY_RUN is set, it will print the 
# file_size and hash as title and then all the files that are part of it below this title.
# If DRY_RUN is false, it will output a series of unix commands, with rm {file_name} when duplicated == True, 
# and echo {file_name} when False
def show_duplicated(results: List[Any], ds:DataStore, include_list: List= None, exclude_list: List= None):
    def print_duplicates(df):
        last_file_size = None
        last_file_hash = None
        for i, r in df.iterrows():
            # print(i,r)
            if r['file_size'] != last_file_size or r['file_hash'] != last_file_hash:
                if config.DRY_RUN:
                    print(f"{r['file_size']} {r['file_hash']}")
                last_file_size = r['file_size']
                last_file_hash = r['file_hash']
            if config.DRY_RUN:
                print(f"\t {r['file_name']} {r['duplicated']}")
            else:
                if r['duplicated']:
                    print (f'rm \"{r['file_name']}\"')
                else:
                    print (f'echo \"{r['file_name']}\"')
    
    df = pd.DataFrame.from_records(results, columns=ds.headers())
    df = filter_df(df, include_list, exclude_list)
    df = df.sort_values(['file_hash', 'file_size', 'file_name'])
    df['duplicated'] = df.duplicated(subset=['file_hash', 'file_size'], keep='first')
    print_duplicates(df)
    delete_non_duplicated(df)

def run(args):
    task = args.task
    target = args.target
    session_ids = args.sessions

    include_list= []
    exclude_list= []
    # if args.include:
    #     try:
    #         with open(args.include) as f:
    #             lines = f.readlines()
    #             for i in lines:
    #                 include_list.append(i)
    #     except Exception as e:
    #         print(e)
    #         return
    if args.exclude:
        try:
            with open(args.exclude) as f:
                lines = f.readlines()
                for i in lines:
                    exclude_list.append(i)
        except Exception as e:
            print(e)
            return

    print_or_quiet(config.show_config())

    ds = DataStore(config.DATASTORE)
    query = DataQuery()
    if task == 'list':
        if target == 'sessions':
            query = list_sessions(query)
        if target == 'duplicatedpaths':
            query = list_duplicatedpaths(query, session_ids)
        if target == 'duplicated':
            query = list_duplicated(query, session_ids)
        if target == 'files':
            query = list_files(query, session_ids)
    if task == 'count':
        if target == 'sessions':
            query = count_sessions(query)
        if target == 'files':
            query = count_files(query, session_ids)

       
    if query:
        try:
            results = ds.exec_query(query)
            if task == 'list':
                if target == 'duplicated':
                    show_duplicated(results, ds, include_list, exclude_list)
                    # df = pd.DataFrame.from_records(results, columns=ds.headers())
                    # df = filter_df(df, include_list, exclude_list)
                    # df = df.sort_values(['file_hash', 'file_size', 'file_name'])
                    # delete_non_duplicated(df)
                    return     
            for i in results:
                print_or_quiet(i)
        except Exception as e:
            print(query)
            raise e
    else:
        print("Nothing to show")
        return

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="data",
        description="Lets you handle the file data store",
        epilog="Use carefully",
    )
    parser.add_argument("task")
    parser.add_argument("target")
    parser.add_argument("-s", "--session", action= 'append', dest='sessions', default=[])
    # parser.add_argument("-i", "--include", action= 'store', dest='include', default=None) # TODO: Solve how to manage include tasks
    parser.add_argument("-x", "--exclude", action= 'store', dest='exclude', default=None)
    parser.add_argument("-p", "--prefer", action= 'store', dest='prefer', default=None)
    parser.add_argument("--no-dry-run", action= 'store_false', dest='dry_run', default=True, help="In dry-run mode (default) the program will show the list of hashes, sizes and then the files. In no-dry-run mode, the system will generate the rm commands")
    args = parser.parse_args()
    # print(args.task, args.target, args)

    config.DRY_RUN = args.dry_run
    
    run(args)
