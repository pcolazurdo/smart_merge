import argparse
import os
import sys
import traceback
import uuid
from pathlib import Path

import pandas as pd

from config import ScanConfig
from data_store import DataStore, FileRecord, ErrorRecord

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

    query = None
    if task == 'list':
        if target == 'sessions':
            query = """SELECT DISTINCT session_id FROM files"""
        if target == 'duplicatedpaths':
            query = """SELECT file_name, COUNT(*) as cnt FROM files"""
            if len(session_ids) > 0:
                s = ', '.join([f"'{sess}'" for sess in session_ids])
                sessions_list = f'({s})'
                query = f"""{query} WHERE session_id in {sessions_list}"""
            query = f"""{query} GROUP BY file_size, file_hash HAVING cnt > 1"""
        if target == 'duplicated':
            query2 = """SELECT file_hash, file_size, COUNT(*) as cnt FROM files"""
            query2 = f'{query2} WHERE file_hash != "{config.UNDER_THRESHOLD_TEXT}"'
            if len(session_ids) > 0:
                s = ', '.join([f"'{sess}'" for sess in session_ids])
                sessions_list = f'({s})'
                query2 = f"""{query2} AND session_id in {sessions_list}"""
            query2 = f"""{query2} GROUP BY file_size, file_hash HAVING cnt > 1"""
            query = f"""SELECT f.file_hash, f.file_size, f.file_name FROM files AS f INNER JOIN ({query2}) AS q ON
             f.file_hash == q.file_hash 
             AND f.file_size == q.file_size 
             ORDER BY f.file_hash, f.file_size
             """
        if target == 'files':
            query = """SELECT * FROM files"""
            query = f'{query} WHERE file_hash != "{config.UNDER_THRESHOLD_TEXT}"'
            if len(session_ids) > 0:
                s = ', '.join([f"'{sess}'" for sess in session_ids])
                sessions_list = f'({s})'
                query = f"""{query} AND session_id in {sessions_list}"""
            
    if task == 'count':
        if target == 'sessions':
            query = """SELECT COUNT(DISTINCT session_id) FROM files"""
        if target == 'files':
            query = """SELECT * FROM files"""
            if len(session_ids) > 0:
                s = ', '.join([f"'{sess}'" for sess in session_ids])
                sessions_list = f'({s})'
                query = f"""{query} WHERE session_id in {sessions_list}"""
        
    
    if query:
        try:
            print(query)
            result = ds.execute_query(query)
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
    print(args.task, args.target, args)
    
    run(args)
