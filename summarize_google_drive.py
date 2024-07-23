import csv
import os.path
import sqlite3

from datetime import datetime, timedelta
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from inspectdrive import (
    check_db,
    create_query_clauses, 
    create_timestamp_bookends, 
    drop_table, 
    handle_missing_parents,
    provide_creds, 
    request_file_info,
    DB_NAME,
    SCOPES,
)

from inspect_db import (
    add_folder_path_to_folder_var,
    add_parent_name_to_folder_var,
    get_documents,
    get_folders, 
    output_report,
    set_up_folder_var,
    stringify_folder_path,
    summarize_rows,
    walk_folder_path,
)

db = 'drive_results.db'


def create_db(db, scopes):
  
    creds = provide_creds(scopes) 
    service = build("drive", "v3", credentials=creds)
  
    ts = create_timestamp_bookends(10)
    qc = create_query_clauses(ts)

    table_name = "drive"
  
    # Drop table from db if exists
    drop_table(db, table_name)

    # Query api and store results
    print("Reading Google Drive info.")
    request_file_info(service=service, query_list=qc)

    # Print audit info  
    check_db(db, table_name)

    print("Finished creating db.")

    print("****HANDLING ANY MISSING PARENT VALUES****")
    handle_missing_parents(db, service)


    return None

def create_report(db):

    folder_rows = get_folders(db)
    document_rows = get_documents(db)
    folders = set_up_folder_var(folder_rows)
    folders = add_parent_name_to_folder_var(folders)
    folders = add_folder_path_to_folder_var(folders) 
    
    print("Summary of data to output:\n")
    data_summary = summarize_rows(folders, 5)
    for folder, detail in data_summary:
        print(folder, detail)

    output_report(folders)


if __name__ == "__main__":
    create_db(DB_NAME, SCOPES)
    create_report(DB_NAME)          
