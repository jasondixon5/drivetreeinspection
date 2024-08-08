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
    add_cumulative_folder_size_to_folders_var,
    add_folder_path_to_folder_var,
    add_parent_name_to_folder_var,
    get_documents,
    get_folders, 
    output_report,
    set_up_folder_var,
    stringify_folder_path,
    summarize_rows,
    walk_folder_path,
    write_output_to_db,
)

# TODO: Handle potential missing crdentials file

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

def transform(db):

    folder_rows = get_folders(db)
    document_rows = get_documents(db)

    print("Setting up folder info")
    folders = set_up_folder_var(folder_rows)

    print("Adding parent name info") 
    folders = add_parent_name_to_folder_var(folders)
    
    print("Adding folder path info") 
    folders = add_folder_path_to_folder_var(folders)
    
    print("Calculating cumulative folder size info")
    folders = add_cumulative_folder_size_to_folders_var(db, folders)
    
    return folders

def write_summary_to_file(folders):

    output_report(folders)

def write_summary_to_db(db, folders):

    write_output_to_db(folders, db)

def output_the_data(db, folders):

    write_summary_to_db(db, folders)
    write_summary_to_file(folders)
    

def main(db_name=DB_NAME, scopes=SCOPES):

    create_db(DB_NAME, SCOPES)
    folders = transform(DB_NAME)
    output_the_data(DB_NAME, folders)
    
    print("\nFinished script.\n")

    return 0
    
if __name__ == "__main__":

    main()    
