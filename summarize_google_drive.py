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
    provide_creds, 
    request_file_info
)

from inspect_db import (
    add_folder_path_to_folder_var,
    add_parent_name_to_folder_var,
    fill_folder_var,
    get_documents,
    get_folders, 
    output_report,
    set_up_folder_var,
    stringify_folder_path,
    summarize_rows,
    walk_folder_path,
)

db = 'drive_results.db'

# If modifying these scopes, delete the file token.json.
SCOPES = ["https://www.googleapis.com/auth/drive.metadata.readonly"]

def create_db():
  
    creds = provide_creds() 
    service = build("drive", "v3", credentials=creds)
  
    ts = create_timestamp_bookends(10)
    qc = create_query_clauses(ts)

    db_name = "drive_results.db"
    table_name = "drive"
  
    # Drop table from db if exists
    drop_table(db_name, table_name)

    # Query api and store results
    print("Reading Google Drive info.")
    request_file_info(service=service, query_list=qc)

    # Print audit info  
    check_db(db_name, table_name)

    print("Finished creating db.")

    return None

def create_report():

    folder_rows = get_folders(db)
    document_rows = get_documents(db)
    folders = set_up_folder_var(folder_rows)
    folders = fill_folder_var(folders, document_rows)
    folders = add_parent_name_to_folder_var(folders)
    folders = add_folder_path_to_folder_var(folders) 
    summarize_rows(folders, 5)
    output_report(folders)


if __name__ == "__main__":
    create_db()
    create_report()

          
