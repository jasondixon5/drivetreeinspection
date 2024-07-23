import csv
import os.path
import sqlite3

from datetime import datetime, timedelta


from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# If modifying these scopes, delete the file token.json.
SCOPES = [
                    "https://www.googleapis.com/auth/drive.metadata.readonly",
]

DB_NAME = "drive_results.db"

# TODOS:


def main():
    
    # missing_parents = check_missing_parents(DB_NAME)
    # for p in missing_parents:
    #     print(p)

    # print("\n***********\n")

    # creds = provide_creds(SCOPES) 
    # service = build("drive", "v3", credentials=creds)

    # id_detail_map = query_api_for_missing_parents(missing_parents, service)

    # for parent_id, detail in id_detail_map.items():
    #     print(parent_id)
    #     print(detail)
    #     print("\n")

    # creds = provide_creds(SCOPES) 
    # service = build("drive", "v3", credentials=creds)
    # print("****HANDLING ANY MISSING PARENT VALUES****")
    # handle_missing_parents(DB_NAME, service)

    # print("Checking db for top-level id")
    # check_db_for_specific_entry(DB_NAME, 'drive', '0AH0oInLp4i6JUk9PVA')

    print("WARNING: Run summarize script instead of this script.")
    print("Finished script.")

    return 0

def query_one_file(service, file_id):

    q = f"id = {file_id}"

    results = service.files().get(
        fileId=file_id, 
        fields='id, name, mimeType, createdTime, modifiedTime, parents'
    ).execute()

    return results
                
def create_timestamp_bookends(yearly_queries_cap):

    timestamp_bookends = []

    current_year = datetime.now().year

    for years_ago in range(0, yearly_queries_cap):
        yr = current_year - years_ago
        start = datetime(yr, 1, 1).strftime('%Y-%m-%dT00:00:00')
            
        timestamp_bookends.append(start)

    return sorted(timestamp_bookends)

def create_query_clauses(timestamp_list):

    non_time_query = "trashed=false and "

    queries = []

    for i in range(len(timestamp_list) - 1):
        time_query = f"createdTime >= '{timestamp_list[i]}' and createdTime < '{timestamp_list[i+1]}'"    
        queries.append(non_time_query + time_query)

    # Handle before oldest and after newest timestmap
    # Assumes timestamps are sorted!!!
    before = f"createdTime < '{timestamp_list[0]}'" 
    queries.append(non_time_query + before)

    after = f"createdTime >= '{timestamp_list[-1]}'"
    queries.append(non_time_query + after)

    return queries

def request_drive_info(service):

    """
    Get info about a user's drives

    UPDATE: Deprecated. The drives api is for shared drives not root ("My Drive"),
    making it less useful for this project.
    """
        
    call_count = 0
    page_token = None

    while call_count >= 0:

        try:

            print("Call to fetch drive info")
                    
            results = (
                service.drives()
                .list(
                        pageSize=100, 
                        # fields="nextPageToken, files(id, name, kind, createdTime)",
                        pageToken=page_token,
                    )
                .execute()
            )
            
            page_token = results.get("nextPageToken")

            items = results.get("drives", [])

            if page_token:
                call_count += 1
            else:
                call_count = -1

        except HttpError as error:
            # TODO(developer) - Handle errors from drive API.
            print(f"An error occurred: {error}")
            call_count = -1

    return items

def get_file_types(service):

    """
    Helper function that uses similar approach as request_drive_info
    to inspect a drive. Returns list of the file types (MIME types) found.
    """

    call_count = 0
    page_token = None

    file_types = set()

    try:

        call_count = 0
        page_token = None
    
        while call_count >= 0:
                        
            results = (
                service.files()
                .list(
                        pageSize=1000, 
                        fields="nextPageToken, files(id, name, parents, mimeType, size, createdTime)",
                        pageToken=page_token,
                    )
                .execute()
            )
            
            page_token = results.get("nextPageToken")

            items = results.get("files", [])
            if items:
                for item in items:
                    file_types.add(item['mimeType'])
            
            if page_token:
                call_count += 1
            else:
                call_count = -1
    
    except HttpError as error:
        # TODO(developer) - Handle errors from drive API.
        print(f"An error occurred: {error}")

    file_types = sorted(list(file_types))

    return file_types
    


def request_file_info(service, query_list):
    
    call_count = 0
    page_token = None

    try:

        for q in query_list:
            
            call_count = 0
            page_token = None
        
            while call_count >= 0:
                            
                results = (
                    service.files()
                    .list(
                            pageSize=1000, 
                            fields="nextPageToken, files(id, name, parents, mimeType, size, createdTime)",
                            q=q,
                            pageToken=page_token,
                        )
                    .execute()
                )
                
                page_token = results.get("nextPageToken")

                items = results.get("files", [])
                # Store results in db                
                handle_items_db(items)

                if page_token:
                    call_count += 1
                else:
                    call_count = -1
        
    except HttpError as error:
        # TODO(developer) - Handle errors from drive API.
        print(f"An error occurred: {error}")

def handle_items_csv(items):

    if not items:
        print("No files found.")
        return

    try:
        
        with open('inspect_results.csv', 'a', newline='') as csvfile:

                writer = csv.writer(csvfile)

                for item in items:

                    try:

                        item_id = item['id']
                        item_name = item['name']
                        item_parents = item['parents'][0] if item.get('parents') is not None else ''
                        item_mime_type = item['mimeType']
                        is_folder = 1 if item_mime_type == 'application/vnd.google-apps.folder' else 0
                        item_size = item['size']
                        item_created = item['createdTime']

                        writer.writerow([
                            item_id, 
                            item_name,
                            item_parents,
                            item_mime_type,
                            is_folder,
                            item_size,
                            item_created,
                        ])

                    except Exception as e:
                        print(f"Exception encountered when trying to write to file:\n")
                        print(item)
                        print(f"Exception: {e}")
                    
    except Exception as e:
            print(f"Exception encountered when trying to write to file:\n\n{e}")


    print("Finished batch of files.")

def handle_items_db(items):

    if not items:
        print("No files found.")
        return


    con = sqlite3.connect(DB_NAME)
    cur = con.cursor()

    # Check if table already exists and create if not
    res = cur.execute("SELECT name from sqlite_master WHERE name='drive'")
    if res.fetchone() is None:
        cur.execute("CREATE TABLE drive(id, name, parents, mime_type, is_folder, size, created)")

    for item in items:

        data = ({
            "item_id": item['id'],
            "item_name": item['name'],
            # Research showed there was only one or 0 parents
            "item_parents": (item['parents'][0] if item.get('parents') is not None else ''),
            "item_mime_type": item['mimeType'],
            "is_folder": (1 if item['mimeType'] == 'application/vnd.google-apps.folder' else 0),
            "item_size": item.get('size'),
            "item_created": item['createdTime'],
        })

        cur.execute("""
            INSERT INTO drive VALUES(
                :item_id, 
                :item_name, 
                :item_parents, 
                :item_mime_type, 
                :is_folder,
                :item_size, 
                :item_created)""", data)

        con.commit()

    print("Stored batch of files in db.")

def check_db(db_name, table_name):

    con = sqlite3.connect(db_name)

    cur = con.cursor()

    table_row_count = cur.execute(f"SELECT COUNT(*) FROM {table_name}").fetchall()
    sample_rows = cur.execute(f"SELECT * FROM {table_name} LIMIT 10")

    return table_row_count, sample_rows

def check_db_for_specific_entry(db_name, table_name, id_of_entry):

    con = sqlite3.connect(db_name)
    cur = con.cursor()
    q = f"SELECT * FROM {table_name} WHERE id = '{id_of_entry}'"
    results = cur.execute(q)

    
    for r in results:
        print(r)
        print("\n")

    con.close()

def check_missing_parents(db_name):

    """
    Get the id of any folders that are listed as a parent but did
    not have an entry returned by the files API call.
    This should be the case for the top-level folder (My Drive).
    """

    con = sqlite3.connect(db_name)
    cur = con.cursor()

    q = """
        SELECT DISTINCT t1.parents
        FROM drive t1
        LEFT JOIN drive t2 ON t1.parents = t2.id
        WHERE t2.id IS NULL
        AND t1.parents != ''
    """
    missing_parents = cur.execute(q).fetchall()

    # Clean up format; it's a tuple of 1 item, convert to list
    missing_parents = [x[0] for x in missing_parents]

    return missing_parents

def query_api_for_missing_parents(missing_parents, service):

    """
    For any missing parents (other than empty string) query API for details
    so can add to the DB.
    Missing parent ID's that are not an empty string are usually the root folder.
    Missing parent ID's that are an empty string are usually a shared folder not owned by the user.
    """

    id_details_map = {} # Store the data as it's retrieved

    for parent_id in missing_parents:
        try:
            print(f"***Querying API for ID: {parent_id}****\n")
            results = (
                service.files()
                .get(
                        fileId=parent_id
                    )
                .execute()
            )

            id_details_map[parent_id] = results
        
        except HttpError as error:
            # TODO(developer) - Handle errors from drive API.
            print(f"An error occurred: {error}")

    
    for parent_id, details in id_details_map.items():

        # Add missing fields that are returned by call to files.list endpoint but not this one    
        details['parents'] = None
        details['size'] = 0
        details['createdTime'] = None


    return id_details_map

def add_missing_parents_entries_to_db(db, id_details_map):
        
        """
        After retrieving the info for parents that did not have an entry
        in the payload from the files.list endpoint, add entries for those
        parents to the db.
        This will usually just be the top-level My Drive folder.
        """
        con = sqlite3.connect(db)
        cur = con.cursor()

        for _, details in id_details_map.items():
        
            data = ({
                "item_id": details['id'],
                "item_name": details['name'],
                "item_parents": (details['parents'][0] if details.get('parents') is not None else ''),
                "item_mime_type": details['mimeType'],
                "is_folder": (1 if details['mimeType'] == 'application/vnd.google-apps.folder' else 0),
                "item_size": details.get('size'),
                "item_created": details['createdTime'],
            })

            cur.execute("""
            INSERT INTO drive VALUES(
                :item_id, 
                :item_name, 
                :item_parents, 
                :item_mime_type, 
                :is_folder,
                :item_size, 
                :item_created)""", data)

        con.commit()

def handle_missing_parents(db, service):

    """
    Helper function to check db for any parent values who don't have an
    entry in the table. 
    Usually this is the case for the top-level My Drive folder.
    """

    missing_parents = check_missing_parents(db)
    id_details_map = query_api_for_missing_parents(missing_parents, service)
    print(id_details_map)
    add_missing_parents_entries_to_db(db, id_details_map)

    
def drop_table(db_name, table_name):
    
    con = sqlite3.connect(db_name)
    cur = con.cursor()

    # Check if table already exists and drop if it does
    res = cur.execute(f"SELECT name from sqlite_master WHERE name='{table_name}'")
    
    if res.fetchone() is not None:
        cur.execute(f"DROP TABLE {table_name}")


def provide_creds(scopes):

    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", scopes)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            # Hack to get around refresh error
            try:
                creds.refresh(Request())
            except Exception as e:
                print(f"Workaround for: \n {e}")
                flow = InstalledAppFlow.from_client_secrets_file(
                    "credentials.json", scopes
                    )
                creds = flow.run_local_server(port=0)
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                    "credentials.json", scopes
            )
            creds = flow.run_local_server(port=0)
        
        # Save the credentials for the next run
        with open("token.json", "w") as token:
            token.write(creds.to_json())

    return creds

if __name__ == "__main__":
    main()