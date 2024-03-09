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
SCOPES = ["https://www.googleapis.com/auth/drive.metadata.readonly"]

# TODOS:
# * Look into read operation timeout error; possible need for chunking
# * Avoid writing headers if not first write to file


def main():
  
  creds = provide_creds() 
  service = build("drive", "v3", credentials=creds)
  
  ts = create_timestamp_bookends(10)
  qc = create_query_clauses(ts)

  # Query api and store results
  request_file_info(service=service, query_list=qc)

  # Print audit info to stdout
  check_db("drive_results.db", "drive")

  print("Finished script.")

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
  # timestamp_text_templ = f"createdTime >= '{timestamp_pair[0]}' and createdTime <= {}" 

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
              fields="nextPageToken, files(id, name, parents, mimeType, createdTime)",
              q=q,
              pageToken=page_token,
            )
          .execute()
        )
        
        page_token = results.get("nextPageToken")

        items = results.get("files", [])
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
            item_created = item['createdTime']

            writer.writerow([
              item_id, 
              item_name,
              item_parents,
              item_mime_type,
              is_folder,
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


  con = sqlite3.connect("drive_results.db")
  cur = con.cursor()

  # Check if table already exists and create if not
  res = cur.execute("SELECT name from sqlite_master WHERE name='drive'")
  if res.fetchone() is None:
    cur.execute("CREATE TABLE drive(id, name, parents, mime_type, is_folder, created)")

  for item in items:

    data = ({
      "item_id": item['id'],
      "item_name": item['name'],
      "item_parents": (item['parents'][0] if item.get('parents') is not None else ''),
      "item_mime_type": item['mimeType'],
      "is_folder": (1 if item['mimeType'] == 'application/vnd.google-apps.folder' else 0),
      "item_created": item['createdTime'],
    })

    cur.execute("""
      INSERT INTO drive VALUES(
        :item_id, 
        :item_name, 
        :item_parents, 
        :item_mime_type, 
        :is_folder, 
        :item_created)""", data)

    con.commit()

  print("Finished batch of files.")

def check_db(db_name, table_name):

  con = sqlite3.connect(db_name)

  cur = con.cursor()

  print(f"Table count for {table_name}")
  print(cur.execute(f"SELECT COUNT(*) FROM {table_name}").fetchall())
  
  # drive_results = cur.execute(f"SELECT * FROM {table_name} LIMIT 100")
  print("Result sample...\n")
  for row in cur.execute(f"SELECT * FROM {table_name} LIMIT 100"):
    print(row)
  
  


def provide_creds():

  creds = None
  # The file token.json stores the user's access and refresh tokens, and is
  # created automatically when the authorization flow completes for the first
  # time.
  if os.path.exists("token.json"):
    creds = Credentials.from_authorized_user_file("token.json", SCOPES)
  # If there are no (valid) credentials available, let the user log in.
  if not creds or not creds.valid:
    if creds and creds.expired and creds.refresh_token:
      # Hack to get around refresh error
      try:
        creds.refresh(Request())
      except Exception as e:
        print(f"Workaround for: \n {e}")
        flow = InstalledAppFlow.from_client_secrets_file(
          "credentials.json", SCOPES
          )
        creds = flow.run_local_server(port=0)
    else:
      flow = InstalledAppFlow.from_client_secrets_file(
          "credentials.json", SCOPES
      )
      creds = flow.run_local_server(port=0)
    
    # Save the credentials for the next run
    with open("token.json", "w") as token:
      token.write(creds.to_json())

  return creds

if __name__ == "__main__":
  main()