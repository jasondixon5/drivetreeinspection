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
          "https://www.googleapis.com/auth/drive.readonly"]

# TODOS:
# * Look into read operation timeout error; possible need for chunking
# * Avoid writing headers if not first write to file
# * Create new script with ability to pass arguments to do things like
#   create/replace db and do db + output report with one command


def main():
  
  # creds = provide_creds() 
  # service = build("drive", "v3", credentials=creds)
  
  # examples = {
  #   "Folder at root level": "11-l1BkeUwvHg33Il71Nw3aLQoJkA6qnr",
  #   "File within a folder": '1nYIOcYHOSJJLocOB7JtGfO6g57ITfvoY',
  #   "Root drive id": '0AH0oInLp4i6JUk9PVA',
  #   "File at root level": '1CJxjOHX9fnhsVmwB4aR6uBVdHzzwH8KD',
  #   "Shared folder not owned by drive owner": '0BwHomZPdcm1cT2VVcTFSWGxpdEU',
  #   "Shared file not owned by drive owner":  '19USGyx8zWOIKlG7OeobJgfAlLk9MlWl1Fl2NqjJxVFo',
  # }
  
  
  # for desc, file_id in examples.items():
    
  #   print(desc)
  #   query_one_file(service, file_id)
  #   print("\n")

    # ts = create_timestamp_bookends(10)
  # qc = create_query_clauses(ts)

  # db_name = "drive_results.db"
  # table_name = "drive"
  
  # # Drop table from db if exists
  # drop_table(db_name, table_name)

  # # Query api and store results
  # request_file_info(service=service, query_list=qc)

  # # Print audit info
  # check_db(db_name, table_name)
    
  #request_drive_info(service)

  # get_file_types(service)

  print("WARNING: Run summarize script instead of this script.")
  print("Finished script.")

  return 0

def query_one_file(service, file_id):

  q = f"id = {file_id}"

  # results = (
  #         service.files()
  #         .list(
  #             pageSize=1000, 
  #             fields="files(id, name, parents, mimeType, size, createdTime)",
  #             q=q,
  #           )
  #         .execute()
  #       )
  results = service.files().get(
    fileId=file_id, 
    fields='id, name, mimeType, createdTime, modifiedTime, parents'
  ).execute()

  print(results)

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
      # print(items)
      print(results)

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

  print("File types")
  file_types = sorted(list(file_types))
  for ft in file_types:
    print(ft)



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


  con = sqlite3.connect("drive_results.db")
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

  print("Finished batch of files.")

def check_db(db_name, table_name):

  con = sqlite3.connect(db_name)

  cur = con.cursor()

  print(f"Table count for {table_name}")
  print(cur.execute(f"SELECT COUNT(*) FROM {table_name}").fetchall())
  
  # drive_results = cur.execute(f"SELECT * FROM {table_name} LIMIT 100")
  print("Result sample...\n")
  for row in cur.execute(f"SELECT * FROM {table_name} LIMIT 10"):
    print(row)
  
def drop_table(db_name, table_name):
  
  con = sqlite3.connect(db_name)
  cur = con.cursor()

  # Check if table already exists and drop if it does
  res = cur.execute(f"SELECT name from sqlite_master WHERE name='{table_name}'")
  
  if res.fetchone() is not None:
    cur.execute(f"DROP TABLE {table_name}")


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