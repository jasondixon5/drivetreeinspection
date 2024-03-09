import csv
import os.path

from datetime import datetime, timedelta

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# If modifying these scopes, delete the file token.json.
SCOPES = ["https://www.googleapis.com/auth/drive.metadata.readonly"]

# TODOS:
# * Set output to screen for each request so know script is still working
# * Look into read operation timeout error; possible need for chunking
# * Add path info (key?)
# * Avoid writing headers if not first write to file
# * Add timestamp to output filename
# * Find out why script may be writing same file multiple times





def main():
  
  # creds = provide_creds() 
  # service = build("drive", "v3", credentials=creds)

  # request_file_info(service)

  # print(get_minimum_timestamp(create_timestamp_bookends()))
  # print(create_timestamp_bookends())

  ts = create_timestamp_bookends()
  qc = create_query_clauses(ts)
  for q in qc:
    print(q) 

  print("Finished script.")

def create_timestamp_bookends():

  timestamp_bookends = []

  yearly_queries_cap = 10
  current_year = datetime.now().year

  for years_ago in range(0, yearly_queries_cap):
    yr = current_year - years_ago
    start_str = datetime(yr, 1, 1).strftime('%Y-%m-%dT00:00:00')
    # end_str = datetime(yr, 12, 31).strftime('%Y-%m-%dT12:59:59')
    
    # timestamp_bookends.append((start_str, end_str))
    timestamp_bookends.append(start_str)

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
              pageSize=100, 
              fields="nextPageToken, files(kind, id, name, parents, mimeType)",
              q="trashed=false and createdTime >= '2023-01-01T00:00:00'",
              pageToken=page_token,
            )
          .execute()
        )
        
        page_token = results.get("nextPageToken")

        items = results.get("files", [])
        handle_items(items)

        if page_token:
          call_count += 1
        else:
          call_count = -1
    
  except HttpError as error:
    # TODO(developer) - Handle errors from drive API.
    print(f"An error occurred: {error}")

def handle_items(items):

  if not items:
    print("No files found.")
    return

  try:
    
    with open('inspect_results.csv', 'a', newline='') as csvfile:

        fieldnames = ['kind', 'id', 'name', 'parents', 'mimeType']
        writer=csv.DictWriter(csvfile, fieldnames=fieldnames)

        for item in items:

          try:

            item_id = item['id']
            item_name = item['name']
            item_parents = item['parents'][0] if item.get('parents') is not None else ''
            item_kind = item['kind']
            item_mime_type = item['mimeType']

            writer.writerow({
              'kind': item_kind,
              'id': item_id, 
              'name': item_name,
              'parents': item_parents,
              'mimeType': item_mime_type,
              })

          except Exception as e:
            print(f"Exception encountered when trying to write to file:\n")
            print(item)
            print(f"Exception: {e}")
          
  except Exception as e:
      print(f"Exception encountered when trying to write to file:\n\n{e}")


  print("Finished batch of files.")

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