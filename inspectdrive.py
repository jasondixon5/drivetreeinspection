import csv
import os.path

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



def main():
  """Shows basic usage of the Drive v3 API.
  Prints the names and ids of the first 10 files the user has access to.
  """
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

  request_file_info(creds)

def request_file_info(creds):
  call_count = 0

  try:
    service = build("drive", "v3", credentials=creds)
  
    while call_count >= 0:
      results = (
        service.files()
        .list(
            pageSize=100, 
            fields="nextPageToken, files(kind, driveId, id, name, parents, mimeType)",
            q="trashed=false and createdTime >= '2023-01-01T00:00:00'")
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
  # print("Files:")
  try:
    
    # print(items)
    with open('inspect_results.csv', 'a', newline='') as csvfile:
        fieldnames = ['kind', 'driveId', 'id', 'name', 'parents']
        writer=csv.DictWriter(csvfile, fieldnames=fieldnames)
        # writer.writeheader()
        for item in items:
          try:

            item_id = item['id']
            item_name = item['name']
            item_parents = item['parents'][0] if item.get('parents') is not None else ''
            item_kind = item['kind']
            item_drive_id = item['driveId']
            item_mime_type = item['mimeType']

            writer.writerow({
              'kind': item_kind,
              'driveId': item_drive_id,
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


  print("Finished drive inspection script.")

 
if __name__ == "__main__":
  main()