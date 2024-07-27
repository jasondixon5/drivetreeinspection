import argparse
import csv
import sqlite3

from datetime import datetime

def get_folders(db):

    con = sqlite3.connect(db)

    with con:
        
        query = """SELECT
            id
        , name
        , parents
        , mime_type
        , is_folder
        , size
        , created
        FROM drive
        WHERE is_folder = 1
        """
    
    cur = con.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    con.close()

    return rows

def set_up_folder_var(rows):

    folders = {}

    for row in rows:

        id = row[0]
        name = row[1]
        parent_id = row[2]

        folders[id] = {
            "name": name,
            "parent_id": parent_id,
            "size_bytes": 0,
            "size_mb": 0,
            "size_kb": 0,
            "size_gb": 0,
            "parent_name": None,
        }

    # Add default entry for shared items not owned by user
    folders['shared'] = {
        "name": "shared",
        "parent_id": '',
        "size_bytes": 0,
        "size_mb": 0,
        "size_kb": 0,
        "size_gb": 0,
        "parent_name": ''
    }
    
    return folders

def add_parent_name_to_folder_var(folders):

    for id, details in folders.items():
        # Add name of parent folder to details array
        
        # Files with no parent ID can be:
        # * shared folders unowned by user
        # * the root folder (My Drive)        
        parent_id = details.get("parent_id")
        parent_details = None

        if not parent_id and details.get("name") not in ("My Drive", 'shared'):
            parent_details = folders.get("shared")
            details["parent_id"] = "shared" # Not ideal to modify during iter but need for output
        else:
            parent_details = folders.get(parent_id)

        if parent_details:
            parent_name = parent_details.get("name")
            # Add parent name to details of the child file we're working on
            details["parent_name"] = parent_name
            
    return folders

def walk_folder_path(folders, folder_id):

    """NOTE: Requires parent name to be added to folder var before using"""

    folder_path = []

    folder_detail = folders.get(folder_id)
    
    while folder_detail:

        parent_id = folder_detail.get("parent_id")
        parent_name = folder_detail.get("parent_name")
        if parent_name:
            folder_path.append(parent_name)
        
        folder_detail = folders.get(parent_id)
        
    return folder_path

def stringify_folder_path(folder_path):

    # Input is a list representing the path of the folder
    # Path is built from walking upwards from current folder, so string version will be reverse

    rev_path = list(reversed(folder_path))
    return " -> ".join(rev_path)

def add_folder_path_to_folder_var(folders):

    folder_id_list = folders.keys()

    for folder_id in folder_id_list:
        folder_path_list = walk_folder_path(folders, folder_id)
        folder_path_str = stringify_folder_path(folder_path_list)
        
        folder_details = folders.get(folder_id)
        if folder_details:
            folder_details["folder_path_list"] = folder_path_list
            folder_details["folder_path_str"] = folder_path_str

    return folders

def get_documents(db):

    con = sqlite3.connect(db)

    with con:
        
        query = """SELECT
            id
        , name
        , parents
        , mime_type
        , is_folder
        , size
        , created
        FROM drive
        WHERE is_folder = 0
        """
    
    cur = con.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    con.close()

    return rows

def add_direct_doc_size_to_folder_var(folders, rows):

    for row in rows:

        folder_id = row[2]
        size = int(row[5]) if row[5] is not None else 0

        folder = folders.get(folder_id)
        if folder:
            # size is 2nd item in list
            folder["size_bytes"] += size
        else:
            # Store in default entry
            folder = folders['shared']
            folder["size_bytes"] += size

    # Calculate size in other than bytes
    for folder_id, folder_details in folders.items():
        
        byte_size = folder_details.get("size_bytes")
        folder_details["size_kb"] = round(byte_size / 1024, ndigits=2) #KB
        folder_details["size_mb"] = round(byte_size / 1_048_576, ndigits=2) #MB
        folder_details["size_gb"] = round(byte_size / 1_073_741_824, ndigits=2) #GB

    return folders

def summarize_rows(folders, limit=None):

    count = 0
    data_summary = []

    if limit is None:
        limit = 5

    
    for folder, val in folders.items():
        if count >= limit:
            break
        else:
            data_summary.append(tuple((folder, val)))
            count += 1

    return data_summary

def output_report(folders):

    dt = datetime.now().strftime("%Y-%m-%d-%I-%M-%S-%p")
    output_filename = f"inspect_results_{dt}.csv"

    print(f"\nCreating output filename {output_filename}\n")

    with open(output_filename, 'a', newline='') as csvfile:

        writer = csv.writer(csvfile)

        headers = [
            "Folder ID",
            "Folder Name",
            "Parent ID",
            "Parent Name",
            "Size (bytes)",
            "Size (KB)",
            "Size (MB)",
            "Size (GB)",
            "Path",
            "Path List",
            "Folder URL",
        ]
        
        writer.writerow(headers)        

        for folder_id, folder_details in folders.items():
            
            name = folder_details.get("name")
            parent = folder_details.get("parent_id")
            parent_name = folder_details.get("parent_name")
            size_bytes = folder_details.get("size_bytes")
            size_kb = folder_details.get("size_kb")
            size_mb = folder_details.get("size_mb")
            size_gb = folder_details.get("size_gb")
            folder_path = folder_details.get("folder_path_str")
            folder_path_list = str(folder_details.get("folder_path_list"))
            folder_url = f"https://drive.google.com/drive/u/0/folders/{folder_id}"

            writer.writerow([
                folder_id,
                name,
                parent,
                parent_name,
                size_bytes,
                size_kb,
                size_mb,
                size_gb,
                folder_path,
                folder_path_list,
                folder_url,
            ])

    print(f"\nFinished writing output filename {output_filename}\n")

def write_output_to_db(folders, db):

    table_name = 'folder_summary'

    stored_dt = datetime.now().strftime("%Y-%m-%d-%I-%M-%S-%p")
    

    print(f"\nWriting output to db {db}, table {table_name}\n")

    con = sqlite3.connect(db)
    cur = con.cursor()

    cur.execute(f"DROP TABLE IF EXISTS {table_name}")
    
    cur.execute(f"CREATE TABLE {table_name}(folder_id, name, parent_id, parent_name, size_bytes, size_kb, size_mb, size_gb, folder_path_str, folder_path_list, folder_url, stored)")

    
    for folder_id, folder_details in folders.items():

        data = ({
            "folder_id": folder_id,
            "name": folder_details.get("name"),
            "parent_id":  folder_details.get("parent_id"),
            "parent_name":  folder_details.get("parent_name"),
            "size_bytes":  folder_details.get("size_bytes"),
            "size_kb":  folder_details.get("size_kb"),
            "size_mb":  folder_details.get("size_mb"),
            "size_gb":  folder_details.get("size_gb"),
            "folder_path":  folder_details.get("folder_path_str"),
            "folder_path_list":  str(folder_details.get("folder_path_list")),
            "folder_url":  f"https://drive.google.com/drive/u/0/folders/{folder_id}",
            "stored": stored_dt,
        })

        cur.execute(f"""
                    INSERT INTO {table_name} VALUES(
                    :folder_id,
                    :name,
                    :parent_id,
                    :parent_name,
                    :size_bytes,
                    :size_kb,
                    :size_mb,
                    :size_gb,
                    :folder_path,
                    :folder_path_list,
                    :folder_url,
                    :stored
                    )""", data)
    
    con.commit()
            
    print(f"\nWrite complete.\n")

def check_no_parent_folders(db):

    con = sqlite3.connect(db)

    with con:
        
        query = """SELECT
            id
        , name
        , parents
        , mime_type
        , is_folder
        , size
        , created
        FROM drive
        WHERE is_folder = 1 AND (
            parents IS NULL
            OR parents = ''
        )
        """
    
    cur = con.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    con.close()

    return rows

def main():

    print("WARNING: Run summarize script instead of this script.")
    print("Finished script.")
    
    return 0

if __name__ == "__main__":
    main()
