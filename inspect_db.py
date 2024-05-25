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

    print(f"Fetched {len(rows)} folders.")

    return rows

def set_up_folder_var(rows):

    folders = {}

    for row in rows:

        id = row[0]
        name = row[1]
        parent_id = row[2]

        folders[id] = [name, parent_id, 0, 0, 0, 0]

    # Add default entry
    folders['0'] = ['default', '', 0, 0, 0, 0]

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

    print(f"Fetched {len(rows)} documents (non folders).")

    return rows

def fill_folder_var(folders, rows):

    for row in rows:

        folder_id = row[2]
        size = int(row[5]) if row[5] is not None else 0

        folder = folders.get(folder_id)
        if folder:
            # size is 2nd item in list
            folder[2] += size
        else:
            # Store in default entry
            folder = folders['0']
            folder[2] += size

    # Calculate size in other than bytes
    for folder_id, folder_details in folders.items():
        
        byte_size = folder_details[2]
        folder_details[3] = round(byte_size / 1024, ndigits=2) #KB
        folder_details[4] = round(byte_size / 1_048_576, ndigits=2) #MB
        folder_details[5] = round(byte_size / 1_073_741_824, ndigits=2) #GB

    return folders

def build_folder_path(folders):

    # Get folder path
    print("Building folder path")
    for folder_id, folder_details in folders.items():
        folder_path_interim = []
        parent_id = folder_details[1]
        
        while parent_id != '':
            parent_details = folders.get(parent_id)
            if parent_details is None:
                continue
            else:

                parent_name = parent_details[0]
                folder_path_interim.append(parent_name)
        
        folder_path = "/".join(folder_path_interim)
        folder_details[6] = folder_path

def summarize_rows(folders):

    for folder, val in folders.items():
        print(folder, val)

def output_report(folders):

    dt = datetime.now().strftime("%Y-%m-%d-%I-%M-%S-%p")
    output_filename = f"inspect_results_{dt}.csv"

    with open(output_filename, 'a', newline='') as csvfile:

        writer = csv.writer(csvfile)

        headers = [
            "Folder Name",
            "Parent",
            "Size (bytes)",
            "Size (KB)",
            "Size (MB)",
            "Size (GB)",
            "Folder URL",
        ]
        
        writer.writerow(headers)        

        for folder_id, folder_details in folders.items():
            
            name = folder_details[0]
            parent = folder_details[1]
            size_bytes = folder_details[2]
            size_kb = folder_details[3]
            size_mb = folder_details[4]
            size_gb = folder_details[5]
            folder_url = f"https://drive.google.com/drive/u/0/folders/{folder_id}"

            writer.writerow([
                name,
                parent,
                size_bytes,
                size_kb,
                size_mb,
                size_gb,
                folder_url,
            ])

def main():

    db = 'drive_results.db' 
    folder_rows = get_folders(db)
    document_rows = get_documents(db)
    folders = set_up_folder_var(folder_rows)
    folders = fill_folder_var(folders, document_rows)
    # summarize_rows(folders)
    output_report(folders)

    return 0

if __name__ == "__main__":
    main()
