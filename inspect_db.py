import argparse
import csv
import sqlite3

from datetime import datetime

# TODO: Handle edge case of id for root folder (My Drive). Doesn't show up in table but does show up as parent id of folders at root
# TODO: Decide if need sentinel value in details array to say it's a shared folder (shared but not owned by script runner)
# TODO: Determine how shared-unowned folders AND documents appear in db


# def check_for_root_folder(db):

#     con = sqlite3.connect(db)

#     print("Query for size of drive table")
#     with con:
#         query = """SELECT COUNT(*) FROM drive"""

#     cur = con.cursor()
#     cur.execute(query)
#     rows = cur.fetchall()
#     con.close()
    
#     for row in rows:
#         print(row)
    
#     con = sqlite3.connect(db)
#     print("Results of query to db for root drive info")
#     with con:
        
#         query = """SELECT
#             id
#         , name
#         , parents
#         , mime_type
#         , is_folder
#         , size
#         , created
#         FROM drive
#         WHERE name = 'My Drive'
#         """
    
#     cur = con.cursor()
#     cur.execute(query)
#     rows = cur.fetchall()
#     con.close()
#     for row in rows:
#         print(row)
#     print(f"Fetched {len(rows)} folders.")

#     return rows
 


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
    folders['0'] = ['root', '', 0, 0, 0, 0]

    return folders

def add_parent_name_to_folder_var(folders):

    errors = set()
    for id, details in folders.items():
        # Add name of parent folder to details array
        parent_id = details[1]
        parent_details = folders.get(parent_id, folders.get('0')) # If no details for parent in db, must be root

        if parent_details is None:
            errors.add(parent_id)        
        else:
            parent_name = parent_details[0]
            folders[id].append(parent_name)
            
    print(f"Errors during parent name retrieval: {errors}")

    return folders

def walk_folder_path(folders, folder_id):

    """NOTE: Requires parent name to be added to folder var before using"""

    folder_path = []

    folder_detail = folders.get(folder_id)
    
    while folder_detail:

        parent_id = folder_detail[1]
        parent_name = folder_detail[6]
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
            folder_details.append(folder_path_str)

    for folder_id in list(folder_id_list)[0:10]:
        print(f"Details for {folder_id}: {folders.get(folder_id)}")

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

def summarize_rows(folders, limit=None):

    count = 0
    print(f"Summarizing folders and related info up to limit {limit}")
    for folder, val in folders.items():
        if limit is not None and count < 0:
            break
        else:
            print(folder, val)
            count -= 1

def output_report(folders):

    dt = datetime.now().strftime("%Y-%m-%d-%I-%M-%S-%p")
    output_filename = f"inspect_results_{dt}.csv"

    print(f"Creating output filename {output_filename}")

    with open(output_filename, 'a', newline='') as csvfile:

        writer = csv.writer(csvfile)

        headers = [
            "Folder Name",
            "Parent ID",
            "Parent Name",
            "Size (bytes)",
            "Size (KB)",
            "Size (MB)",
            "Size (GB)",
            "Path",
            "Folder URL",
        ]
        
        writer.writerow(headers)        

        for folder_id, folder_details in folders.items():
            
            name = folder_details[0]
            parent = folder_details[1]
            parent_name = folder_details[6]
            size_bytes = folder_details[2]
            size_kb = folder_details[3]
            size_mb = folder_details[4]
            size_gb = folder_details[5]
            folder_path = folder_details[7]
            folder_url = f"https://drive.google.com/drive/u/0/folders/{folder_id}"

            writer.writerow([
                name,
                parent,
                parent_name,
                size_bytes,
                size_kb,
                size_mb,
                size_gb,
                folder_path,
                folder_url,
            ])

    print(f"Finished writing output filename {output_filename}")


# def check_root_dir(db):

#     con = sqlite3.connect(db)

#     with con:
        
#         query = """SELECT
#             id
#         , name
#         , parents
#         , mime_type
#         , is_folder
#         , size
#         , created
#         FROM drive
#         WHERE id = '0AH0oInLp4i6JUk9PVA'
#         """
    
#     cur = con.cursor()
#     cur.execute(query)
#     rows = cur.fetchall()
#     con.close()

#     print("Query to inspect table contents for root folder.")
#     print(f"Fetched {len(rows)} folders.")

#     for row in rows:
#         print(row)

#     return rows

# def check_nepal_shared_folder(db):

#     con = sqlite3.connect(db)

#     with con:
        
#         query = """SELECT
#             id
#         , name
#         , parents
#         , mime_type
#         , is_folder
#         , size
#         , created
#         FROM drive
#         WHERE id = '0BwHomZPdcm1cT2VVcTFSWGxpdEU'
#         """
    
#     cur = con.cursor()
#     cur.execute(query)
#     rows = cur.fetchall()
#     con.close()

#     print("Query to inspect table contents for root folder.")
#     print(f"Fetched {len(rows)} folders.")

#     for row in rows:
#         print(row)

#     return rows

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

    print("Query to inspect table contents for folders without parents.")
    print(f"Fetched {len(rows)} folders.")

    for row in rows:
        print(row)

    return rows

def argumentify():

    """
    Arguments

    : 
    """

    parser = argparse.ArgumentParser()

def main():

    db = 'drive_results.db'

    # check_for_root_folder(db)

    folder_rows = get_folders(db)
    document_rows = get_documents(db)
    print("Setting up folder variable")
    folders = set_up_folder_var(folder_rows)
    print("Filling folder variable")
    folders = fill_folder_var(folders, document_rows)
    print("Adding parent name to folder variable")
    folders = add_parent_name_to_folder_var(folders)
    
    print('\n*************************\n')
    print("Building path for folders")
    # test_id = '0B30oInLp4i6JYXpnRG1xX0YyaHc' 
    # print(f"info for test folder {test_id}")
    # print(folders.get(test_id))
    # parent_of_test_id = '0AH0oInLp4i6JUk9PVA'
    # print(folders.get(parent_of_test_id))
    # folders = add_path_list_to_folder_var_all_folders(folders, test_id) 
    # NB: To get print statements within function as func is running, do not store results in var
    # add_path_list_to_folder_var_all_folders(folders, test_id) 

    # test_id = '0B30oInLp4i6JeHozYjR5UVF0R1E'
    # print(f"info for test folder {test_id}")
    # print(folders.get(test_id))
    # folders = add_path_list_to_folder_var_all_folders(folders, test_id)
    # print(folders.get(test_id))
    
    # test_id = '0B30oInLp4i6JeHozYjR5UVF0R1E' # Nested two levles
    # walk_folder_path(folders, test_id)
    # print('\n*************************\n') 
    
    # test_id = '1CqkDcdjTakchhxTl_wXfYi3GRFutxdRI' # At root
    # walk_folder_path(folders, test_id)
    # print('\n*************************\n')

    # test_id = '0BwHomZPdcm1cT2VVcTFSWGxpdEU' # Nepal, unowned shared at root
    # walk_folder_path(folders, test_id)
    # print('\n*************************\n')

    # test_id = '0B30oInLp4i6JYloweGpzRVFxREU' # Nepal subfolder, nested two levels
    # folder_path = walk_folder_path(folders, test_id)
    # print(stringify_folder_path(folder_path))
    # print('\n*************************\n')

    folders = add_folder_path_to_folder_var(folders) 

    # summarize_rows(folders, 5)
    # output_report(folders)
    # add_parent_name_to_folder_var(folders)
    # check_root_dir(db)
    # check_nepal_shared_folder(db)
    # check_no_parent_folders(db)
    
    print("WARNING: Run summarize script instead of this script.")
    print("Finished script.")
    
    return 0

if __name__ == "__main__":
    main()
