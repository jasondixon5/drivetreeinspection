import sqlite3



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

        folders[id] = [name, 0, parent_id]

    # Add default entry
    folders['0'] = ['default', 0, '']

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
            folder[1] += size
        else:
            # Store in default entry
            folder = folders['0']
            folder[1] += size

        
    return folders

def summarize_rows(folders):

    for folder, val in folders.items():
        print(folder, val)


def main():

    db = 'drive_results.db' 
    folder_rows = get_folders(db)
    document_rows = get_documents(db)
    folders = set_up_folder_var(folder_rows)
    folders = fill_folder_var(folders, document_rows)
    summarize_rows(folders)

    return 0

if __name__ == "__main__":
    main()


# with con:
#     query = """SELECT
#         id
#     , name
#     , parents
#     , mime_type
#     , is_folder
#     , size
#     , created
#     FROM drive
#     """
   
#    # Verify table is there
    
# #    query = "SELECT name from sqlite_master WHERE name='drive'"
#     cur = con.cursor()
#     cur.execute(query)
#     rows = cur.fetchall()

#     # for row in rows:
#     #     print(row)
    
#     # query = "pragma table_info(drive)"
#     # cur = con.cursor()
#     # cur.execute(query)
#     # rows = cur.fetchall()
#     # for row in rows:
#     #     print(row)

#     print(f"# of rows: {len(rows)}")
    

# con.close()




