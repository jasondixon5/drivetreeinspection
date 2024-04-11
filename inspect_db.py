import sqlite3

db = 'drive_results.db' 
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
    """
   
   # Verify table is there
    
#    query = "SELECT name from sqlite_master WHERE name='drive'"
    cur = con.cursor()
    cur.execute(query)
    rows = cur.fetchall()

    # for row in rows:
    #     print(row)
    
    # query = "pragma table_info(drive)"
    # cur = con.cursor()
    # cur.execute(query)
    # rows = cur.fetchall()
    # for row in rows:
    #     print(row)

    print(f"# of rows: {len(rows)}")
    

con.close()




