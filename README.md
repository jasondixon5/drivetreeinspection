# Drive Tree Inspection

After installing requirements, run:

`python summarize_google_drive.py`

# Program Steps

The program will perform the following activities:

1. If this is the first run of the program, set up credentials. A browser window will open to accept the permissions.
2. Query the user's Google Drive
3. Store the queried data in a db.
4. Query the db and perform a series of transformations and analysis.
5. Create and save an output file locally that summarizes the drive info.