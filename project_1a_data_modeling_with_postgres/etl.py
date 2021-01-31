import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """Process given song data file and insert the transformed data into PostgreSQL server
    via the provided cursor

    :param cur: database cursor object used to run the statements within single transaction
    :param filepath: JSON file to be processed
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']]
    for i, row in song_data.iterrows():
        cur.execute(song_table_insert, row)
    
    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']]
    for i, row in artist_data.iterrows():
        cur.execute(artist_table_insert, row)


def process_log_file(cur, filepath):
    """Process given event log file and insert the transformed data into PostgreSQL server
    via the provided cursor

    :param cur: database cursor object used to run the statements within single transaction
    :param filepath: JSON file to be processed
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == "NextSong"]
    df['ts'] = df['ts'].astype('datetime64[ms]')

    # convert timestamp column to datetime
    t = df['ts']
    
    # insert time data records
    time_data = []
    for data in t:
        time_data.append([data, data.hour, data.day, data.weekofyear, data.month, data.year, data.day_name()])

    column_labels = ['timestamp', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    time_df = pd.DataFrame.from_records(data=time_data, columns=column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (
            row['ts'],
            row['userId'],
            row['level'],
            songid,
            artistid,
            row['sessionId'],
            row['location'],
            row['userAgent']
        )
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """Recursively extracts data from JSON files found in specified directory, then processes each file the provided
    function.

    :param cur: db cursor object to be passed to the specified function
    :param conn: db connection object used to commit the transaction
    :param filepath: directory to be scanned for JSON files
    :param func: function to be used to process each found file
    """

    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """ETL pipeline for loading json data into PostgreSQL database"""
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()

    print("Processing has been finished successfully...")


if __name__ == "__main__":
    main()
