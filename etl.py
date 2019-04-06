import os
import glob
import psycopg2
import pandas as pd
import json 
import datetime 

from sql_queries import *


def process_song_file(cur, filepath):
    # open song file
    df = pd.read_json(filepath, typ='records')

    # # insert song record
    song_data = tuple(df[["song_id", 
                          "title", 
                          "artist_id", 
                          "year", 
                          "duration"]])
    cur.execute(song_table_insert, song_data)

    # # insert artist record
    artist_data = tuple(df[["artist_id", 
                            "artist_name", 
                            "artist_location", 
                            "artist_latitude", 
                            "artist_longitude"]])
    cur.execute(artist_table_insert, artist_data)

def process_log_file(cur, filepath):

    df = pd.read_json(filepath, lines=True)

    # filter based on next song
    df = df.loc[df['page'] == 'NextSong']
    t = df['ts']

    def extract_time_record(date):
        datetime_object = datetime.datetime.utcfromtimestamp(date/1000.)
        hour = datetime_object.strftime('%H')
        day = datetime_object.strftime('%d')
        week = datetime_object.strftime('%U')
        month = datetime_object.strftime('%m')
        year = datetime_object.strftime('%Y')
        weekday = datetime_object.strftime('w')
        return {
            'start_time': datetime_object,
            'hour': int(hour),
            'day': int(day),
            'week': int(week),
            'month': int(month),
            'year': int(year)
        }

    time_df = pd.DataFrame(list(map(extract_time_record, list(t))))
    time_df = time_df[['start_time', 'hour', 'day', 'week', 'month', 'year']]

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))
    # # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # # insert songplay records
    for index, row in df.iterrows():

    #     # get songid and artistid from song and artist tables
        query_parameters = (row.song, row.artist, row.length)
        results = cur.execute(song_select, query_parameters)
        songid, artistid = results if results else None, None

    #     # insert songplay record
        songplay_data = (datetime.datetime.utcfromtimestamp(row.ts / 1000.), 
                         row.userId, 
                         songid, 
                         artistid, 
                         row.sessionId, 
                         row.location, 
                         row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
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
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()