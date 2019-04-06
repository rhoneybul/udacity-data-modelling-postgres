# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays(
    songplay_id SERIAL,
    start_time TIMESTAMP,
    user_id INT,
    song_id INT,
    artist_id INT,
    session_id INT,
    location VARCHAR,
    user_agent VARCHAR,
    PRIMARY KEY (songplay_id)
)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users(
    user_id INT NOT NULL,
    first_name VARCHAR,
    last_name VARCHAR,
    gender VARCHAR,
    level VARCHAR,
    PRIMARY KEY (user_id)
)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs(
    song_id VARCHAR NOT NULL,
    title VARCHAR,
    artist_id VARCHAR,
    year INT,
    duration FLOAT,
    PRIMARY KEY (song_id)
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists(
    artist_id VARCHAR NOT NULL,
    name VARCHAR,
    location VARCHAR,
    latitude FLOAT,
    longitude FLOAT,
    PRIMARY KEY (artist_id)
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time(
    start_time TIMESTAMP,
    hour INT,
    day INT,
    week INT,
    month INT, 
    year INT,
    weekday INT,
    PRIMARY KEY (start_time)
)
""")

# INSERT RECORDS

songplay_table_insert = ("""
insert into songplays (start_time, user_id, song_id, artist_id, session_id, location, user_agent)
    values (%s, %s, %s, %s, %s, %s, %s)
""")

user_table_insert = ("""
insert into users (user_id, first_name, last_name, gender, level)
    values (%s, %s, %s, %s, %s)
    on conflict (user_id) do nothing
""")

song_table_insert = ("""
insert into songs (song_id, title, artist_id, year, duration) 
       values (%s, %s, %s, %s, %s)
       on conflict (song_id) do nothing
""")

artist_table_insert = ("""
insert into artists(artist_id, name, location, latitude, longitude)
    values (%s, %s, %s, %s, %s)
    on conflict (artist_id) do nothing
""")


time_table_insert = ("""
insert into time(start_time, hour, day, week, month, year)
    values (%s, %s, %s, %s, %s, %s)
    on conflict (start_time) do nothing
""")

# FIND SONGS

song_select = ("""
select song_id, artists.artist_id from songs 
    join artists on songs.artist_id=artists.artist_id 
    where songs.title=%s and artists.name=%s and songs.duration=%s
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]