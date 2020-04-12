import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS STAGING_EVENTS"
staging_songs_table_drop = "DROP TABLE IF EXISTS STAGING_SONGS"
songplay_table_drop = "DROP TABLE IF EXISTS SONGPLAYS"
user_table_drop = "DROP TABLE IF EXISTS USERS"
song_table_drop = "DROP TABLE IF EXISTS SONGS"
artist_table_drop = "DROP TABLE IF EXISTS ARTISTS"
time_table_drop = "DROP TABLE IF EXISTS TIME"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS STAGING_EVENTS(
        event_id INT IDENTITY(0,1),
        artist_name VARCHAR(300),
        auth VARCHAR(300),
        user_first_name VARCHAR(300),
        user_gender VARCHAR(1),
        item_in_session INTEGER,
        user_last_name VARCHAR(300),
        song_length DOUBLE PRECISION,
        user_level VARCHAR(300),
        location VARCHAR(300),
        method VARCHAR(300),
        page VARCHAR(300),
        registration VARCHAR(300),
        session_id BIGINT,
        song_title VARCHAR(300),
        status INTEGER,
        ts VARCHAR(300),
        user_agent TEXT,
        user_id INTEGER,
        PRIMARY KEY (event_id));
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS STAGING_SONGS(
        song_id VARCHAR(300),
        num_songs INTEGER,
        artist_id VARCHAR(300),
        artist_latitude DOUBLE PRECISION,
        artist_longitude DOUBLE PRECISION,
        artist_location VARCHAR(300),
        artist_name VARCHAR(300),
        title VARCHAR(300),
        duration DOUBLE PRECISION,
        year INTEGER,
        PRIMARY KEY (song_id));
""")


songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS SONGPLAYS(
        songplay_id INTEGER IDENTITY(0,1),
        start_time TIMESTAMP REFERENCES time(start_time),
        user_id INTEGER REFERENCES users(user_id),
        level VARCHAR(300),
        song_id VARCHAR(300) REFERENCES songs(song_id),
        artist_id VARCHAR(300) REFERENCES artists(artist_id),
        session_id INTEGER,
        location VARCHAR(300),
        user_agent VARCHAR(300),
        PRIMARY KEY (songplay_id)
    );
""")


user_table_create = ("""
    CREATE TABLE IF NOT EXISTS USERS(
        user_id INTEGER,
        first_name VARCHAR(300),
        last_name VARCHAR(300),
        gender VARCHAR(1),
        level VARCHAR(300),
        PRIMARY KEY (user_id)
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS SONGS(
        song_id VARCHAR(300),
        title VARCHAR(300),
        artist_id VARCHAR(300),
        year INTEGER,
        duration DOUBLE PRECISION,
        PRIMARY KEY (song_id)
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS ARTISTS(
        artist_id VARCHAR(300),
        name VARCHAR(300),
        location VARCHAR(300),
        lattitude DOUBLE PRECISION,
        longitude DOUBLE PRECISION,
        PRIMARY KEY (artist_id)
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS TIME(
        start_time TIMESTAMP,
        hour INTEGER,
        day INTEGER,
        week INTEGER,
        month INTEGER,
        year INTEGER,
        weekday INTEGER,
        PRIMARY KEY (start_time)
    );
""")



# STAGING TABLES

staging_events_copy = ("""
    COPY STAGING_EVENTS FROM {}
    IAM_ROLE '{}'
    REGION 'us-west-2'
    JSON {}
""").format(config.get('S3','LOG_DATA'),
                        config.get('IAM_ROLE', 'ARN'),
                        config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""
    COPY STAGING_SONGS FROM {}
    IAM_ROLE '{}'
    REGION 'us-west-2'    
    JSON 'auto'
""").format(config.get('S3','SONG_DATA'),
                        config.get('IAM_ROLE', 'ARN'))


# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO SONGPLAYS (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
        SELECT DISTINCT 
            TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' as start_time, 
            SE.user_id, 
            SE.user_level,
            SS.song_id,
            SS.artist_id,
            SE.session_id,
            SE.location,
            SE.user_agent
        FROM staging_events SE, staging_songs SS
        WHERE SE.song_title = SS.title AND SE.page = 'NextSong'
        AND user_id NOT IN (
            SELECT DISTINCT SP.user_id FROM SONGPLAYS SP WHERE SP.user_id = user_id
            AND SP.start_time = start_time AND SP.session_id = session_id )
""")

user_table_insert = ("""
    INSERT INTO USERS (user_id, first_name, last_name, gender, level)
        SELECT DISTINCT
            user_id,
            user_first_name,
            user_last_name,
            user_gender,
            user_level
        FROM staging_events
        WHERE page = 'NextSong'
        AND user_id NOT IN (SELECT DISTINCT user_id FROM USERS)
""")

song_table_insert = ("""
    INSERT INTO SONGS (song_id, title, artist_id, year, duration)
        SELECT DISTINCT
            song_id,
            title,
            artist_id,
            year,
            duration,
        FROM staging_songs
        WHERE song_id NOT IN (select DISTINCT song_id FROM SONGS)
""")

artist_table_insert = ("""
    INSERT INTO ARTTISTS (artist_id, name, location, latitude, longitude)
        SELECT DISTINCT
            artist_id,
            artist_name,
            artist_location,
            artist_latitude,
            artist_longitude
        FROM staging_songs
        WHERE artist_id NOT IN (SELECT DISTINCT artist_id FROM ARTTISTS)

""")

time_table_insert = ("""
    INSERT INTO TIME (start_time, hour, day, week, month, year, weekday)
        SELECT DISTINCT
            start_time,
            EXTRACT (hr from start_time) AS hour,
            EXTRACT (d from start_time) AS day,
            EXTRACT (w from start_time) AS week,
            EXTRACT (mon from start_time) AS month,
            EXTRACT (yr from start_time) AS year,
            EXTRACT (weekday from start_time) AS weekday,
        FROM (
            SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' as start_time
            FROM STAGING_EVENTS
        )
        WHERE start_time NOT IN (SELECT DISTINCT start_time FROM TIME)
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
