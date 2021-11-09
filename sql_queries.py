import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events
(
        artist        VARCHAR,
        auth          VARCHAR,
        firstName     VARCHAR,
        gender        VARCHAR,
        itemInSession INTEGER,
        lastName      VARCHAR,
        length        FLOAT, 
        level         VARCHAR,
        location      VARCHAR,
        method        VARCHAR,
        page          VARCHAR,
        registration  FLOAT, 
        sessionId     INTEGER,
        song          VARCHAR,
        status        INTEGER,
        ts            TIMESTAMP,
        userAgent     VARCHAR,
        userId        INTEGER
    );
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays 
(
        song_id           VARCHAR PRIMARY KEY,
        artist_id         VARCHAR,
        artist_latitude   FLOAT, 
        artist_location   VARCHAR, 
        artist_longitude  FLOAT, 
        artist_name       VARCHAR,
        duration          FLOAT, 
        num_songs         INTEGER,
        title             VARCHAR,
        year              INTEGER
);

""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay 
(
            songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY sortkey, 
            start_time  TIMESTAMP, 
            user_id     INTEGER, 
            level       VARCHAR,
            song_id     VARCHAR, 
            artist_id   VARCHAR, 
            session_id  INTEGER, 
            location    VARCHAR, 
            user_agent  VARCHAR
);

""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users 
(
            user_id    INTEGER PRIMARY KEY distkey, 
            first_name VARCHAR, 
            last_name  VARCHAR, 
            gender     VARCHAR, 
            level      VARCHAR
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS song 
(
            song_id   VARCHAR PRIMARY KEY sortkey, 
            title     VARCHAR, 
            artist_id VARCHAR, 
            year      INTEGER, 
            duration  FLOAT
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artist 
(
            artist_id VARCHAR PRIMARY KEY distkey, 
            name      VARCHAR, 
            location  VARCHAR, 
            latitude  FLOAT, 
            longitude FLOAT
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
            start_time TIMESTAMP PRIMARY KEY sortkey distkey, 
            hour       INTEGER, 
            day        INTEGER, 
            week       INTEGER, 
            month      INTEGER, 
            year       INTEGER, 
            weekday    INTEGER
);
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events 
FROM {}
CREDENTIALS 'aws_iam_role={}'
COMPUPDATE OFF region 'us-west-2'
FORMAT AS json {}
""").format(config.get('S3',       'LOG_DATA'),
           config.get('IAM_ROLE', 'ARN'),
           config.get('S3',       'LOG_JSONPATH'))

staging_songs_copy = ("""
COPY staging_songs 
FROM {}
CREDENTIALS 'aws_iam_role={}'
COMPUPDATE OFF region 'us-west-2'
FORMAT AS json 'auto'
""").format(config.get('S3',       'SONG_DATA'), 
           config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT TO_TIMESTAMP(se.ts,'YYYY-MM-DD HH24:MI:SS') AS start_time
                se.userId    AS user_id, 
                se.level     AS level, 
                ss.song_id   AS song_id, 
                ss.artist_id AS artist_id, 
                se.sessionId AS session_id, 
                se.location  AS location, 
                se.userAgent AS user_agent 
FROM staging_songs ss
JOIN staging_events se 
ON (ss.title = se.song AND ss.artist_name = se.artist)
AND se.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO user (user_id, first_name, last_name, gender, level)
SELECT DINSTICT userId    AS user_id,
                firstName AS first_name, 
                lastName  AS last_name, 
                gender    AS gender, 
                level     AS level
FROM staging_events
WHERE userId IS NOT NULL
AND page = 'NextSong';
""")

song_table_insert = ("""
INSERT INTO song (song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id AS song_id,
                title AS title, 
                artist_id AS artist_id, 
                year AS year, 
                duration AS duration
FROM  staging_songs
WHERE song_id IS NOT NULL;   
""")

artist_table_insert = ("""
INSERT INTO dim_artist (artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id AS artist_id, 
                artist_name AS name, 
                artist_location AS location,  
                artist_latitude AS latitude, 
                artist_longitude AS longitude
FROM  staging_songs
WHERE artist_id IS NOT NULL; 
""")

time_table_insert = ("""
INSERT INTO dim_time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT TO_TIMESTAMP(ts,'YYYY-MM-DD HH24:MI:SS') AS start_time,
                EXTRACT(HOUR    FROM ts) AS hour,
                EXTRACT(DAY     FROM ts) AS day, 
                EXTRACT(WEEK    FROM ts) AS week, 
                EXTRACT(MONTH   FROM ts) AS month, 
                EXTRACT(YEAR    FROM ts) AS year,
                EXTRACT(WEEKDAY FROM ts) AS  weekday
FROM staging_events 
WHERE ts IS NOT NULL;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
