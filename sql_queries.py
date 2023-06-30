import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
DWH_ROLE_ARN = config.get("IAM_ROLE","ARN")
LOG_DATA = config.get("S3","LOG_DATA")
LOG_JSONPATH = config.get("S3","LOG_JSONPATH")
SONG_DATA = config.get("S3","SONG_DATA")

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
CREATE TABLE staging_events (
    artist_name   VARCHAR,
    auth          VARCHAR,
    first_name    VARCHAR,
    gender        CHAR(1),
    itemInSession SMALLINT,
    last_name     VARCHAR,
    length        DECIMAL(9,5),
    level         VARCHAR,
    location      VARCHAR,
    method        VARCHAR,
    page          VARCHAR,
    registration  BIGINT,
    session_id    INT,
    song_title    VARCHAR,
    status        SMALLINT,
    timestamp     BIGINT,
    user_agent    VARCHAR,
    user_id       INT
)
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs (
    artist_id        VARCHAR,
    artist_latitude  DECIMAL(10,8),
    artist_longitude DECIMAL(11,8),
    artist_location  VARCHAR,
    artist_name      VARCHAR,
    song_id          VARCHAR,
    title            VARCHAR,
    duration         DECIMAL(9,5),
    year             SMALLINT
)
""")

songplay_table_create = ("""
CREATE TABLE songplays (
    songplay_id  INT IDENTITY(0, 1),
    start_time   TIMESTAMP sortkey NOT NULL,
    user_id      INT,
    level        VARCHAR,
    song_id      VARCHAR distkey NOT NULL,
    artist_id    VARCHAR,
    session_id   INT,
    location     VARCHAR,
    user_agent   VARCHAR
)
""")

user_table_create = ("""
CREATE TABLE users (
    user_id INT  NOT NULL sortkey,
    first_name   VARCHAR,
    last_name    VARCHAR,
    gender       CHAR(1),
    level        VARCHAR
) diststyle all;
""")

song_table_create = ("""
CREATE TABLE songs (
    song_id    VARCHAR NOT NULL sortkey distkey,
    title      VARCHAR NOT NULL,
    artist_id  VARCHAR,
    year       SMALLINT,
    duration   DECIMAL(9,5)
)
""")

artist_table_create = ("""
CREATE TABLE artists (
    artist_id  VARCHAR NOT NULL sortkey, 
    name       VARCHAR NOT NULL, 
    location   VARCHAR, 
    latitude   DECIMAL(10,8), 
    longitude  DECIMAL(11,8)
) diststyle all;
""")

time_table_create = ("""
CREATE TABLE time (
    start_time  TIMESTAMP NOT NULL sortkey, 
    hour        SMALLINT, 
    day         SMALLINT, 
    week        SMALLINT, 
    month       SMALLINT, 
    year        SMALLINT, 
    weekday     VARCHAR
) diststyle all;
""")

# STAGING TABLES
"""Inserts data into staging_events table from s3 json log files"""
staging_events_copy = ("""
COPY staging_events
FROM {}
iam_role '{}'
json {}
""").format(LOG_DATA, DWH_ROLE_ARN, LOG_JSONPATH)

"""Inserts data into staging_songs table from s3 json song files"""
staging_songs_copy = ("""
COPY staging_songs
FROM {}
iam_role '{}'
json 'auto ignorecase';
""").format(SONG_DATA, DWH_ROLE_ARN)

# FINAL TABLES
"""Inserts data into songplays table from staging_events & staging_songs tables"""
songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT TIMESTAMP 'epoch' + e.timestamp/1000 * INTERVAL '1 second',
           e.user_id, e.level, s.song_id, s.artist_id, e.session_id, e.location, e.user_agent
      FROM staging_events e
      JOIN staging_songs s    
        ON e.artist_name = s.artist_name
       AND e.song_title = s.title
     WHERE e.page = 'NextSong'
""")

"""Inserts data into users table from staging_events table"""
user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT 
           user_id, first_name, last_name, gender,
           FIRST_VALUE(level) OVER (PARTITION BY user_id, first_name, last_name, gender 
                                        ORDER BY first_name ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
      FROM staging_events
     WHERE user_id IS NOT NULL
""")

"""Inserts data into songs table from staging_songs table"""
song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT song_id, title, artist_id, year, duration 
      FROM staging_songs
""")

"""Inserts data into artists table from staging_songs table"""
artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT artist_id,
        first_value(artist_name)      OVER (PARTITION BY artist_id
                                                ORDER BY artist_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) f_artist_name,
        first_value(artist_location)  OVER (PARTITION BY artist_id 
                                                ORDER BY artist_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) f_artist_location,
        first_value(artist_latitude)  OVER (PARTITION BY artist_id 
                                                ORDER BY artist_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) f_artist_latitude,
        first_value(artist_longitude) OVER (PARTITION BY artist_id 
                                                ORDER BY artist_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) f_artist_longitude
      FROM staging_songs
""")

"""Inserts data into time table from songplays table"""
time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT 
        start_time,
        extract(hour    FROM start_time),
        extract(day     FROM start_time),
        extract(week    FROM start_time),
        extract(month   FROM start_time),
        extract(year    FROM start_time),
        extract(weekday FROM start_time)
      FROM songplays
""")

# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]