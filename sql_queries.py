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
artist_table_drop = "DROP TABLE IF EXISTS atrists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_events (
    artist VARCHAR NULL,
    auth VARCHAR NULL,
    first_name VARCHAR NULL,
    gender VARCHAR NULL,
    item_in_session INT NULL,
    last_name VARCHAR NULL,
    length FLOAT NULL,
    level VARCHAR NULL,
    location VARCHAR NULL,
    method VARCHAR NULL,
    page VARCHAR NULL,
    registration BIGINT NULL,
    session_id INT NULL,
    song VARCHAR NULL,
    status INT NULL,
    ts BIGINT NULL,
    user_agent VARCHAR NULL,
    user_id VARCHAR NULL
); 
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs INT NULL,
    artist_id VARCHAR NULL,
    artist_latitude FLOAT NULL,
    artist_longitude FLOAT NULL,
    artist_location VARCHAR NULL,
    artist_name VARCHAR NULL,
    song_id VARCHAR NULL,
    title VARCHAR NULL,
    duration FLOAT NULL,
    year INT NULL
); 
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays(
    songplay_id BIGINT IDENTITY(0, 1) NOT NULL PRIMARY KEY,
    start_time TIMESTAMP NOT NULL,
    user_id VARCHAR NOT NULL,
    level VARCHAR NOT NULL, 
    song_id VARCHAR NULL,
    artist_id VARCHAR,
    session_id VARCHAR NOT NULL, 
    location VARCHAR NULL, 
    user_agent VARCHAR NULL
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id VARCHAR NOT NULL PRIMARY KEY, 
    first_name VARCHAR NOT NULL, 
    last_name VARCHAR NOT NULL, 
    gender VARCHAR NULL, 
    level VARCHAR NOT NULL
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR NOT NULL PRIMARY KEY, 
    title VARCHAR NOT NULL, 
    artist_id VARCHAR NOT NULL, 
    year INT NULL, 
    duration FLOAT NOT NULL
);
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
    artist_id varchar NOT NULL PRIMARY KEY, 
    name varchar NOT NULL, 
    location VARCHAR NULL, 
    latitude FLOAT NULL, 
    longitude FLOAT NULL
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time timestamp NOT NULL PRIMARY KEY, 
    hour INT NOT NULL, 
    day INT NOT NULL, 
    week INT NOT NULL, 
    month INT NOT NULL, 
    year INT NOT NULL, 
    weekday INT NOT NULL
);
""")

# STAGING TABLES

staging_events_copy = ("""
COPY public.staging_events
FROM {}
CREDENTIALS 'aws_iam_role={}'
FORMAT AS JSON {}
REGION 'us-west-2';
""").format(
    config.get('S3', 'LOG_DATA'),
    config.get('IAM_ROLE', 'ARN'),
    config.get('S3', 'LOG_JSONPATH')
)

staging_songs_copy = ("""
COPY public.staging_songs
FROM {}
CREDENTIALS 'aws_iam_role={}'
FORMAT AS JSON 'auto ignorecase'
REGION 'us-west-2';
""").format(
    config.get('S3', 'SONG_DATA'),
    config.get('IAM_ROLE', 'ARN')
)

# FINAL TABLES
songplay_table_insert = ("""
    INSERT INTO public.songplays (
    start_time,
    user_id,
    level, 
    song_id,
    artist_id,
    session_id, 
    location, 
    user_agent
    )
    SELECT DISTINCT
        t.start_time,
        se.user_id,
        se.level, 
        s.song_id,
        a.artist_id,
        se.session_id, 
        se.location, 
        se.user_agent
    FROM
    public.staging_events se
    JOIN public.songs s ON (se.song = s.title)
    JOIN public.artists a ON (se.artist = a.name)
    JOIN public.time t on (t.start_time = timestamp 'epoch' + se.ts * interval '1 second')
WHERE
    se.page = 'NextSong'
""")

user_table_insert = ("""
    INSERT INTO public.users (
        user_id, 
        first_name, 
        last_name, 
        gender, 
        level
    ) 
    WITH CTE AS (
        select user_id, max(ts) as [max_ts]
            from public.staging_events
            where page = 'NextSong'
            GROUP BY user_id
    )
    SELECT DISTINCT
        se.user_id, 
        se.first_name, 
        se.last_name, 
        se.gender, 
        se.level
    FROM 
        public.staging_events se
    JOIN    
        CTE ON CTE.user_id = se.user_id and CTE.max_ts = se.ts
    WHERE
        page = 'NextSong'
    
""")

song_table_insert = ("""
    INSERT INTO public.songs (
        song_id, 
        title, 
        artist_id, 
        year, 
        duration
    )
    SELECT DISTINCT
        song_id,
        title,
        artist_id,
        year,
        duration
    FROM 
        public.staging_songs
""")

artist_table_insert = ("""
    INSERT INTO public.artists (
        artist_id, 
        name, 
        location, 
        latitude, 
        longitude
    )
    SELECT DISTINCT
        artist_id, 
        artist_name, 
        artist_location, 
        artist_latitude, 
        artist_longitude
    FROM public.staging_songs
""")

time_table_insert = ("""
    INSERT INTO public.time (
        start_time, 
        hour, 
        day, 
        week, 
        month, 
        year, 
        weekday
    )
     WITH CTE AS (
        SELECT DISTINCT
            timestamp 'epoch' + ts * interval '1 second' AS start_time
        FROM 
            public.staging_events
        WHERE
            page = 'NextSong' 
    )
    SELECT
        CTE.start_time, 
        EXTRACT(hour FROM CTE.start_time) AS hour, 
        EXTRACT(day FROM CTE.start_time) AS day, 
        EXTRACT(week FROM CTE.start_time) AS week, 
        EXTRACT(month FROM CTE.start_time) AS month, 
        EXTRACT(year FROM CTE.start_time) AS year, 
        EXTRACT(weekday FROM CTE.start_time) AS weekday
    FROM 
        CTE
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create,
                        user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop,
                      user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert,
                        songplay_table_insert]

