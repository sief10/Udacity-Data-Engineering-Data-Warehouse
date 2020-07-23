import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN             = config.get('IAM_ROLE', 'ARN')
# DROP TABLES

staging_events_table_drop = "DROP Table if exists staging_events"
staging_songs_table_drop = "DROP Table if exists staging_songs"
songplay_table_drop = "DROP Table if exists SONGPLAY_TBL"
user_table_drop = "DROP TABLE IF EXISTS USER_TBL"
song_table_drop = "DROP TABLE IF EXISTS SONG_TBL"
artist_table_drop = "DROP TABLE IF EXISTS ARTIST_TBL"
time_table_drop = "DROP TABLE IF EXISTS TIME_TBL"
stg_table_drop ="DROP TABLE IF EXISTS SONG_STG"

# CREATE TABLES

staging_events_table_create= ("""
 CREATE TABLE staging_events (
  event_id    BIGINT IDENTITY(0,1)    NOT NULL,
                artist      VARCHAR                 NULL,
                auth        VARCHAR                 NULL,
                firstName   VARCHAR                 NULL,
                gender      VARCHAR                 NULL,
                itemInSession VARCHAR               NULL,
                lastName    VARCHAR                 NULL,
                length      VARCHAR                 NULL,
                level       VARCHAR                 NULL,
                location    VARCHAR                 NULL,
                method      VARCHAR                 NULL,
                page        VARCHAR                 NULL,
                registration VARCHAR                NULL,
                sessionId   INTEGER                 NULL SORTKEY DISTKEY,
                song        VARCHAR                 NULL,
                status      INTEGER                 NULL,
                ts          BIGINT                  NULL,
                userAgent   VARCHAR                 NULL,
                userId      INTEGER                 NULL
    );
""")

staging_songs_table_create = ("""
 CREATE TABLE IF NOT EXISTS staging_songs (
                num_songs           INTEGER         NULL,
                artist_id           VARCHAR         NOT NULL SORTKEY DISTKEY,
                artist_latitude     VARCHAR         NULL,
                artist_longitude    VARCHAR         NULL,
                artist_location     VARCHAR(500)   NULL,
                artist_name         VARCHAR(500)   NULL,
                song_id             VARCHAR         NOT NULL,
                title               VARCHAR(500)   NULL,
                duration            DECIMAL(9)      NULL,
                year                INTEGER         NULL
    )
""")

songplay_table_create = ("""
CREATE TABLE SONGPLAY_TBL(
SONGPLAY_ID BIGINT IDENTITY(0,1) PRIMARY KEY ,
START_TIME TIMESTAMP NOT NULL ,
USER_ID int NOT NULL,
SONG_ID VARCHAR  NOT NULL,
ARTIST_ID VARCHAR NOT NULL,
SESSION_ID INTEGER,
LOCATION VARCHAR(500),
USER_AGENT VARCHAR,
LEVEL  VARCHAR(10)
)
""")

user_table_create = ("""
CREATE TABLE USER_TBL (
USER_ID int PRIMARY KEY, 
FNAME VARCHAR, 
LNAME VARCHAR, 
GENDER char(1), 
LEVEL VARCHAR(10)
)
""")

song_table_create = ("""
CREATE TABLE SONG_TBL
 (
 SONG_ID Varchar PRIMARY KEY, 
 TITLE VARCHAR(500), 
 ARTIST_ID VARCHAR, 
 YEAR int, 
 DURATION float
 )
""")

artist_table_create = ("""
CREATE TABLE ARTIST_TBL(
ARTIST_ID VARCHAR PRIMARY KEY, 
NAME VARCHAR, 
LOCATION VARCHAR(500), 
LATITUDE VARCHAR, 
LONGITUDE VARCHAR
)
""")

time_table_create = ("""
CREATE TABLE TIME_TBL(
START_TIME TIMESTAMP PRIMARY KEY,
YEAR  int,
MONTH int,
WEEK  int,
DAY   int,
HOUR  int,
WEEKDAY VARCHAR(15)
)
""")

# STAGING TABLES

staging_events_copy = ("""
 copy staging_events from 's3://udacity-dend/log_data'
    credentials 'aws_iam_role={}'
    format as json 's3://udacity-dend/log_json_path.json'
    ACCEPTINVCHARS AS '^'
    STATUPDATE ON
    region 'us-west-2';
""").format(ARN)

staging_songs_copy = ("""
  copy staging_songs from 's3://udacity-dend/song_data'
    credentials 'aws_iam_role={}'
    format as json 'auto'
    ACCEPTINVCHARS AS '^'
    STATUPDATE ON
    region 'us-west-2';
""").format(ARN)

# FINAL TABLES

song_table_insert = ("""INSERT INTO SONG_TBL (SONG_ID,TITLE,ARTIST_ID,YEAR,DURATION) 
SELECT distinct song_id , title , artist_id , year , duration from staging_songs 
"""  )

artist_table_insert = ("""INSERT INTO ARTIST_TBL (ARTIST_ID, NAME, LOCATION, LATITUDE, LONGITUDE)
SELECT artist_id,artist_name,artist_location,artist_latitude,artist_longitude FROM staging_songs
""")

time_table_insert = ("""INSERT INTO TIME_TBL(START_TIME ,YEAR,MONTH,WEEK,DAY,HOUR,WEEKDAY)
 SELECT  DISTINCT TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second'        AS start_time,
            EXTRACT(hour FROM start_time)    AS hour,
            EXTRACT(day FROM start_time)     AS day,
            EXTRACT(week FROM start_time)    AS week,
            EXTRACT(month FROM start_time)   AS month,
            EXTRACT(year FROM start_time)    AS year,
            EXTRACT(week FROM start_time)    AS weekday
    FROM    staging_events             
    WHERE page = 'NextSong';
""")

user_table_insert=(""" INSERT INTO USER_TBL (USER_ID, FNAME, LNAME, GENDER, LEVEL) SELECT distinct userId,firstName,lastName,gender,level  FROM staging_events WHERE page='NextSong' """)

stg_table_insert= (""" INSERT INTO SONG_STG (SESSION_ID, USER_AGENT, SONG_TITLE,ARTIST_NAME, SONG_DURATION,USER_ID,LEVEL,START_TIME) values (%s,%s,%s,%s,%s,%s,%s,%s) """)


songplay_table_insert = ("""INSERT INTO SONGPLAY_TBL(START_TIME,USER_ID,ARTIST_ID,SONG_ID,SESSION_ID,LOCATION,USER_AGENT,LEVEL) 
SELECT  
TIMESTAMP 'epoch' + e.ts/1000 * INTERVAL '1 second' AS start_time ,e.userId,s.song_id,s.artist_id,e.sessionId,s.artist_location,e.userAgent,e.level
FROM staging_songs S INNER JOIN staging_events e ON e.artist=s.artist_name AND e.song=s.title
Where e.page='NextSong';
""")



# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop,stg_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries=[song_table_insert,artist_table_insert,user_table_insert,time_table_insert,songplay_table_insert]



