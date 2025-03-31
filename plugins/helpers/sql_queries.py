class SqlQueries:
    songplay_table_insert = ("""
    INSERT INTO songplays (play_id, start_time, user_id, "level", song_id, artist_id,
        session_id, location, user_agent)
        SELECT DISTINCT
            md5(events.sessionid || events.start_time) play_id,
            events.start_time,
            events.userid user_id,
            events.level,
            songs.song_id,
            songs.artist_id,
            events.sessionid session_id,
            events.location,
            events.useragent user_agent
            FROM (SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
        FROM staging_events
        WHERE page='NextSong') events
        LEFT JOIN staging_songs songs
        ON events.song = songs.title
            AND events.artist = songs.artist_name
            AND events.length = songs.duration;
    """)

    user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, "level")
        SELECT distinct userid user_id, firstname first_name, lastname last_name, gender, "level"
        FROM staging_events
        WHERE page='NextSong'
        ORDER BY user_id;
    """)

    song_table_insert = ("""
    INSERT INTO public.songs (song_id, title, artist_id, year, duration)
    SELECT distinct SS.song_id, title, SS.artist_id, year, duration
    FROM public.staging_songs SS
    JOIN public.songplays SP ON SP.song_id = SS.song_id
    ORDER BY SS.song_id;
    """)

    artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT distinct SS.artist_id, artist_name name, artist_location AS location,
        artist_latitude AS latitude, artist_longitude AS longitude
        FROM public.staging_songs SS
        JOIN public.songplays SP ON SP.artist_id = SS.artist_id
        ORDER BY SS.artist_id;
    """)

    time_table_insert = ("""
    INSERT INTO public."time" (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT start_time, extract(hour from start_time) AS hour, extract(day from start_time) AS day,
        extract(week from start_time) AS week, extract(month from start_time) AS month,
        extract(year from start_time) AS year, extract(weekday from start_time) AS weekday
        FROM songplays
        ORDER BY start_time;
    """)
    # STAGING TABLES

    STAGING_TABLE_COPY = ("""
    COPY public.{} FROM '{}'
    ACCESS_KEY_ID '{}'
    SECRET_ACCESS_KEY '{}'
    FORMAT AS JSON '{}';
    """)

    COUNT_TABLE_ROWS = ("""
    SELECT COUNT({}) FROM public.{};
    """)

    DROP_TABLE_SQL = ("DROP TABLE IF EXISTS public.{};")

    # DROP_ALL_TABLES = ("""
    #      DROP TABLE IF EXISTS public.staging_events;
    #      DROP TABLE IF EXISTS public.staging_songs;
    #      DROP TABLE IF EXISTS public.songplays;
    #      DROP TABLE IF EXISTS public.users;
    #      DROP TABLE IF EXISTS public.songs;
    #      DROP TABLE IF EXISTS public.artists;
    #      DROP TABLE IF EXISTS public.time;
    #                    """)

    # DROP_STAGING_EVENTS_TABLE = ("DROP table IF EXISTS public.staging_events;")
    # DROP_STAGING_SONGS_TABLE = ("DROP table IF EXISTS public.staging_songs;")
    # DROP_SONGPLAYS_FACT_TABLE = ("DROP table IF EXISTS public.songplays;")
    # DROP_USERS_DIMENSION_TABLE = ("DROP table IF EXISTS public.users;")
    # DROP_SONG_DIMENSION_TABLE = ("DROP table IF EXISTS public.songs;")
    # DROP_ARTIST_DIMENSION_TABLE = ("DROP table IF EXISTS public.artists;")
    # DROP_TIME_DIMENSION_TABLE = ("DROP table IF EXISTS public.time;")

    CREATE_ARTIST_DIMENSION_TABLE = ("""
    CREATE TABLE public.artists (
        artist_id varchar(256) NOT NULL,
        name varchar(512),
        location varchar(512),
        latitude numeric(18,0),
        longitude numeric(18,0)
        );
    """)

    CREATE_SONGPLAYS_FACT_TABLE = ("""
    CREATE TABLE public.songplays (
	play_id varchar(32) NOT NULL,
	start_time timestamp NOT NULL,
	user_id int4 NOT NULL,
	"level" varchar(256),
	song_id varchar(256),
	artist_id varchar(256),
	session_id int4,
	location varchar(256),
	user_agent varchar(256),
	CONSTRAINT songplays_pkey PRIMARY KEY (play_id));""")

    CREATE_SONG_DIMENSION_TABLE = ("""
    CREATE TABLE public.songs (
	song_id varchar(256) NOT NULL,
	title varchar(512),
	artist_id varchar(256),
	"year" int4,
	duration numeric(18,0),
	CONSTRAINT songs_pkey PRIMARY KEY (song_id)
    );
    """)

    CREATE_TIME_DIMENSION_TABLE = ("""
    CREATE TABLE public."time" (
	start_time timestamp NOT NULL,
	"hour" int4,
	"day" int4,
	week int4,
	"month" varchar(256),
	"year" int4,
	weekday varchar(256),
	CONSTRAINT time_pkey PRIMARY KEY (start_time)
    );
    """)

    CREATE_USERS_DIMENSION_TABLE = ("""
    CREATE TABLE IF NOT EXISTS public.users (
        user_id int4 NOT NULL,
        first_name varchar(256),
        last_name varchar(256),
        gender varchar(256),
        "level" varchar(256),
        CONSTRAINT users_pkey PRIMARY KEY (user_id)
    );
    """)

    CREATE_STAGING_EVENTS_TABLE = ("""
    CREATE TABLE public.staging_events (
        artist varchar(256),
        auth varchar(256),
        firstname varchar(256),
        gender varchar(256),
        iteminsession int4,
        lastname varchar(256),
        length numeric(18,0),
        "level" varchar(256),
        location varchar(256),
        "method" varchar(256),
        page varchar(256),
        registration numeric(18,0),
        sessionid int4,
        song varchar(256),
        status int4,
        ts int8,
        useragent varchar(256),
        userid int4
    );""")

    CREATE_STAGING_SONGS_TABLE = ("""
    CREATE TABLE public.staging_songs (
        num_songs int4,
        artist_id varchar(256),
        artist_name varchar(512),
        artist_latitude numeric(18,0),
        artist_longitude numeric(18,0),
        artist_location varchar(512),
        song_id varchar(256),
        title varchar(512),
        duration numeric(18,0),
        "year" int4
    );""")



    CREATE_TABLE_QUERIES = ({
        "staging_events": CREATE_STAGING_EVENTS_TABLE,
        "staging_songs": CREATE_STAGING_SONGS_TABLE,
        "songplays": CREATE_SONGPLAYS_FACT_TABLE,
        "users": CREATE_USERS_DIMENSION_TABLE,
        "songs": CREATE_SONG_DIMENSION_TABLE,
        "artists": CREATE_ARTIST_DIMENSION_TABLE,
        "time": CREATE_TIME_DIMENSION_TABLE
    })

    LOAD_TABLE_QUERIES = ({
        "songplays": songplay_table_insert,
        "users": user_table_insert,
        "songs": song_table_insert,
        "artists": artist_table_insert,
        "time": time_table_insert
    })
