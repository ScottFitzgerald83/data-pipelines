BEGIN TRANSACTION;

CREATE TABLE IF NOT EXISTS events_stage
(
    artist          VARCHAR ENCODE ZSTD,
    auth            VARCHAR ENCODE ZSTD,
    first_name      VARCHAR ENCODE ZSTD,
    gender          VARCHAR ENCODE ZSTD,
    item_in_session INTEGER ENCODE ZSTD,
    last_name       VARCHAR ENCODE ZSTD,
    length          DOUBLE PRECISION ENCODE ZSTD,
    level           VARCHAR ENCODE ZSTD,
    location        VARCHAR ENCODE ZSTD,
    method          VARCHAR ENCODE ZSTD,
    page            VARCHAR ENCODE ZSTD,
    registration    DOUBLE PRECISION ENCODE ZSTD,
    session_id      INTEGER ENCODE ZSTD,
    song            VARCHAR ENCODE ZSTD,
    status          VARCHAR ENCODE ZSTD,
    ts              BIGINT ENCODE ZSTD,
    user_agent      VARCHAR ENCODE ZSTD,
    user_id         INTEGER ENCODE ZSTD
);

CREATE TABLE IF NOT EXISTS songs_stage
(
    artist_id        VARCHAR ENCODE ZSTD,
    artist_latitude  DOUBLE PRECISION ENCODE ZSTD,
    artist_location  VARCHAR ENCODE ZSTD,
    artist_longitude DOUBLE PRECISION ENCODE ZSTD,
    artist_name      VARCHAR ENCODE ZSTD,
    duration         DOUBLE PRECISION ENCODE ZSTD,
    num_songs        INTEGER ENCODE ZSTD,
    song_id          VARCHAR ENCODE ZSTD,
    title            VARCHAR ENCODE ZSTD,
    year             INTEGER ENCODE ZSTD
);

CREATE TABLE IF NOT EXISTS songs
(
    song_id   VARCHAR NOT NULL ENCODE ZSTD,
    title     VARCHAR ENCODE ZSTD,
    artist_id VARCHAR NOT NULL ENCODE ZSTD,
    year      INTEGER ENCODE ZSTD,
    duration  DOUBLE PRECISION ENCODE ZSTD,
    PRIMARY KEY (song_id)
)
    SORTKEY (title);

CREATE TABLE IF NOT EXISTS artists
(
    artist_id VARCHAR NOT NULL ENCODE ZSTD,
    name      VARCHAR NOT NULL ENCODE ZSTD,
    location  VARCHAR ENCODE ZSTD,
    latitude  DOUBLE PRECISION ENCODE ZSTD,
    longitude DOUBLE PRECISION ENCODE ZSTD,
    PRIMARY KEY (artist_id)
);

CREATE TABLE IF NOT EXISTS users
(
    user_id    INTEGER NOT NULL ENCODE ZSTD,
    first_name VARCHAR NOT NULL ENCODE ZSTD,
    last_name  VARCHAR NOT NULL ENCODE ZSTD,
    gender     VARCHAR ENCODE ZSTD,
    level      VARCHAR ENCODE ZSTD,
    PRIMARY KEY (user_id)
)
    DISTSTYLE ALL;

CREATE TABLE IF NOT EXISTS time
(
    start_time TIMESTAMP NOT NULL ENCODE DELTA32K,
    hour       INTEGER ENCODE ZSTD,
    day        INTEGER ENCODE ZSTD,
    week       INTEGER ENCODE ZSTD,
    month      INTEGER ENCODE ZSTD,
    year       INTEGER ENCODE ZSTD,
    weekday    INTEGER ENCODE ZSTD,
    PRIMARY KEY (start_time)
);

CREATE TABLE IF NOT EXISTS songplays
(
    songplay_id INTEGER IDENTITY (0,1) ENCODE ZSTD,
    start_time  TIMESTAMP ENCODE DELTA32K,
    user_id     INTEGER ENCODE ZSTD,
    level       VARCHAR ENCODE ZSTD,
    song_id     VARCHAR ENCODE ZSTD,
    artist_id   VARCHAR ENCODE ZSTD,
    session_id  INTEGER ENCODE ZSTD,
    location    VARCHAR ENCODE ZSTD,
    user_agent  VARCHAR ENCODE ZSTD,
    PRIMARY KEY (songplay_id)
);

END TRANSACTION;