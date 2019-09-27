class TestHelpers:
    failed_summary_box = """
*******************************************************************************************
**************************      FAILED TEST SUMMARY      **********************************                     
*******************************************************************************************
"""

    quality_checks_box = """
*******************************************************************************************
********************      DATA QUALITY CHECKS SUMMARY      ********************************                     
*******************************************************************************************
"""

    end_block = """
*******************************************************************************************
*******************************************************************************************
"""
    table_names = ['events_stage', 'songs_stage', 'songplays', 'users', 'songs', 'time']
    events_stage_cols = [
        'artist',
        'auth',
        'first_name',
        'gender',
        'item_in_session',
        'last_name',
        'length',
        'level',
        'location',
        'method',
        'page',
        'registration',
        'session_id',
        'song',
        'status',
        'ts',
        'user_agent',
        'user_id']
    songs_stage_cols = [
        'num_songs',
        'artist_id',
        'artist_name',
        'artist_latitude',
        'artist_longitude',
        'artist_location',
        'song_id',
        'title',
        'duration',
        'year'
    ]
    songplays_cols = [
        'play_id',
        'start_time',
        'user_id',
        'level',
        'song_id',
        'artist_id',
        'session_id',
        'location',
        'user_agent'
    ]
    users_cols = [
        'user_id',
        'first_name',
        'last_name',
        'gender',
        'level'
    ]
    songs_cols = [
        'song_id',
        'title',
        'artist_id',
        'year',
        'duration'
    ]
    artists_cols = [
        'artist_id',
        'name',
        'location',
        'latitude',
        'longitude'
    ]
    time_cols = [
        'start_time',
        'hour',
        'day',
        'week',
        'month',
        'year',
        'weekday']
    tests_to_run = {'test_row_counts': table_names,
                    'test_null_values': {'events_stage': events_stage_cols,
                                         'songs_stage': songs_stage_cols,
                                         'songplays': songplays_cols,
                                         'users': users_cols,
                                         'songs': songs_cols,
                                         'artists': artists_cols,
                                         'time': time_cols
                                         }
                    }
