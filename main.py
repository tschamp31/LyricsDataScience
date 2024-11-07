import os
from typing import Sequence
from ytmusicapi import YTMusic
import lyricsgenius

from sqlalchemy.dialects.mssql import pyodbc
from sqlalchemy.sql import text
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, Boolean, ForeignKey, Uuid
from sqlalchemy.orm import sessionmaker

# Database connection string
DATABASE_URL = os.environ.get("MSSQL_STRING")

# Create an engine
engine = create_engine(DATABASE_URL,connect_args={"trusted": True})

# Create a metadata instance
metadata = MetaData(schema='dbo')

# Define all tables
videos = Table('videos', metadata,
               Column('video_id', String(50), primary_key=True),
               Column('title', String(255)),
               Column('like_status', String(20)),
               Column('in_library', Boolean),
               Column('is_available', Boolean),
               Column('is_explicit', Boolean),
               Column('video_type', String(50)),
               Column('views', String(20)),
               Column('duration_seconds', Integer)
               )

artists = Table('artists', metadata,
                Column('artist_id', String(50), primary_key=True),
                Column('name', String(255))
                )

albums = Table('albums', metadata,
               Column('album_id', String(50), primary_key=True),
               Column('name', String(255))
               )

thumbnails = Table('thumbnails', metadata,
                   Column('thumbnail_id', Integer, primary_key=True, autoincrement=True),
                   Column('url', String(255)),
                   Column('width', Integer),
                   Column('height', Integer),
                   Column('video_id', String(50), ForeignKey('videos.video_id'))
                   )

video_artists = Table('video_artists', metadata,
                      Column('video_id', String(50), ForeignKey('videos.video_id'),
                             primary_key=True),
                      Column('artist_id', String(50), ForeignKey('artists.artist_id'),
                             primary_key=True)
                      )

video_albums = Table('video_albums', metadata,
                     Column('video_id', String(50), ForeignKey('videos.video_id'),
                            primary_key=True),
                     Column('album_id', String(50), ForeignKey('albums.album_id'), primary_key=True)
                     )

feedback_tokens = Table('feedback_tokens', metadata,
                        Column('video_id', String(50), ForeignKey('videos.video_id'),
                               primary_key=True),
                        Column('add_token', String(255)),
                        Column('remove_token', String(255))
                        )

videos_lyrics = Table('videos_lyrics', metadata,
                        Column('video_id', String(50), ForeignKey('videos.video_id'),
                               primary_key=True),
                              Column('lyrics_id',  Uuid, ForeignKey('lyrics.lyrics_id'),
                                primary_key=True),
                        )

lyrics = Table('lyrics', metadata,
                              Column('lyrics_id', Uuid, primary_key=True),
                                Column('lyrics', String(255))
                        )

# Create a session
Session = sessionmaker(bind=engine)
session = Session()

# Commit the transaction
session.commit()

# Close the session
session.close()

def upsert(table, primary_key, data, exclude_primary_key=True):
    placeholders = ', '.join(f':{column} AS {column}' for column in data)
    data_dict_no_key = data.copy()
    if exclude_primary_key:
        data_dict_no_key.clear()
        for column in data:
            if column != primary_key:
                data_dict_no_key[column] = data[column]
    columns = ', '.join(data_dict_no_key.keys())
    not_matched_clause = ', '.join(f'source.{column}' for column in data_dict_no_key)
    set_clause = ', '.join(f'{column} = source.{column}' for column in data_dict_no_key)
    values = tuple(data.values())

    upsert_query = f"""
        MERGE LivingRoomSQL.dbo.{table} AS target
        USING (SELECT {placeholders}) AS source
        ON (target.{primary_key} = source.{primary_key})
        WHEN MATCHED THEN 
            UPDATE SET {set_clause}
        WHEN NOT MATCHED THEN
            INSERT ({columns})
            VALUES ({not_matched_clause})
        OUTPUT $action, INSERTED.{primary_key};
    """

    #print(upsert_query)
    results = session.execute(text(upsert_query), data).fetchall()  # Use values twice for SELECT and INSERT
    #print(results)
    session.commit()
    return results

genius = lyricsgenius.Genius(os.environ.get("LYRIC_GENIUS_TOKEN"))
#yt = YTMusic('oauth.json')
#list_of_songs = yt.get_library_songs(10,order='recently_added')

for artists_db_entry in session.execute(artists.select()).fetchall():
    #print(artists_db_entry)
    for video_artists_entry in session.execute(video_artists.select().where(video_artists.c.artist_id==artists_db_entry.artist_id)).fetchall():
        #print(video_artists_entry)
        for video_entry in session.execute(videos.select().where(videos.c.video_id==video_artists_entry.video_id)).fetchall():
            #print(video_entry)
            try:
                #lyrics = yt.get_lyrics(entry['videoId'])
                check_if_lyrics = session.execute(videos_lyrics.select().where(videos_lyrics.c.video_id == video_entry.video_id)).fetchone()
                print(check_if_lyrics)
                if check_if_lyrics is None:
                    print("Fetching Lyrics")
                    song = genius.search_song(video_entry.title,artists_db_entry.name)
                    #print(song.lyrics)
                    lyrics_dict = {}
                    lyrics_dict['lyrics'] = song.to_text()
                    lyrics_dict['lyrics_id'] = None
                    lyrics_upsert_results = upsert('lyrics', 'lyrics_id', lyrics_dict)
                    print(lyrics_upsert_results[0].lyrics_id)
                    video_lyrics_dict = {}
                    video_lyrics_dict['lyrics_id'] = lyrics_upsert_results[0].lyrics_id
                    video_lyrics_dict['video_id'] = video_entry.video_id
                    video_lyrics_results = upsert('videos_lyrics', 'lyrics_id', video_lyrics_dict, False)
                    print(video_lyrics_results[0].lyrics_id)
                else:
                    print('Lyrics Already Exist')
            except Exception as e:
                print(e)



# import pyodbc
#
# # Define the connection string
# conn_str = (
#     r'DRIVER={ODBC Driver 17 for SQL Server};'
#     r'SERVER=YourServerName;'
#     r'DATABASE=LivingRoomSQL;'
#     r'UID=YourUsername;'
#     r'PWD=YourPassword'
# )
#
# # Establish the connection
# conn = pyodbc.connect(conn_str)
# cursor = conn.cursor()
#
# # Example data to be upserted
# data = {
#     'SmsMessages': {
#         'Id': 'unique-identifier-1',
#         'PhoneNumber': '1234567890',
#         'Message': 'Hello World',
#         'SentAt': '2023-10-01 12:00:00'
#     },
#     'albums': {
#         'album_id': 'unique-identifier-2',
#         'title': 'Sample Album',
#         'artist_id': 'artist-identifier-1'
#     },
#     'artists': {
#         'artist_id': 'unique-identifier-3',
#         'name': 'Sample Artist'
#     },
#     'feedback_tokens': {
#         'token_id': 'unique-identifier-4',
#         'token': 'TokenValue'
#     },
#     'lyrics': {
#         'lyrics_id': 'unique-identifier-5',
#         'lyrics': 'Sample Lyrics'
#     },
#     'thumbnails': {
#         'thumbnail_id': 'unique-identifier-6',
#         'image': 'ImageData'
#     },
#     'video_albums': {
#         'video_album_id': 'unique-identifier-7',
#         'title': 'Sample Video Album'
#     },
#     'video_artists': {
#         'video_artist_id': 'unique-identifier-8',
#         'name': 'Sample Video Artist'
#     },
#     'videos': {
#         'video_id': 'unique-identifier-9',
#         'title': 'Sample Video',
#         'like_status': 'Liked',
#         'in_library': True,
#         'is_available': True,
#         'is_explicit': False,
#         'video_type': 'Music',
#         'views': '1000',
#         'duration': '00:03:30',
#         'duration_seconds': 210,
#         'duration_time': '00:03:30'
#     },
#     'videos_lyrics': {
#         'video_lyrics_id': 'unique-identifier-10',
#         'video_id': 'unique-identifier-9',
#         'lyrics_id': 'unique-identifier-5'
#     }
# }
#
# # Define a common UPSERT function
# def upsert(table, primary_key, data):
#     columns = ', '.join(data.keys())
#     placeholders = ', '.join('?' for _ in data)
#     set_clause = ', '.join(f'{column} = source.{column}' for column in data)
#     values = tuple(data.values())
#
#     upsert_query = f"""
#     MERGE LivingRoomSQL.dbo.{table} AS target
#     USING (SELECT {placeholders}) AS source ({columns})
#     ON (target.{primary_key} = source.{primary_key})
#     WHEN MATCHED THEN
#         UPDATE SET {set_clause}
#     WHEN NOT MATCHED THEN
#         INSERT ({columns})
#         VALUES ({placeholders});
#     """
#     cursor.execute(upsert_query, values + values)  # Use values twice for SELECT and INSERT
#     conn.commit()
#
# # Perform UPSERT for each table with example data
# tables_primary_keys = {
#     'SmsMessages': 'Id',
#     'albums': 'album_id',
#     'artists': 'artist_id',
#     'feedback_tokens': 'token_id',
#     'lyrics': 'lyrics_id',
#     'thumbnails': 'thumbnail_id',
#     'video_albums': 'video_album_id',
#     'video_artists': 'video_artist_id',
#     'videos': 'video_id',
#     'videos_lyrics': 'video_lyrics_id'
# }
#
# for table, primary_key in tables_primary_keys.items():
#     if table in data:
#         upsert(table, primary_key, data[table])
#
# # Close the connection
# cursor.close()
# conn.close()
# # Insert into each table
# for song in list_of_songs:
#     print(song)
#
#     q = videos.select().where(videos.c.video_id==song['videoId'])
#     song_exists = session.execute(q).first()
#     session.commit()
#     if song_exists is None:
#         song_entry_insert = videos.insert().values(video_id=song['videoId'],title=song['title'], like_status=song['likeStatus'],
#                                in_library=song['inLibrary'], is_available=song['isAvailable'],
#                                is_explicit=song['isExplicit'], video_type=song['videoType'],
#                                views=song['views'], duration=song['duration'],
#                                duration_seconds=song['duration_seconds'])
#         song_entry = session.execute(song_entry_insert)
#         session.commit()
#
#     q = artists.select().where(artists.c.artist_id==song['artists'][0]['id'])
#     artist_exists = session.execute(q).first()
#     session.commit()
#     if artist_exists is None and song['artists'][0]['id'] is not None:
#         artist_entry_insert = artists.insert().values(artist_id=song['artists'][0]['id'], name=song['artists'][0]['name'])
#         artist_entry = session.execute(artist_entry_insert)
#         session.commit()
#
#     if song['album'] is not None:
#         q = albums.select().where(albums.c.album_id==song['album']['id'], albums.c.name==song['album']['name'])
#         album_exists = session.execute(q).first()
#         session.commit()
#
#         if album_exists is None:
#             album_entry_insert = albums.insert().values(album_id=song['album']['id'], name=song['album']['name'])
#             album_entry = session.execute(album_entry_insert)
#             session.commit()
#
#     q = thumbnails.select().where(thumbnails.c.url==song['thumbnails'][0]['url'])
#     thumbnail_exists = session.execute(q).first()
#     session.commit()
#     if thumbnail_exists is None:
#         thumbnail_entry_insert = thumbnails.insert().values(url=song['thumbnails'][0]['url'],
#                                                     width=song['thumbnails'][0]['width'],
#                                                     height=song['thumbnails'][0]['height'],
#                                                     video_id=song['videoId'])
#         thumbnail_entry = session.execute(thumbnail_entry_insert)
#         session.commit()
#
#     q = video_artists.select().where(video_artists.c.video_id==song['videoId'])
#     video_artists_exists = session.execute(q).first()
#     session.commit()
#     if video_artists_exists is None and song['artists'][0]['id'] is not None:
#         video_artists_entry_insert = video_artists.insert().values(video_id=song['videoId'],
#                                                       artist_id=song['artists'][0]['id'])
#         video_artists_entry = session.execute(video_artists_entry_insert)
#         session.commit()
#
#     if song['album'] is not None:
#         q = video_albums.select().where(video_albums.c.video_id==song['videoId'], video_albums.c.album_id==song['album']['id'])
#         video_albums_exists = session.execute(q).first()
#         session.commit()
#         if video_albums_exists is None:
#             video_albums_entry_insert = video_albums.insert().values(video_id=song['videoId'], album_id=song['album']['id'])
#             video_albums_entry = session.execute(video_albums_entry_insert)
#             session.commit()
#
#     q = feedback_tokens.select().where(feedback_tokens.c.video_id==song['videoId'])
#     feedback_tokens_exists = session.execute(q).first()
#     session.commit()
#     if feedback_tokens_exists is None:
#         feedback_tokens_entry_insert = feedback_tokens.insert().values(video_id=song['videoId'],
#                                                         add_token=song['feedbackTokens']['add'],
#                                                         remove_token=song['feedbackTokens']['remove'])
#         feedback_tokens_entry = session.execute(feedback_tokens_entry_insert)
#         session.commit()