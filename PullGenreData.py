import pylast
import pandas as pd
import numpy as np
import datetime
import json
import pymongo

API_KEY = "a66e2f168fdbcda137799a2c165678ee"
API_SECRET = "0472282104fce606c4d59bd659a66397"

# In order to perform a write operation you need to authenticate yourself
username = 'philosiphicus'
password_hash = 'f22ec97b78a7ebf61bf26c6a0cedf014'

# year = 2019

network = pylast.LastFMNetwork(api_key=API_KEY, api_secret=API_SECRET,
                               username=username, password_hash=password_hash)
user = network.get_user(username)
user_registered_time = user.get_unixtime_registered()

client = pymongo.MongoClient("mongodb+srv://musicdb:musicdb@cluster0-cld3q.mongodb.net/test?retryWrites=true&w=majority")
db = client.musicdb

to_csv = pd.DataFrame(columns=['artist', 'album', 'track', 'listen_date'])


artists = db.artists
tracks = db.tracks

# start_date = user_registered_time
# start_date = datetime.date(2019, 7, 1).strftime('%s')
start_date = tracks.find().sort([('listen_date', -1)]).limit(1)[0]['listen_date'].strftime('%s')
end_date = datetime.datetime.now().strftime('%s')
# end_date = datetime.date(2019, 7, 26).strftime('%s')
total_tracks = []
recent_tracks = []
# recent_tracks = user.get_recent_tracks(limit=1000, time_from=start_date, time_to=end_date)
# total_tracks.extend(recent_tracks)
while (True):
    recent_tracks = user.get_recent_tracks(
        limit=1000, time_from=start_date, time_to=(recent_tracks[-1].timestamp if recent_tracks else end_date))
    if (not recent_tracks):
        break
    total_tracks.extend(recent_tracks)

    # print(recent_tracks[len(recent_tracks)-1])
    print(datetime.datetime.utcfromtimestamp(int(recent_tracks[-1].timestamp)).strftime('%D %H:%M:%S'))
print(len(total_tracks))



artist_list = artists.distinct('name')
track_list = list(tracks.aggregate( [ {"$group": { "_id": { 'track':"$track", 'listen_date': "$listen_date" } } } ] ));

for index, t in enumerate(total_tracks):
    artist_name = t.track.artist.name
    track_playback_date = datetime.datetime.strptime(t.playback_date, '%d %b %Y, %H:%M')

    if artist_name not in artist_list:
        artist_list.append(artist_name)
        genre_list = t.track.artist.get_top_tags(limit=10)
        genres = [g.item.get_name() for g in genre_list]
        artists.insert_one({
            'name': artist_name,
            'genres': genres
        })
        print(f'Added {artist_name} to database.')
    else:
        print(f'{artist_name} already present in the database.')

    try:
        track_index = track_list.index({'_id': {'track': t.track.title, 'listen_date': track_playback_date}})
    except ValueError:
        track_index = False;

    if track_index is False:
        track_list.append({ "_id": { 'track': t.track.title, 'listen_date': track_playback_date} })
        print(f'Adding {t.track.title}, listened to on {t.playback_date}.')
        tracks.insert_one({
            'artist': artist_name,
            'album': t.album,
            'track': t.track.title,
            'listen_date': track_playback_date
        })
    else:
        print(f'Artist {artist_name} play at {t.playback_date} already recorded in database.')