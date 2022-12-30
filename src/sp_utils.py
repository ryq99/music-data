import os
import json
import time
from tqdm import tqdm
import numpy as np
import pandas as pd
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials


def get_tracks(track_uris, sp, batch_size=10, sleep=3, market=None):
    """
    Get tracks data from Spotify.
    Args:
        track_uris (list): a list of track uris (uuid of Spotify)
        sp (spotipy.Spotify): Spotify connector
        batch_size (int): the number of tracks queried at one time (quota related)
        sleep (int): sleep time between two queries in seconds (quota related)
        market (str): country code, default as None (documentation: https://spotipy.readthedocs.io/en/2.22.0/#spotipy.client.Spotify.recommendations)
    Returns:
        tracks (pd.DataFrame)
    """
    tracks = pd.DataFrame()

    for b in tqdm(range(len(track_uris) // batch_size), desc='In Progress'):
        batch_uris = track_uris[b*batch_size:min((b+1)*batch_size, len(track_uris))]
        result = sp.tracks(batch_uris, market)['tracks']
        audio_features = sp.audio_features(batch_uris)

        for t, af in zip(result, audio_features):
            track = pd.DataFrame([[
                # track attributes
                t['uri'],
                t['name'],
                t['popularity'],
                t['duration_ms'],
                len(t['available_markets']), # could be binary features
                t['external_urls']['spotify'],
                ', '.join([d['name'] for d in t['artists']]),
                # track audio features
                af['danceability'],
                af['energy'],
                af['loudness'],
                af['speechiness'],
                af['acousticness'],
                af['instrumentalness'],
                af['liveness'],
                af['valence'],
                af['time_signature'],
                af['tempo'],
                # album attributes
                t['album']['uri'], 
                t['album']['name'],
                t['album']['album_type'],
                t['album']['images'][0]['url'],
                t['album']['total_tracks'],
                t['album']['release_date'],
                t['album']['external_urls']['spotify'],
                ]], columns=[
                    'track_uri', 'track_nm', 'track_pop', 'track_runtime', 'track_num_countries', 'track_url', 'artist_nm', 
                    'track_danceability', 'track_energy', 'track_loudness', 'track_speechiness', 'track_acousticness', 'track_instrumentalness', 'track_liveness', 'track_valence', 'track_tsig', 'track_tempo', 
                    'album_uri', 'album_nm', 'album_type', 'album_img', 'album_num_tracks', 'album_release_dt', 'album_url',
                    ])
            tracks = pd.concat([tracks, track], axis=0)
        
        time.sleep(sleep)

    return tracks

def get_album_copyrights(album_uris, sp, batch_size=10, sleep=3, market=None):
    """
    Get albums copyrights from Spotify.
    Args:
        album_uris (list): a list of album uris (uuid of Spotify)
        sp (spotipy.Spotify): Spotify connector
        batch_size (int): the number of albums queried at one time (quota related)
        sleep (int): sleep time between two queries in seconds (quota related)
        market (str): country code, default as None (documentation: https://spotipy.readthedocs.io/en/2.22.0/#spotipy.client.Spotify.recommendations)
    Returns:
        cprs (pd.DataFrame)
    """
    cprs = pd.DataFrame()

    for b in tqdm(range(len(album_uris) // batch_size), desc='In Progress'):
        batch_uris = album_uris[b*batch_size:min((b+1)*batch_size, len(album_uris))]
        result = sp.albums(batch_uris, market)['albums']

        for a in result:
            cpr = pd.DataFrame([[
                a['uri'], 
                a['copyrights'][0]['text'] if a['copyrights'] else '',
                ', '.join([c['type'] for c in a['copyrights']]) if a['copyrights'] else '',
                a['label'] if a['label'] else '',
                ]], columns=['album_uri', 'album_cpr_txt', 'album_cpr_type', 'album_label'])
            cprs = pd.concat([cprs, cpr], axis=0)

        time.sleep(sleep)

    return cprs

    