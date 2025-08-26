import csv
import sqlite3
import logging
import spotipy
import requests
from bs4 import BeautifulSoup
from spotipy.oauth2 import SpotifyOAuth
from datetime import datetime, timedelta
import time
import argparse
import sys
import random
import smtplib
import os
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from urllib.parse import quote
from tqdm import tqdm

SPOTIPY_001_CLIENT_ID = os.getenv('SPOTIPY_001_CLIENT_ID')
SPOTIPY_001_CLIENT_SECRET  = os.getenv('SPOTIPY_001_CLIENT_SECRET')
SPOTIPY_001_REDIRECT_URI = 'https://musicbirthday.com/callback'
SPOTIPY_001_SCOPE = 'user-top-read playlist-modify-public user-read-private'

MAX_PLAYLIST_LENGTH = 50
SPOTIFY_GET_LIMIT = 50
MOST_RECENT_YEAR = 2022
SLEEP_TIMER = 0.4

LAST_FM_API_KEY = os.getenv('LAST_FM_API_KEY')
LAST_FM_API_SECRET = os.getenv('LAST_FM_API_SECRET')
SKIP_KEYWORDS = {}
SKIP_YEARS_DUE_TO_BEING_TOO_RECENT = {'2025','2024','2023','2022','2021',}

SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
SMTP_USERNAME = os.getenv('MB_SMTP_UN')
SMTP_PASSWORD = os.getenv('MB_SMTP_PW')

def configure_logging():
    logging.getLogger().setLevel(logging.INFO)
    file_handler = logging.FileHandler(f"musicbirthday.log")
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(funcName)s - %(message)s'))
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(funcName)s - %(message)s'))
    logging.getLogger().addHandler(file_handler)
    logging.getLogger().addHandler(console_handler)

def spotify_oauth_1():
	return SpotifyOAuth(SPOTIPY_001_CLIENT_ID,SPOTIPY_001_CLIENT_SECRET,SPOTIPY_001_REDIRECT_URI,SPOTIPY_001_SCOPE)

def spotify_helper_object():
    return spotipy.Spotify(auth_manager=SpotifyOAuth(SPOTIPY_001_CLIENT_ID,SPOTIPY_001_CLIENT_SECRET,'http://localhost:8000/callback',scope='',cache_path='.cache3'),requests_timeout=5)

def generate_album_row(album):
    row_to_add = [''] * 71
    row_to_add[0], row_to_add[1], row_to_add[2] = album['id'], album['album_type'], album['total_tracks']
    row_to_add_curr_index = 3
    for image_url in album.get('images',[]):
        row_to_add[row_to_add_curr_index] = image_url['url']
        row_to_add_curr_index += 1
        if row_to_add_curr_index >= 7:
            logging.critical("TOO MANY IMAGE_URLS, GOTTA CHANGE 'albums.csv'!")
            exit()
    row_to_add[7], row_to_add[8], row_to_add[9] = album['name'], album['release_date'], album['release_date_precision']
    row_to_add_curr_index = 10
    for artist in album['artists']:
        row_to_add[row_to_add_curr_index] = artist['id']
        row_to_add_curr_index += 1
        if row_to_add_curr_index >= 70:
            logging.critical("TOO MANY ARTISTS, GOTTA CHANGE 'albums.csv'!")
            exit()
    row_to_add[70] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    return row_to_add, album['id']

def generate_artist_rows(artist):
    rows_to_add = []
    row_to_add = [''] * 34
    row_to_add[0], row_to_add[1]  = artist['id'], str(artist['followers'].get('total',0))
    row_to_add_curr_index = 2
    for genre in artist.get('genres',[]):
        row_to_add[row_to_add_curr_index] = genre
        row_to_add_curr_index += 1
        if row_to_add_curr_index >= 22:
            logging.critical("TOO MANY GENRES, NEED TO CHANGE 'artists.csv'!")
            exit()
    row_to_add_curr_index = 22
    for image_url in artist.get('images',[]):
        row_to_add[row_to_add_curr_index] = image_url['url']
        row_to_add_curr_index += 1
        if row_to_add_curr_index >= 27:
            logging.critical("TOO MANY IMAGE_URLS, GOTTA CHANGE 'artists.csv'!")
            exit()
    row_to_add[27], row_to_add[28], row_to_add[33]  = artist['name'], str(artist.get('popularity',0)), datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    for rta in handle_birthday_deathday(artist):
        row_to_add_temp = row_to_add[:]
        row_to_add_temp[29], row_to_add_temp[30], row_to_add_temp[31], row_to_add_temp[32] = rta[0], rta[1], rta[2], rta[3]
        rows_to_add.append(row_to_add_temp)
    logging.info(rows_to_add)
    return rows_to_add

def handle_date(date_string):
    if len(date_string)==0:
        return None
    for string_format in ('%m/%d/%Y','%Y-%m-%d',):
        try:
            return datetime.strptime(date_string,string_format)
        except Exception as e:
            pass
    return None

def cached_musicbirthday_values():
    month_to_match, day_to_match = datetime.today().month, datetime.today().day
    to_return = {'birthday':{},'deathday':{},'release_date':{}}
    artists_already_written, albums_already_written = set(), set()
    logging.info(f"Loading cached values")
    with open('artists.csv','r',encoding='utf-8') as infile:
        reader = csv.reader(infile,delimiter=',')
        next(reader)
        for artist_row in reader:
            artist_id, band_member, birthday, deathday = artist_row[0], artist_row[-5], handle_date(artist_row[-4]), handle_date(artist_row[-3])
            artists_already_written.add(artist_id)
            if birthday is not None and month_to_match == birthday.month and day_to_match == birthday.day:
                to_return['birthday'][artist_id] = {'band_member':band_member,'date':birthday}
            if deathday is not None and month_to_match == deathday.month and day_to_match == deathday.day:
                to_return['deathday'][artist_id] = {'band_member':band_member,'date':deathday}
    with open('albums.csv','r',encoding='utf-8') as infile:
        reader = csv.reader(infile,delimiter=',')
        next(reader)
        for album_row in reader:
            album_id, release_type, release_date, artist_ids = album_row[0], album_row[1], handle_date(album_row[8]), set(album_row[10:69])
            artist_ids.discard("")
            albums_already_written.add(album_id)
            if release_date is not None and release_date.year <= MOST_RECENT_YEAR and month_to_match == release_date.month and day_to_match == release_date.day and release_type.upper() != 'COMPILATION':
                to_return['release_date'][album_id] = {'type':release_type,'date':release_date,'artist_ids':artist_ids}
    logging.info(f"Loaded data | {len(to_return['birthday'])} BIRTHDAYS | {len(to_return['deathday'])} DEATHDAYS | {len(to_return['release_date'])} ALBUMS")
    return to_return, artists_already_written, albums_already_written  #artists_already_written, albums_already_written

def musicbrainz_request(query_string):
    num_retries = 3
    while True:
        try:
            mb_object = requests.get(f"https://musicbrainz.org/ws/2/artist/{query_string}",timeout=4)
            if mb_object.status_code == 503:
                retry_after = mb_object.headers.get('Retry-After')
                if retry_after:
                    logging.warning(f"MUSICBRAINZ rate limit reached | Sleeping {retry_after}")
                    time.sleep(int(retry_after))
            else:
                to_return = mb_object.json()
                logging.info(to_return)
                return mb_object.json()
        except Exception as e:
            logging.warning(f"{query_string} | {e} | {num_retries} retries left")
            num_retries -= 1
            if num_retries < 0:
                return None

def mb_suffix():
    return """?inc=aliases+annotation+area-rels+artist-rels+label-rels+recording-rels+release-rels+release-group-rels+series-rels+url-rels+work-rels+genres+ratings+tags+artist-credits+recordings+releases+release-groups+works&fmt=json"""

def musicbrainz_artist_object(artist_name):
    try:
        tO_return = musicbrainz_request(musicbrainz_request(f"?query=artist:{quote(artist_name)}&fmt=json")['artists'][0]['id']+mb_suffix())
        logging.info(f"{to_return}")
        return to_return
    except Exception as e:
        logging.error(f"{artist_name} | {e}")
        return None

def soup(url,headers=None):
    retry_count = 2
    while True:
        try:
            response = requests.get(url,headers=headers,timeout=3)
            if response.status_code==404:
                return None
            return BeautifulSoup(response.content,'html.parser')
        except Exception as e:
            retry_count -= 1
            logging.warning(f"{query_string} | {e} | {retry_count} retries left")
            if retry_count == 0:
                return None

def get_wikipedia_deathday(soup):
    try:
        return soup.find('th', string="Died").find_next_sibling('td').find('span').get_text(strip=True).replace('(','').replace(')','')
    except Exception as e:
        logging.warning(f"{e} | ARTIST ISN'T DEAD")
        return ''

def get_artist_real_name(soup_object,artist_name):
    try:
        to_return = soup_object.find('th', string="Birth name").find_next_sibling('td').get_text(strip=True)
    except:
        try:
            to_return = soup_object.find('th', string="Born").find_next_sibling('td').find('div', {'class':'nickname'}).get_text(strip=True)
        except:
            try:
                to_return = soup_object.find('th', string="Also known as").find_next_sibling('td').get_text(strip=True)
            except Exception as e:
                to_return = artist_name
    logging.info(f"{artist_name}")
    return artist_name

def get_request(url):
    while True:
        try:
            return requests.get(url,timeout=3.5).json()
        except Exception as e:
            logging.error(f"{url} | {e}")
            time.sleep(1.5)

def handle_birthday_deathday(artist):
    artist_name = artist['name']
    for possible_wiki_ending in ('','_(musician)','_(rapper)'):
        soup_object = soup(f"https://en.wikipedia.org/wiki/{artist_name}{possible_wiki_ending}")
        if soup_object is None:
            continue
        wikipedia_birthday = soup_object.find('span',{'class':'bday'})
        if wikipedia_birthday:
            to_return = [[get_artist_real_name(soup_object,artist_name),wikipedia_birthday.get_text(strip=True),get_wikipedia_deathday(soup_object),'wikipedia']]
            logging.info(to_return)
            return to_return
    artist_name = artist_name.replace('&','%26')
    musicbrainz_object = musicbrainz_artist_object(artist_name)
    if musicbrainz_object is None:
        to_return = [['','','','']]
        logging.info(to_return)
        return to_return
    if musicbrainz_object['type'] == 'Person':
        lifespan_object = musicbrainz_object.get('life-span',dict())
        to_return = [[musicbrainz_object.get('name',artist_name),lifespan_object.get('begin',''),lifespan_object.get('end',''),'musicbrainz']]
        logging.info(to_return)
        return to_return
    elif musicbrainz_object['type'] == 'Group':
        to_return = []
        a_names_already_used = set()
        for relation in musicbrainz_object.get('relations', []):
            if relation['type'] == 'member of band':
                member_object = relation.get('artist', {})
                member_name = member_object.get('name',None)
                if member_name in a_names_already_used:
                    continue
                a_names_already_used.add(member_name)
                member_id = member_object.get('id',None)
                if member_name and member_id:
                    member_full_object = get_request(f"https://musicbrainz.org/ws/2/artist/{member_id}/{mb_suffix()}")
                    lifespan_object = member_full_object.get('life-span',dict())
                    if lifespan_object.get('begin',''):
                        to_return.append([member_name,lifespan_object.get('begin',''),lifespan_object.get('end',''),'musicbrainz'])
                        logging.info(to_return)
                    else:
                        try:
                            famous_birthdays_soup = soup(f"https://www.famousbirthdays.com/people/{member_name.lower().replace(' ','-')}.html")
                            if artist['name'] in str(famous_birthdays_soup):
                                birthday_value = datetime.strptime(famous_birthdays_soup.find('span',{'class':"type-16-18"}).find_next_sibling('span').text.strip(),"%B %d, %Y").strftime('%Y-%m-%d')
                                to_return.append([member_name,birthday_value,'','musicbrainz_famousbirthdays'])
                                logging.info(to_return)
                        except Exception as e:
                            logging.error(f"{e}")
        if len(to_return)==0:
            to_return = [['','','','']]
            logging.info(to_return)
            return to_return
        return to_return
    to_return = [['','','','']]
    logging.info(to_return)
    return to_return

def recently_scanned_(days_lookback = 3):
    to_return = set()
    try:
        with open('artists_recently_scanned.csv','r',encoding='utf-8') as infile:
            reader = csv.reader(infile,delimiter=',')
            for row in reader:
                if handle_date(row[1]) + timedelta(days = days_lookback)>datetime.now():
                    to_return.add(row[0])
    except Exception as e:
        logging.error(f"{e}")
        pass
    logging.info(f"FOUND {len(to_return)} RECENTLY SCANNED ARTISTS")
    return to_return

def define_spotify_objects(sp_oauth,refresh_token):
    token_info = sp_oauth.refresh_access_token(refresh_token)
    access_token = token_info['access_token']
    return spotipy.Spotify(auth=access_token), access_token #spotify_helper_object(), access_token

def check_if_track_or_album_is_special(track,album_row,musicbirthday_values,ids_related_to_user_for_today,time_range,MOST_RECENT_YEAR,month_to_match,day_to_match,SKIP_KEYWORDS):
    album_id, release_type, release_date, artist_ids = album_row[0], album_row[1], handle_date(album_row[8]), set(album_row[10:69])
    if release_type.lower() == 'compilation':
        return ids_related_to_user_for_today
    artist_ids.discard("")
    if album_id in musicbirthday_values['release_date']:
        if album_id not in ids_related_to_user_for_today:
            logging.info(f"{album_id} | {time_range} | {release_date} | ALBUM")
            ids_related_to_user_for_today[album_id] = {'type':time_range+'_track_album_release_date','id_type':'album','id':album_id,'year':musicbirthday_values['release_date'][album_id]['date'].year}
        if track is not None and track['id'] not in ids_related_to_user_for_today:
            logging.info(f"{track['id']} | {time_range} | {release_date} | TRACK")
            ids_related_to_user_for_today[track['id']] = {'type':time_range+'_track_album_release_date','id_type':'track','id':track['id'],'year':musicbirthday_values['release_date'][album_id]['date'].year}
    for index, artist_id in enumerate(artist_ids):
        if artist_id in musicbirthday_values['birthday']  and artist_id not in ids_related_to_user_for_today:
            logging.info(f"{artist_id} | BIRTHDAY | {musicbirthday_values['birthday'][artist_id]['date']}")
            ids_related_to_user_for_today[artist_id] = {'type':time_range+'_track_artist_birthday_'+ ('main' if index == 0 else 'support'),'id_type':'artist','id':artist_id,'year':musicbirthday_values['birthday'][artist_id]['date'].year,'band_member':musicbirthday_values['birthday'][artist_id]['band_member']}
        if artist_id in musicbirthday_values['deathday'] and artist_id not in ids_related_to_user_for_today:
            logging.info(f"{artist_id} | DEATHDAY | {musicbirthday_values['deathday'][artist_id]['date']}")
            ids_related_to_user_for_today[artist_id] = {'type':time_range+'_track_artist_deathday_'+ ('main' if index == 0 else 'support'),'id_type':'artist','id':artist_id,'year':musicbirthday_values['deathday'][artist_id]['date'].year,'band_member':musicbirthday_values['deathday'][artist_id]['band_member']}
    if release_date is not None and release_date.year <= MOST_RECENT_YEAR and release_date.month == month_to_match and release_date.day == day_to_match:
        for index_ in range(10,70):
            if str(album_row[index_])=='0LyfQWJT6nXafLPZqxe9Of':
                break
        else:
            for skip_keyword in SKIP_KEYWORDS:
                if skip_keyword in album_row[7]:
                    break
            else:
                musicbirthday_values['release_date'][album_row[0]] = {'type':release_type,'date':release_date,'artist_ids':artist_ids}
                if album_id not in ids_related_to_user_for_today:
                    logging.info(f"{album_id} | {time_range} | {release_date} | ALBUM")
                    ids_related_to_user_for_today[album_id] = {'type':time_range+'_track_album_release_date','id_type':'album','id':album_id,'year':release_date.year}
                if track is not None and track['id'] not in ids_related_to_user_for_today:
                    logging.info(f"{track['id']} | {time_range} | {release_date} | TRACK")
                    ids_related_to_user_for_today[track['id']] = {'type':time_range+'_track_album_release_date','id_type':'track','id':track['id'],'year':release_date.year}
    return ids_related_to_user_for_today

def get_user_top_tracks(SPOTIFY_GET_LIMIT,time_range,TRACK_OFFSET,SLEEP_TIMER,sp):
    while True:
        try:
            user_top_tracks = sp.current_user_top_tracks(limit=SPOTIFY_GET_LIMIT, time_range=time_range+'_term',offset=TRACK_OFFSET)['items']                        
            break
        except Exception as e:
            logging.error(f"{e}")
            time.sleep(SLEEP_TIMER * 5)
    return user_top_tracks

def get_user_top_artists(SPOTIFY_GET_LIMIT,time_range,ARTIST_OFFSET,SLEEP_TIMER,sp):
    while True:
        try:
            user_top_artists = sp.current_user_top_artists(limit=SPOTIFY_GET_LIMIT, time_range=time_range+'_term',offset=ARTIST_OFFSET)['items']                     
            break
        except Exception as e:
            logging.error(f"{e}")
            time.sleep(SLEEP_TIMER * 5)
    return user_top_artists

def get_artist_top_albums(artist,SPOTIFY_GET_LIMIT,ALBUM_OFFSET):
    while True:
        try:
            sub_albums_from_artist = sp.artist_albums(artist['id'], include_groups='album,single,compilation,appears_on',limit=SPOTIFY_GET_LIMIT,offset=ALBUM_OFFSET)['items']                  
            break
        except Exception as e:
            logging.error(f"{e}")
            time.sleep(SLEEP_TIMER * 5)
    return sub_albums_from_artist

def write_album_id_to_csv(album_row,albums_already_written):
    with open('albums.csv','a+',encoding='utf-8',newline='') as outfile:
        csv.writer(outfile,delimiter=',').writerow(album_row)
        logging.info(f"NEW ALBUM ROW {album_row}")
    albums_already_written.add(album_id)
    return albums_already_written

def write_artist_id_to_csv(artist,artists_already_written,month_to_match, day_to_match,ids_related_to_user_for_today):
    for artist_row in generate_artist_rows(artist):
        with open('artists.csv','a+',encoding='utf-8',newline='') as outfile:
            csv.writer(outfile,delimiter=',').writerow(artist_row)
            logging.info(f"NEW ARTIST ROW {artist_row}")
            artists_already_written.add(artist_row[0])
            birthday, deathday = handle_date(artist_row[-4]), handle_date(artist_row[-3])
            if birthday is not None and month_to_match == birthday.month and day_to_match == birthday.day:
                ids_related_to_user_for_today[artist['id']] = {'type':time_range+'_artist_birthday_top','id_type':'artist','id':artist['id'],'year':birthday.year,'band_member':artist_row[-5]}
            if deathday is not None and month_to_match == deathday.month and day_to_match == deathday.day:
                ids_related_to_user_for_today[artist['id']] = {'type':time_range+'_artist_deathday_top','id_type':'artist','id':artist['id'],'year':deathday.year,'band_member':artist_row[-5]}
    return artists_already_written,ids_related_to_user_for_today

def check_if_artist_is_special(artist,musicbirthday_values,ids_related_to_user_for_today,time_range):
    if artist['id'] in musicbirthday_values['birthday'] and artist['id'] not in ids_related_to_user_for_today:
        logging.info(f"{artist['id']} | BIRTHDAY | {musicbirthday_values['birthday'][artist['id']]['date']}")
        ids_related_to_user_for_today[artist['id']] = {'type':time_range+'_artist_birthday_top','id_type':'artist','id':artist['id'],'year':musicbirthday_values['birthday'][artist['id']]['date'].year,'band_member':musicbirthday_values['birthday'][artist['id']]['band_member']}
    if artist['id'] in musicbirthday_values['deathday'] and artist['id'] not in ids_related_to_user_for_today:
        logging.info(f"{artist['id']} | DEATHDAY | {musicbirthday_values['deathday'][artist['id']]['date']}")
        ids_related_to_user_for_today[artist['id']] = {'type':time_range+'_artist_deathday_top','id_type':'artist','id':artist['id'],'year':musicbirthday_values['deathday'][artist['id']]['date'].year,'band_member':musicbirthday_values['deathday`'][artist['id']]['band_member']}
    return ids_related_to_user_for_today

def get_track_playcount(track_playcount,track_artist,track):
    try:
        track_playcount += int(requests.get("http://ws.audioscrobbler.com/2.0/",timeout = 3,
        params = {
        'method': 'track.getInfo',
        'api_key': LAST_FM_API_KEY,
        'artist': track_artist['name'],
        'track': track['name'],
        'format': 'json'}).json()['track']['playcount'])
        logging.info(f"{track_artist['name'] } {track['name']} | {track_playcount}")
    except Exception as e:
        logging.error(f"{e}")
    return track_playcount

def update_tracks_to_consider_with_info(track,values,track_playcount,album,tracks_to_consider):
    tracks_to_consider[track['id']] = {
        'type': values['type'],
        'track_playcount': track_playcount,
        'id_type': values['id_type'],
        'artists':track['artists'],
        'album_label':album.get('label',None),
        'album_popularity':album.get('popularity',0),
        'track_popularity':0,
        'track_duration_ms':track['duration_ms'],
        'score':0,
        'year':values['year'],
        'band_member':values.get('band_member','')
    }
    return tracks_to_consider

def translate_type_to_english(type_match,year,band_member_name):
    if type_match.endswith('_album_release_date') or type_match.endswith('release_date_top_album'):
        return f'was released on this date in {year}'
    elif '_birthday_' in type_match:
        return f'has artist {band_member_name} celebrating a birthday today! They were born in {year}'
    elif '_deathday_' in type_match:
        return f'has artist {band_member_name} who passed away on this date in {year}'

def get_track_scores(tracks_to_consider):
    for track_id,values_ in tqdm(tracks_to_consider.items()):
        track_score = 0
        if int(values_['track_duration_ms']) < 90000 or int(values_['track_duration_ms']) > 600000:
            logging.warning(f"{track_id} | SKIPPING ({round(values_['track_duration_ms']/60000,2)} MINUTES)")
            continue
        if int(values_['year']) in SKIP_YEARS_DUE_TO_BEING_TOO_RECENT:
            logging.warning(f"{track_id} | SKIPPING BECAUSE TOO RECENT {values_[year]}")
            continue
        if values_['id_type'] == 'track':
            track_score += 6
        elif (values_['id_type'] == 'album' and '_artist_' in values_['type']) or values_['id_type'] == 'artist':
            track_score += 3
        else:
            logging.warning(f"{track_id} | SKIPPING BECAUSE JUST ALBUM RELEASE DATE")
            continue
        if values_['type'].startswith('short'):
            track_score += 5
        elif values_['type'].startswith('medium'):
            track_score += 3
        elif values_['type'].startswith('long'):
            track_score += 1
        track_year_score_multipier = 0 
        temp, temp_diff = min(int(year__) for year__ in SKIP_YEARS_DUE_TO_BEING_TOO_RECENT) - int(values_['year']), 5
        while temp > 0:
            temp -= temp_diff
            track_year_score_multipier += 1
        track_score += track_year_score_multipier
        playcount_thresholds = [1000,10000,50000,100000,250000,500000,1000000,2000000,3000000,4000000,5000000,10000000,25000000,50000000,100000000,1000000000,999999999999]
        playcount_thresholds_index = 0
        while int(values_['track_playcount']) > playcount_thresholds[playcount_thresholds_index]:
            playcount_thresholds_index += 1
        track_score += playcount_thresholds_index / 1.2
        track_score += float(max(values_['album_popularity'],values_['track_popularity']))/50
        logging.info(f"{track_id} | {track_score}")
        tracks_to_consider[track_id]['score'] = track_score
    return tracks_to_consider

def get_track_ids_to_add_to_playlist(tracks_to_consider, sp, ARTIST_ID_TRACK_LIMIT = 1, TRACK_SCORE_MIN = 3):
    reasons_in_english = {}
    try:
        track_ids_to_add_to_playlist, artist_id_track_id_count, tracks_to_consider_after_looking_at_artist_name_and_track_name, tracks_to_consider_for_real = [], {}, {}, {}
        tracks_to_consider_keys = list(tracks_to_consider.copy().keys())
        while len(tracks_to_consider_keys)>50:
            for index, possible_track in tqdm(enumerate(sp.tracks(tracks_to_consider_keys[:50],'US')['tracks'])):
                track_name, track_popularity = possible_track['name']+' by '+', '.join(_['name'] for _ in possible_track['artists']), possible_track['popularity']
                logging.info(f"{track_name} | {track_popularity}")
                if track_name in tracks_to_consider_after_looking_at_artist_name_and_track_name:
                    if tracks_to_consider_after_looking_at_artist_name_and_track_name[track_name]['popularity'] < possible_track['popularity']:
                        del tracks_to_consider_after_looking_at_artist_name_and_track_name[track_name]
                        tracks_to_consider_after_looking_at_artist_name_and_track_name[track_name] = {'id':possible_track['id'],'popularity':possible_track['popularity'],'track_data':tracks_to_consider[possible_track['id']]}
                        logging.info(f"UPDATED TRACK {track_name} WITH NEW POPULARITY / ID | {possible_track['id']} ({possible_track['popularity']})")
                    else:
                        logging.info('SKIPPED A TRACK BECAUSE SAME ARTIST AND TRACK NAME')
                else:
                    tracks_to_consider_after_looking_at_artist_name_and_track_name[track_name] = {'id':possible_track['id'],'popularity':possible_track['popularity'],'track_data':tracks_to_consider[possible_track['id']]}
                    logging.info(f"NEW TRACK {track_name} | {possible_track['id']} ({possible_track['popularity']})")
            tracks_to_consider_keys = tracks_to_consider_keys[50:]
        for index, possible_track in tqdm(enumerate(sp.tracks(tracks_to_consider_keys,'US')['tracks'])):
            track_name, track_popularity = possible_track['name']+' by '+', '.join(_['name'] for _ in possible_track['artists']), possible_track['popularity']
            logging.info(f"{track_name} | {track_popularity}")
            if track_name in tracks_to_consider_after_looking_at_artist_name_and_track_name:
                if tracks_to_consider_after_looking_at_artist_name_and_track_name[track_name]['popularity'] < possible_track['popularity']:
                    del tracks_to_consider_after_looking_at_artist_name_and_track_name[track_name]
                    tracks_to_consider_after_looking_at_artist_name_and_track_name[track_name] = {'id':possible_track['id'],'popularity':possible_track['popularity'],'track_data':tracks_to_consider[possible_track['id']]}
                    logging.info(f"UPDATED TRACK {track_name} WITH NEW POPULARITY / ID | {possible_track['id']} ({possible_track['popularity']})")
                else:
                    logging.info('SKIPPED A TRACK BECAUSE SAME ARTIST AND TRACK NAME')
            else:
                tracks_to_consider_after_looking_at_artist_name_and_track_name[track_name] = {'id':possible_track['id'],'popularity':possible_track['popularity'],'track_data':tracks_to_consider[possible_track['id']]}
                logging.info(f"NEW TRACK {track_name} | {possible_track['id']} ({possible_track['popularity']})")
        for full_track_name,track in tracks_to_consider_after_looking_at_artist_name_and_track_name.items():
            tracks_to_consider_for_real[track['id']] = track['track_data']
            tracks_to_consider_for_real[track['id']]['full_track_name'] = full_track_name
        id_count_allowed_left = {
        'birthday':16,
        'deathday':4,
        'release_date':30
        }
        while len(track_ids_to_add_to_playlist) < MAX_PLAYLIST_LENGTH:
            if ARTIST_ID_TRACK_LIMIT >= 50:
                break
            TRACK_SCORE_MAX = 20
            while TRACK_SCORE_MAX > TRACK_SCORE_MIN:
                for track_id, values__ in tracks_to_consider_for_real.items():
                    if track_id in track_ids_to_add_to_playlist:
                        continue
                    lookup_key = None
                    for key in id_count_allowed_left:
                        if key in values__['type']:
                            lookup_key = key
                            break
                    if id_count_allowed_left[lookup_key] == 0:
                        continue
                    if values__['score'] < TRACK_SCORE_MAX:
                        continue
                    for artist_id in values__['artists']:
                        artist_id = artist_id['id']
                        if artist_id in artist_id_track_id_count and artist_id_track_id_count.get(artist_id,0) >= ARTIST_ID_TRACK_LIMIT:
                            break
                        if artist_id not in artist_id_track_id_count:
                            artist_id_track_id_count[artist_id] = 0
                        artist_id_track_id_count[artist_id] += 1
                    else:
                        logging.info(f"ADDING {track_id} | {values__['year']} | {values__['type']} | {values__['track_playcount']} | {values__['id_type']} | {values__['artists']} | {values__['track_duration_ms']} | {values__['score']} | {values__.get('band_member','')}")
                        track_ids_to_add_to_playlist.append(track_id)
                        id_count_allowed_left[lookup_key] -= 1
                        reasons_in_english[track_id] = '<p>' + values__['full_track_name'] + ' ' + translate_type_to_english(values__['type'],values__['year'],values__.get('band_member','')) + '</p>'
                        if len(track_ids_to_add_to_playlist) == MAX_PLAYLIST_LENGTH:
                            break
                    if len(track_ids_to_add_to_playlist) == MAX_PLAYLIST_LENGTH:
                        break
                if len(track_ids_to_add_to_playlist) == MAX_PLAYLIST_LENGTH:
                    break
                TRACK_SCORE_MAX -= 1
            ARTIST_ID_TRACK_LIMIT += 1
        logging.info("Finished adding stuff to playlist, shuffling...")
        random.shuffle(track_ids_to_add_to_playlist)
        random.shuffle(track_ids_to_add_to_playlist)
        logging.info("...shuffled!")
        reasons_in_english_text = """"""
        for track_id in track_ids_to_add_to_playlist:
            reasons_in_english_text += reasons_in_english[track_id]
    except Exception as e:
        logging.critical(f"{e}")
    return track_ids_to_add_to_playlist,reasons_in_english_text

def send_email(user_email,playlist_id,reasons_in_english_text):
    msg = MIMEMultipart()
    msg['From'], msg['To'], msg['Subject'] = "Music Birthday <support@musicbirthday.com>", user_email, f"Your Music Birthday Playlist for {datetime.today().strftime('%B %d')} is here!"
    msg.attach(MIMEText(f"""<html><h1>{datetime.today().strftime('%B %d')}</h1>
    <br><div class="description">See below for a copy of your <strong><a href="https://musicbirthday.com">MusicBirthday.com</a></strong> playlist!</div>
    <p><a href="https://open.spotify.com/playlist/{playlist_id}" style="color: #1DB954;">Open in Spotify</a></p>
    <div class="description">Questions? Issues? Reach out to <strong><a href="mailto:support@musicbirthday.com">support@musicbirthday.com</a></strong></div>
    <div class="description">{reasons_in_english_text}</div></html>""", 'html'))
    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.starttls()
        server.login(SMTP_USERNAME, SMTP_PASSWORD)
        server.send_message(msg)

def get_cached_playcounts():
    to_return = {}
    with open('track_playcounts.tsv','r',encoding='utf-8') as infile:
        for line in infile.read().splitlines():
            to_return[line.split('\t')[0]] = int(line.split('\t')[1])
    return to_return

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-s",type=str, help="Skip Users on First Go (default = Yes",default='y')
    args = parser.parse_args()
    skip_first_loop = (args.s == 'y')
    configure_logging()
    cached_playcounts = get_cached_playcounts()
    artists_scanned_this_run = recently_scanned_()
    user_ids_updated_today = set()
    sp_oauth = spotify_oauth_1()
    target_time = (datetime.now()).replace(hour=0, minute=1, second=0, microsecond=0)
    while True:
        musicbirthday_values, artists_already_written, albums_already_written = cached_musicbirthday_values()
        month_to_match, day_to_match = datetime.today().month, datetime.today().day
        with sqlite3.connect('musicbirthday.db') as conn:
            cursor = conn.cursor()
            cursor.execute("""SELECT count (distinct spotify_id) FROM user""")
            current_user_count = last_user_count = cursor.fetchall()[0][0]
            if datetime.now() >= target_time or current_user_count > len(user_ids_updated_today):
                if current_user_count==last_user_count:
                    target_time = (datetime.now()+timedelta(days=1)).replace(hour=0, minute=1, second=0, microsecond=0)
                    logging.info("Updating all users' playlists")
                    del user_ids_updated_today
                    user_ids_updated_today = set()
                cursor.execute("SELECT spotify_id,refresh_token,email,music_birthday_spotify_playlist_id FROM user")
                for spotify_id, refresh_token, user_email, playlist_id in cursor.fetchall():
                    try:
                        if spotify_id in user_ids_updated_today:
                            logging.warning(f"Skipping '{spotify_id}' ({user_email}) because already parsed")
                            continue
                        user_ids_updated_today.add(spotify_id)
                        if skip_first_loop:
                            logging.warning(f"Skipping '{spotify_id}' ({user_email}) because SKIP FIRST LOOP is enabled")
                            continue
                        logging.info("Refreshing access token")
                        sp, access_token = define_spotify_objects(sp_oauth, refresh_token)
                        user_info = sp.current_user()
                        cursor.execute("UPDATE user SET access_token = ? WHERE refresh_token = ?", (access_token, refresh_token))
                        conn.commit()
                        logging.info(f"Starting playlist generation for '{spotify_id}' ({user_email})")
                        ids_related_to_user_for_today = {}
                        for time_range in ('short','medium','long'):
                            TRACK_OFFSET, last_track_offset = 0, 0
                            while True:
                                time.sleep(SLEEP_TIMER)
                                user_top_tracks = get_user_top_tracks(SPOTIFY_GET_LIMIT,time_range,TRACK_OFFSET,SLEEP_TIMER,sp)
                                for track in user_top_tracks:
                                    TRACK_OFFSET += 1
                                    album_row,album_id = generate_album_row(track['album'])
                                    ids_related_to_user_for_today = check_if_track_or_album_is_special(track,album_row,musicbirthday_values,ids_related_to_user_for_today,time_range,MOST_RECENT_YEAR,month_to_match,day_to_match,SKIP_KEYWORDS)
                                    if album_id not in albums_already_written:
                                        albums_already_written = write_album_id_to_csv(album_row,albums_already_written)
                                if TRACK_OFFSET % SPOTIFY_GET_LIMIT != 0 or last_track_offset == TRACK_OFFSET:
                                    logging.info(f"{time_range} | FOUND {TRACK_OFFSET} TOTAL TRACKS")
                                    break
                                last_track_offset = TRACK_OFFSET
                            ARTIST_OFFSET, last_artist_offset = 0, 0
                            while True:
                                time.sleep(SLEEP_TIMER)
                                user_top_artists = get_user_top_artists(SPOTIFY_GET_LIMIT,time_range,ARTIST_OFFSET,SLEEP_TIMER,sp)
                                for artist in tqdm(user_top_artists):
                                    ARTIST_OFFSET += 1
                                    if artist['id'] not in artists_already_written:
                                        albums_already_written, ids_related_to_user_for_today = write_artist_id_to_csv(artist,artists_already_written,month_to_match, day_to_match,ids_related_to_user_for_today)
                                    else:    
                                        ids_related_to_user_for_today = check_if_artist_is_special(artist,musicbirthday_values,ids_related_to_user_for_today,time_range)
                                    if artist['id'] not in artists_scanned_this_run:
                                        ALBUM_OFFSET, last_album_offset = 0, 0
                                        while True:
                                            time.sleep(SLEEP_TIMER)
                                            sub_albums_from_artist = get_artist_top_albums(artist,SPOTIFY_GET_LIMIT,ALBUM_OFFSET)
                                            for sub_album in tqdm(sub_albums_from_artist):
                                                ALBUM_OFFSET += 1
                                                album_row, album_id = generate_album_row(sub_album)
                                                ids_related_to_user_for_today = check_if_track_or_album_is_special(None,album_row,musicbirthday_values,ids_related_to_user_for_today,time_range,MOST_RECENT_YEAR,month_to_match,day_to_match,SKIP_KEYWORDS)
                                                if album_id not in albums_already_written:
                                                    albums_already_written = write_album_id_to_csv(album_row,albums_already_written)
                                            if ALBUM_OFFSET % SPOTIFY_GET_LIMIT != 0 or last_album_offset == ALBUM_OFFSET:
                                                logging.info(f"{time_range} | {artist['id']} | FOUND {ALBUM_OFFSET} TOTAL ALBUMS")
                                                break
                                            last_album_offset = ALBUM_OFFSET
                                        artists_scanned_this_run.add(artist['id'])
                                        with open('artists_recently_scanned.csv','a+',encoding='utf-8',newline='') as outfile:
                                            writer = csv.writer(outfile,delimiter=',')
                                            writer.writerow([artist['id'],datetime.now().strftime('%m/%d/%Y')])
                                    else:
                                        for album_id in musicbirthday_values['release_date']:
                                            if album_id not in ids_related_to_user_for_today and artist['id'] in musicbirthday_values['release_date'][album_id]['artist_ids']:
                                                logging.info(f"{album_id} | {time_range} | {musicbirthday_values['release_date'][album_id]['date']} | ALBUM (cached)")
                                                ids_related_to_user_for_today[album_id] = {'type':time_range+'_artist_release_date_top_album','id_type':'album','id':album_id,'year':musicbirthday_values['release_date'][album_id]['date'].year}
                                if ARTIST_OFFSET % SPOTIFY_GET_LIMIT != 0 or last_artist_offset == ARTIST_OFFSET:
                                    logging.info(f"{time_range} | FOUND {ARTIST_OFFSET} TOTAL ARTISTS")
                                    break
                                last_artist_offset = ARTIST_OFFSET
                        album_ids_to_get, track_ids_to_get, tracks_to_consider = [], [], {}
                        for spotify_id, values in tqdm(ids_related_to_user_for_today.items()):
                            if values['id_type'] == 'album':
                                album_ids_to_get.append(spotify_id)
                                if len(album_ids_to_get)==20:
                                    for album in sp.albums(album_ids_to_get)['albums']:
                                        for track in album['tracks']['items']:
                                            if track['id'] in tracks_to_consider:
                                                continue
                                            try:
                                                track_playcount = cached_playcounts[track['id']]
                                                logging.info(f"CACHED TRACK COUNT FOUND | {track['name']} | {track_playcount}")
                                            except Exception as e:
                                                track_playcount = 0
                                                for track_artist in track['artists']:
                                                    track_playcount = get_track_playcount(track_playcount,track_artist,track)
                                                    time.sleep(SLEEP_TIMER)
                                                with open('track_playcounts.tsv','a+',encoding='utf-8',newline='') as outfile:
                                                    outfile.write(track['id']+'\t'+str(track_playcount)+'\n')
                                                    outfile.flush()
                                                    cached_playcounts[track['id']] = track_playcount
                                                    logging.info(f"NEW TRACK COUNT FOUND | {track['name']} | {track_playcount}")
                                            tracks_to_consider = update_tracks_to_consider_with_info(track,values,track_playcount,dict(),tracks_to_consider)
                                    del album_ids_to_get
                                    album_ids_to_get = []
                            elif values['id_type'] == 'artist':
                                for track in sp.artist_top_tracks(spotify_id, country='US')['tracks']:
                                    if track['id'] in tracks_to_consider:
                                        continue
                                    track_playcount = 0
                                    try:
                                        track_playcount = cached_playcounts[track['id']]
                                        logging.info(f"CACHED TRACK COUNT FOUND | {track['name']} | {track_playcount}")
                                    except Exception as e:
                                        track_playcount = 0
                                        for track_artist in track['artists']:
                                            track_playcount = get_track_playcount(track_playcount,track_artist,track)
                                            time.sleep(SLEEP_TIMER)
                                        with open('track_playcounts.tsv','a+',encoding='utf-8',newline='') as outfile:
                                            outfile.write(track['id']+'\t'+str(track_playcount)+'\n')
                                            outfile.flush()
                                            cached_playcounts[track['id']] = track_playcount
                                            logging.info(f"NEW TRACK COUNT FOUND | {track['name']} | {track_playcount}")
                                    tracks_to_consider = update_tracks_to_consider_with_info(track,values,track_playcount,dict(),tracks_to_consider)
                            ## values['id_type'] == 'track'
                            else:
                                track_ids_to_get.append(spotify_id)
                                if len(track_ids_to_get)==50:
                                    for track in sp.tracks(track_ids_to_get,'US')['tracks']:
                                        if track['id'] in tracks_to_consider:
                                            continue
                                        try:
                                            track_playcount = cached_playcounts[track['id']]
                                            logging.info(f"CACHED TRACK COUNT FOUND | {track['name']} | {track_playcount}")
                                        except Exception as e:
                                            track_playcount = 0
                                            for track_artist in track['artists']:
                                                track_playcount = get_track_playcount(track_playcount,track_artist,track)
                                                time.sleep(SLEEP_TIMER)
                                            with open('track_playcounts.tsv','a+',encoding='utf-8',newline='') as outfile:
                                                outfile.write(track['id']+'\t'+str(track_playcount)+'\n')
                                                outfile.flush()
                                                cached_playcounts[track['id']] = track_playcount
                                                logging.info(f"NEW TRACK COUNT FOUND | {track['name']} | {track_playcount}")
                                        tracks_to_consider = update_tracks_to_consider_with_info(track,values,track_playcount,dict(),tracks_to_consider)
                        if len(album_ids_to_get)>0:
                            for album in sp.albums(album_ids_to_get)['albums']:
                                for track in album['tracks']['items']:
                                    if track['id'] in tracks_to_consider:
                                        continue
                                    try:
                                        track_playcount = cached_playcounts[track['id']]
                                        logging.info(f"CACHED TRACK COUNT FOUND | {track['name']} | {track_playcount}")
                                    except:
                                        track_playcount = 0
                                        for track_artist in track['artists']:
                                            track_playcount = get_track_playcount(track_playcount,track_artist,track)
                                            time.sleep(SLEEP_TIMER)
                                        with open('track_playcounts.tsv','a+',encoding='utf-8',newline='') as outfile:
                                            outfile.write(track['id']+'\t'+str(track_playcount)+'\n')
                                            outfile.flush()
                                            cached_playcounts[track['id']] = track_playcount
                                            logging.info(f"NEW TRACK COUNT FOUND | {track['name']} | {track_playcount}")
                                    tracks_to_consider = update_tracks_to_consider_with_info(track,values,track_playcount,dict(),tracks_to_consider)
                        if len(track_ids_to_get)>0:
                            for track in sp.tracks(track_ids_to_get,'US')['tracks']:
                                if track['id'] in tracks_to_consider:
                                    continue
                                try:
                                    track_playcount = cached_playcounts[track['id']]
                                except:
                                    track_playcount = 0
                                    for track_artist in track['artists']:
                                        track_playcount = get_track_playcount(track_playcount,track_artist,track)
                                        time.sleep(SLEEP_TIMER)
                                    with open('track_playcounts.tsv','a+',encoding='utf-8',newline='') as outfile:
                                        outfile.write(track['id']+'\t'+str(track_playcount)+'\n')
                                        outfile.flush()
                                        cached_playcounts[track['id']] = track_playcount
                                tracks_to_consider = update_tracks_to_consider_with_info(track,values,track_playcount,dict(),tracks_to_consider)
                        logging.info("Finished parsing all the stuff, just need to get track scores...")
                        tracks_to_consider = get_track_scores(tracks_to_consider)
                        logging.info("...got the scores, determining which tracks to add to playlist...")
                        track_ids_to_add_to_playlist, reasons_in_english_text = get_track_ids_to_add_to_playlist(tracks_to_consider, sp)
                        logging.info("...done!")
                        if playlist_id is None:
                            logging.info("User doesn't have a playlist, creating...")
                            new_playlist = sp.user_playlist_create(user=user_info['id'],name=f"MusicBirthday.com | {datetime.today().strftime('%B %d')}",public=True,description=None)
                            playlist_id = new_playlist['id']
                            logging.info("...playlist created successfully.")
                        logging.info(f"Updating playlist name...")
                        sp.playlist_change_details(playlist_id,name=f"MusicBirthday.com | {datetime.today().strftime('%B %d')}",public=True,description="Your FREE MusicBirthday.com Playlist!")
                        logging.info("...updated, updating playlist with track ids...")
                        sp.playlist_replace_items(playlist_id, track_ids_to_add_to_playlist)
                        logging.info("...updated. Getting current snapshot id...")
                        current_snapshot_id = sp.playlist(playlist_id)['snapshot_id']
                        logging.info("...retrieved, updating database...")
                        cursor.execute("""UPDATE user SET music_birthday_spotify_playlist_id = ?,music_birthday_spotify_snapshot_ids = (music_birthday_spotify_snapshot_ids || '|' || ?)
                        WHERE spotify_id = ?""",(playlist_id,current_snapshot_id, user_info['id']))
                        conn.commit()
                        logging.info("...updated successfully!")
                        if user_email is None:
                            logging.warning("No e-mail sent (NO EMAIL PROVIDED FROM USER)")
                            continue
                        logging.info("Sending email confirmation to user...")
                        send_email(user_email,playlist_id,reasons_in_english_text)
                        logging.info("...email sent successfully!")
                    except Exception as ee:
                        logging.critical(f"{spotify_id} | {user_email} | {ee}")
        sleep_minutes = 15
        logging.info(f"Finished processing users, waiting for {sleep_minutes} minutes until trying again...{target_time}")
        time.sleep(60*sleep_minutes)
        skip_first_loop = False