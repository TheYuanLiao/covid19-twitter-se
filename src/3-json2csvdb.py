# -*- coding: utf-8 -*-
"""
Created on Mon Nov 23 12:16:41 2020

@author: Yuan Liao

Convert collected user timelines (.json) into a organized sqlite3 db
"""

import json
import sys
import subprocess
import os
import io
import pandas as pd
from pathlib import Path
from tqdm import tqdm
import sqlalchemy
import time


def get_repo_root():
    """Get the root directory of the repo."""
    dir_in_repo = os.path.dirname(os.path.abspath('__file__'))
    return subprocess.check_output('git rev-parse --show-toplevel'.split(),
                                   cwd=dir_in_repo,
                                   universal_newlines=True).rstrip()


ROOT_dir = get_repo_root()
sys.path.append(ROOT_dir)


def tweet_parser(tweet):
    tw_id = tweet['id_str']
    # User information
    user = tweet['user']
    name = user['id_str']
    location = user['location']  # string (City, Country abbreviation?State?)
    descp = user['description']  # string
    time_zone = user['time_zone']  # string
    utc_offset = user['utc_offset']  # string

    # Time
    time = tweet['created_at']  # string Mon Jul 24 14:25:00 +0000 2017 <class 'str'>
    time = time.split()
    time = time[1] + ' ' + time[2] + ' ' + time[3] + ' ' + time[5]

    # Geolocation
    geo_label = int(tweet['geo'] != None)
    if geo_label != 0:
        coord_lat = float(tweet['geo']['coordinates'][0])
        coord_lng = float(tweet['geo']['coordinates'][1])
    else:
        coord_lat = None
        coord_lng = None

    # Place
    place_label = int(tweet['place'] != None)
    if place_label != 0:
        place_id = tweet['place']['id']
        country = tweet['place']['country']
        full_name = tweet['place']['full_name']
    else:
        place_id = None
        country = None
        full_name = None

    # Tweet content
    text = tweet['text']
    return (tw_id, time, geo_label, coord_lat, coord_lng,
            place_label, place_id, country, full_name, text,
            name, location, descp, time_zone, utc_offset)


def user2records(user_json_file):
    recs = list()
    for line in open(file, 'r'):
        tweet = json.loads(line)
        recs.append(tweet_parser(tweet))
    return pd.DataFrame(recs, columns=["tw_id", "time", "geo_label", "lat", "lng",
                                       "place_label", "place_id", "place_country", "place_full_name", "content",
                                       "user_name", "user_location", "user_descp", "user_time_zone", "user_utc_offset"])


def dump2sqlite_df(df, db_path, table_name):
    """
    Dump a pandas dataframe to a database: a fast solution.
    :param df: string, data to dump as a table
    :param db_path: string, database connection path
    :param table_name: string, table name to be created in database
    :return: None
    """
    engine = sqlalchemy.create_engine(db_path)
    sqlite_connection = engine.connect()
    stt = time.time()
    df.to_sql(table_name, sqlite_connection, if_exists='replace', index=False)
    print('Data dumped: %g' % (time.time() - stt))


if __name__ == '__main__':
    filepath = os.path.join(ROOT_dir, 'dbs/timelines')
    file_list = os.listdir(filepath)
    file_list = [os.path.join(filepath, x) for x in file_list if
                 (x.endswith(".json")) & (Path(os.path.join(filepath, x)).stat().st_size > 0)]
    df_list = []
    for file in tqdm(file_list, desc="Processing user timelines"):
        df = user2records(file)
        df_list.append(df)
    df = pd.concat(df_list)
    db_path = 'sqlite:///D:\\covid-19-twitter-se\\dbs\\tweets_20201123_se.sqlite'
    dump2sqlite_df(df, db_path, 'records')
    df.loc[df.geo_label == 1, ].to_csv(os.path.join(ROOT_dir, 'dbs',
                                                    'tweets_20201123_se_geolocations.csv'), index=False)
