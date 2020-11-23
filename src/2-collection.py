# -*- coding: utf-8 -*-
"""
Created on Thu Nov 19 10:23:33 2020

@author: liaoy
"""

import yaml
import pandas as pd
from tqdm import tqdm
import multiprocessing as mp
import sys
import subprocess
import os
import tweepy
import time
import jsonpickle


def get_repo_root():
    """Get the root directory of the repo."""
    dir_in_repo = os.path.dirname(os.path.abspath('__file__'))
    return subprocess.check_output('git rev-parse --show-toplevel'.split(),
                                   cwd=dir_in_repo,
                                   universal_newlines=True).rstrip()

ROOT_dir = get_repo_root()
sys.path.append(ROOT_dir)


with open(os.path.join(ROOT_dir, 'dbs', 'keys.yaml')) as f:
    keys_manager = yaml.safe_load(f)


def user_collection(row, batch, progress_logger, api):
    userID = str(row['UserName'])
    filename = 'UserID_'+ userID + '_'+ time.strftime("%Y%m%d-%H%M%S")+'.json'
    if userID not in progress_logger:
        with open(os.path.join(ROOT_dir, 'dbs/timelines', filename), 'w') as f:
    #        try:
    #            new_tweets = api.user_timeline(user_id = df1['user'][i],count=200, tweet_mode="extended", include_rts = True) #user_id = 1
    #        except:
    #            protectedUsers.append(df1['user'][i])
    #            continue  
            try:
                for tweet in tweepy.Cursor(api.user_timeline, id=userID).items():
                    f.write(jsonpickle.encode(tweet._json, unpicklable=False) + '\n')
            except:
                print("Problem pull timelines...")
        
            #You can check how many queries you have left using rate_limit_status() method
            rate_limit = api.rate_limit_status()['resources']['statuses']['/statuses/user_timeline']['remaining']
            print("Remaining: ", rate_limit)
            if rate_limit < 180:
                print("Sleep for 15 min...")
                time.sleep(15*60)
        f.close()
        with open(os.path.join(ROOT_dir, 'dbs/timelines', "progress_" + batch + ".txt"), "a") as f:
            f.write(userID)
            f.write("\n")
            f.close()


def user_timeline_collection(file, keys_manager):
    batch = file.split(".")[0].split("_")[-1]
    ACCESS_TOKEN = keys_manager['key' + str(batch)]['ACCESS_TOKEN']
    ACCESS_SECRET = keys_manager['key' + str(batch)]['ACCESS_SECRET']
    CONSUMER_KEY = keys_manager['key' + str(batch)]['CONSUMER_KEY']
    CONSUMER_SECRET = keys_manager['key' + str(batch)]['CONSUMER_SECRET']
    auth = tweepy.OAuthHandler(ACCESS_TOKEN, ACCESS_SECRET)
    auth.set_access_token(CONSUMER_KEY, CONSUMER_SECRET)
    api = tweepy.API(auth)
    #Error handling
    if (not api):
        print ("Problem connecting to API")
    else:
        progress_logger_file = open(os.path.join(ROOT_dir, "dbs/timelines", "progress_" + batch + ".txt"), "r")
        progress_logger = progress_logger_file.read().splitlines()
        df_batch = pd.read_csv(os.path.join(ROOT_dir, "dbs/queries", file))
        tqdm.pandas(desc="Routing for file %s" % batch)
        df_batch.progress_apply(lambda row: user_collection(row, batch, progress_logger, api), axis=1)


if __name__ == '__main__':
    file_list = os.listdir(ROOT_dir + '/dbs/queries')
    file_list = [x for x in file_list if x.split('.')[-1] == 'csv']
#    file = file_list[0]
#    user_timeline_collection(file, keys_manager)
    pool = mp.Pool(mp.cpu_count())
    pool.starmap(user_timeline_collection, [(x, keys_manager) for x in file_list])
    pool.close()