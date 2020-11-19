# -*- coding: utf-8 -*-
"""
Created on Thu Mar  7 17:18:13 2019

@author: liaoy
"""

import jsonpickle
import tweepy
import time
import pandas as pd
import sqlite3
#%% Step 1 Load top users
region_target = "The Netherlands"
batch_num = 3
df_user = pd.read_csv(region_target + "_valid_users_batch_%s.csv"%batch_num)
Users = list(df_user['UserName'])
df_users_done = pd.read_csv(region_target + "_valid_users_batch_%s_remained_done.csv"%batch_num)
#%% Step 2 Tokens from wujian09.thu@gmail.com for timeline retrieving
ACCESS_TOKEN = 'NiahLYQR3bA7Qwdd2tBEa91zr' # Consumer API key
ACCESS_SECRET = '6xWNmp3lJDfr66j7m40bNqh5bbHBfzorGRp1fsL3lYgnsmiaxf' # Consumer secret key
CONSUMER_KEY = '1110842328728637440-91ZDuAubus8IDbnKixq6HwlBZ24JeH' # Access token
CONSUMER_SECRET = 'g8UlsvOjD63H8M3iENWboejB465MAUR7PtotLIwplJAiz' # Access token secret

auth = tweepy.OAuthHandler(ACCESS_TOKEN, ACCESS_SECRET)
auth.set_access_token(CONSUMER_KEY, CONSUMER_SECRET)

api = tweepy.API(auth)
#Error handling
if (not api):
    print ("Problem connecting to API")
    
for userID in Users:
    filename = 'UserID_'+ str(userID) + '_'+ time.strftime("%Y%m%d-%H%M%S")+'.json'
    df_users_done = pd.read_csv(region_target + "_valid_users_batch_%s_remained_done.csv"%batch_num)
    if userID not in list(df_users_done["UserName"]):
        with open(filename, 'w') as f:
    #        try:
    #            new_tweets = api.user_timeline(user_id = df1['user'][i],count=200, tweet_mode="extended", include_rts = True) #user_id = 1
    #        except:
    #            protectedUsers.append(df1['user'][i])
    #            continue  
            try:
                for tweet in tweepy.Cursor(api.user_timeline, id=userID).items():
                    f.write(jsonpickle.encode(tweet._json, unpicklable=False) + '\n')
                df_users_done = df_users_done.append(pd.DataFrame([[df_user.loc[df_user["UserName"]==userID, "UserID"].values[0], userID]],columns=["UserID", "UserName"]))
                df_users_done.to_csv(region_target + "_valid_users_batch_%s_remained_done.csv"%batch_num, index=False)
            except:
                print("Problem pull timelines...")
        
            #You can check how many queries you have left using rate_limit_status() method
            rate_limit = api.rate_limit_status()['resources']['statuses']['/statuses/user_timeline']['remaining']
            print("Remaining: ", rate_limit)
            if rate_limit < 180:
                print("Sleep for 15 min...")
                time.sleep(15*60)
        f.close()