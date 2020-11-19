# -*- coding: utf-8 -*-
"""
Created on Thu Apr 18 10:55:48 2019

@author: liaoy
"""

import jsonpickle
import tweepy
import time
import pandas as pd
import numpy as np
import sqlite3


if __name__ == "__main__":
    # Step 1 Initialise batches
    region_target = "Sweden"
    df_users = pd.read_csv("Sweden_valid_users.csv")
    chunks = np.array_split(df_users, 6)
    for ele in chunks:
        
