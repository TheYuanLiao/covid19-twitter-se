# -*- coding: utf-8 -*-
"""
Created on Thu Nov 19 10:10:10 2020

@author: liaoy
"""

import pandas as pd
import numpy as np


if __name__ == "__main__":
    # Step 1 Initialise batches
    region_target = "Sweden"
    df_users = pd.read_csv("dbs/Sweden_valid_users.csv")
    chunks = np.array_split(df_users, 6)
    for ele, batch_num in zip(chunks, range(1, 7)):
        ele.to_csv("dbs/queries/users_%s.csv"% (batch_num))
    
    # Step 2 Create progress_logger
    for i in range(1, 7):
        with open("dbs/timelines/progress_%s.txt"%i, "a") as f:
            f.close()