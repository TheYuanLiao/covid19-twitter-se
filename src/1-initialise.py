# -*- coding: utf-8 -*-
"""
Created on Thu Nov 19 10:10:10 2020

@author: liaoy
"""

import pandas as pd
import numpy as np
import sys
import subprocess
import os


def get_repo_root():
    """Get the root directory of the repo."""
    dir_in_repo = os.path.dirname(os.path.abspath('__file__'))
    return subprocess.check_output('git rev-parse --show-toplevel'.split(),
                                   cwd=dir_in_repo,
                                   universal_newlines=True).rstrip()

ROOT_dir = get_repo_root()
sys.path.append(ROOT_dir)


if __name__ == "__main__":
    # Step 1 Initialise batches
    region_target = "Sweden"
    df_users = pd.read_csv(ROOT_dir + "/dbs/Sweden_valid_users.csv")
    chunks = np.array_split(df_users, 6)
    for ele, batch_num in zip(chunks, range(1, 7)):
        ele.to_csv(ROOT_dir + "/dbs/queries/users_%s.csv"% (batch_num), index=False)
    
    # Step 2 Create progress_logger
    for i in range(1, 7):
        with open(ROOT_dir + "/dbs/timelines/progress_%s.txt"%i, "a") as f:
            f.close()