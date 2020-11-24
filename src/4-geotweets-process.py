import os
import sys
import subprocess
import time


def get_repo_root():
    """Get the root directory of the repo."""
    dir_in_repo = os.path.dirname(os.path.abspath('__file__'))
    return subprocess.check_output('git rev-parse --show-toplevel'.split(),
                                   cwd=dir_in_repo,
                                   universal_newlines=True).rstrip()


ROOT_dir = get_repo_root()
sys.path.append(ROOT_dir)
sys.path.insert(0, ROOT_dir + '/lib')

import lib.preprocess as preprocess


if __name__ == '__main__':
    # Preprocess of geotagged tweets in Twitter use timelines
    print("The preprocess of geotagged tweets started.")
    start_time = time.time()
    tw = preprocess.GeotweetsProcessor()

    # Filtering geotweets and save
    tw.tweets_load()
    tw.tweets_labeler_boundary()
    print("Geotagged tweets labelled: domestic vs international.")

    tw.tweets_filter_precise_geolocation()
    print("Place/cross-posting geotagged tweets removed.")

    tw.tweets_time_processor()
    print("UTC time converted to local time.")

    tw.tweets_filter_users_with_enough_geo()
    tw.tweets_filter_users_in_sweden()
    print("Non-Swedish residents removed.")

    tw.users_home_work_save()
    tw.tweets_save()
    print("The longitudinal dataset is preprocessed in %g seconds" % (time.time() - start_time))