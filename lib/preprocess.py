import pandas as pd
import geopandas as gpd
import yaml
import os
import subprocess
import operator
from joblib import Parallel, delayed
import multiprocessing as mp
import numpy as np
from tzwhere import tzwhere
from dateutil import tz

tzg = tzwhere.tzwhere(forceTZ=True)


def get_repo_root():
    """Get the root directory of the repo."""
    dir_in_repo = os.path.dirname(os.path.abspath('__file__'))
    return subprocess.check_output('git rev-parse --show-toplevel'.split(),
                                   cwd=dir_in_repo,
                                   universal_newlines=True).rstrip()


ROOT_dir = get_repo_root()

with open(ROOT_dir + '/lib/geo_info.yaml') as f:
    geo_manager = yaml.load(f, Loader=yaml.FullLoader)


def zones_load(geoinformation):
    # The boundary to use when removing users based on location.
    metric_epsg = geoinformation['epsg']
    zone_id = geoinformation['zone_id']
    zones_path = geoinformation['zones_path']
    zones = gpd.read_file(ROOT_dir + zones_path)
    zones = zones[zones[zone_id].notnull()]
    zones = zones.rename(columns={zone_id: "zone"})
    zones.zone = zones.zone.astype(int)
    zones = zones[zones.geometry.notnull()].to_crs(metric_epsg)
    return zones, zones.assign(a=1).dissolve(by='a').simplify(tolerance=0.2).to_crs("EPSG:4326")


def where_self(row):
    try:
        x = tzg.tzNameAt(row["lat"], row["lng"], forceTZ=True)
    except:
        x = "Unknown"
    return x


def home_work_detection(uid, data):
    d = {}
    data_c = data.copy()
    data_c.sort_values(by=['time_local'], inplace=True)
    place_freq = data_c.groupby(['lat', 'lng']).size()

    # Home detection
    stay_dict_home = {}
    for x in list(place_freq.index):
        # Non-working hours for weekdays
        weekdays = data_c.loc[(data_c['lat'] == x[0]) & (data_c['lng'] == x[1]) &
                              (data_c['hourofday'].isin([0, 1, 2, 3, 4, 5, 6, 7, 19, 20, 21, 22, 23])) &
                              (data_c['weekday'].isin([0, 1, 2, 3, 4])), :].size
        # Most of the time for weekends
        weekends = data_c.loc[(data_c['lat'] == x[0]) & (data_c['lng'] == x[1]) &
                              (data_c['weekday'].isin([5, 6])), :].size
        stay_dict_home[x] = weekdays + weekends
    if len(stay_dict_home) > 0:
        home = max(stay_dict_home.items(), key=operator.itemgetter(1))[0]
        d["home_lat"], d["home_lng"] = home[0], home[1]
        d["home_where"], d["home_freq"] = tzg.tzNameAt(home[0], home[1], forceTZ=True), place_freq[home] / len(data)

        # Work detection
        place_freq.drop(home, inplace=True)
        stay_dict_work = {}
        for x in list(place_freq.index):
            # Working hours for weekdays
            weekdays = data_c.loc[(data_c['lat'] == x[0]) & (data_c['lng'] == x[1]) &
                                  (data_c['hourofday'].isin([8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18])) &
                                  (data_c['weekday'].isin([0, 1, 2, 3, 4])), :]
            stay_dict_work[x] = len(weekdays)
        if len(stay_dict_work) > 0:
            work = max(stay_dict_work, key=stay_dict_work.get)
            d["work_lat"], d["work_lng"] = work[0], work[1]  # (lat, lng)
            d["work_where"], d["work_freq"] = tzg.tzNameAt(work[0], work[1], forceTZ=True), place_freq[work] / len(data)
        else:
            d["work_lat"], d["work_lng"] = np.nan, np.nan
            d["work_where"], d["work_freq"] = None, None
    else:
        d["home_lat"], d["home_lng"] = np.nan, np.nan
        d["home_where"], d["home_freq"] = None, None
        d["work_lat"], d["work_lng"] = np.nan, np.nan
        d["work_where"], d["work_freq"] = None, None
    d['user_name'] = uid
    return d


def pd_apply_parallel_list_of_dict(df_groups, function):
    results = Parallel(n_jobs=mp.cpu_count())(delayed(function)(name, group) for name, group in df_groups)
    return pd.DataFrame(results)


class GeotweetsProcessor:
    def __init__(self):
        # Define the data type
        self.geo_info = geo_manager['provinces']

        # Which .csv file to get geotweets from
        if os.path.exists(ROOT_dir + f"/dbs/tweets_20201123_se_geolocations.csv"):
            self.raw_geotweets = os.path.join(ROOT_dir, "dbs/tweets_20201123_se_geolocations.csv")
        else:
            raise Exception(f"The specified file does not exist!")

        # Where to save CSVs for geotweets and homelocations
        self.csv_geotweets = os.path.join(ROOT_dir, "dbs/geotweets/geotweets_se.csv")

        # Place holder for the processed geotagged tweets
        self.geotweets = None

        # The more precise boundary of Sweden
        _, self.boundary = zones_load(self.geo_info)

        # Place to save the detected home and work locations
        self.csv_home_work = os.path.join(ROOT_dir, "dbs/geotweets/users_home_workplace_se.csv")

        # Place holder for the processed geotagged tweets
        self.home_work = None

    def tweets_load(self):
        # Load geotweets from .csv
        geotweets = pd.read_csv(self.raw_geotweets)
        self.geotweets = gpd.GeoDataFrame(geotweets, crs='EPSG:4326',
                                          geometry=gpd.points_from_xy(geotweets['lng'], geotweets['lat']))

    def tweets_labeler_boundary(self):
        # Filter out the geotagged tweets that are outside the more precise boundary of Sweden
        if self.boundary is not None:
            gdf_domestic = gpd.clip(self.geotweets, self.boundary.convex_hull)
            gdf_domestic.loc[:, 'dom'] = 1
            gdf_international = self.geotweets.loc[~self.geotweets.index.isin(gdf_domestic.index), :]
            gdf_international.loc[:, 'dom'] = 0
            self.geotweets = pd.concat([gdf_domestic, gdf_international])
        else:
            raise Exception(f"The boundary should be loaded first.")

    def tweets_filter_precise_geolocation(self):
        # Filter out the geotagged tweets that potentially point to a non-precise location
        self.geotweets.drop(columns=['geometry'], inplace=True)
        coord_counts = self.geotweets.groupby(['lat', 'lng']).size().sort_values(ascending=False)
        coord_percentages = (coord_counts / self.geotweets.shape[0]).to_frame("perc").reset_index()
        percentages_to_remove = coord_percentages[coord_percentages.perc > 0.001]
        perc_filter = None
        for (_, row) in percentages_to_remove.iterrows():
            f = (self.geotweets.lat != row.lat) & (self.geotweets.lng != row.lng)
            if perc_filter is None:
                perc_filter = f
            else:
                perc_filter = perc_filter & f
        share_removed = perc_filter[~perc_filter].size / len(self.geotweets) * 100
        print("Removing %.2f percentage of center-of-region geotweets" % share_removed)
        self.geotweets = self.geotweets[perc_filter]

    def tweets_time_processor(self):
        # Convert time to local time
        self.geotweets['time'] = pd.to_datetime(self.geotweets['time'], infer_datetime_format=True)
        # Find time zone by coordinates
        self.geotweets.loc[:, 'time_zone'] = self.geotweets.apply(lambda row: where_self(row), axis=1)
        self.geotweets.loc[self.geotweets.dom == 1, 'time_zone'] = 'Europe/Stockholm'
        self.geotweets = self.geotweets.loc[(self.geotweets.time_zone != "Unknown") &
                                            (self.geotweets.time_zone != "uninhabited"), :]
        # Convert to local time
        self.geotweets.loc[:, 'time_local'] = self.geotweets.groupby('time_zone')['time'].apply(lambda x: x.dt.tz_localize('UTC').dt.tz_convert(x.name))
        self.geotweets = self.geotweets.loc[~self.geotweets['time_local'].isna(), :]
        self.geotweets.loc[:, "date"] = self.geotweets.loc[:, "time_local"].apply(lambda x: x.date())
        self.geotweets.loc[:, "hourofday"] = self.geotweets.loc[:, "time_local"].apply(lambda x: x.hour)
        self.geotweets.loc[:, "weekday"] = self.geotweets.loc[:, "time_local"].apply(lambda x: x.weekday())

    def tweets_filter_users_with_enough_geo(self):
        tweet_count_before = self.geotweets.groupby('user_name').size()
        user2select = tweet_count_before[tweet_count_before <= 50].index
        self.geotweets = self.geotweets.loc[self.geotweets['user_name'].isin(user2select), :]

    def tweets_filter_users_in_sweden(self):
        # Identify home and work place
        self.home_work = pd_apply_parallel_list_of_dict(self.geotweets.groupby('user_name'), home_work_detection)
        initial_number_users = len(self.home_work)

        # Remove the users without detected home or work
        self.home_work = self.home_work.dropna(how='any')
        print(self.home_work)
        # Filter out the users who do not live in Sweden
        self.home_work = gpd.GeoDataFrame(self.home_work, crs='EPSG:4326',
                                          geometry=gpd.points_from_xy(self.home_work['home_lng'],
                                                                      self.home_work['home_lat']))
        self.home_work = gpd.clip(self.home_work, self.boundary.convex_hull)

        # Filter out the users who do not work in Sweden
        self.home_work = gpd.GeoDataFrame(self.home_work.drop(columns=['geometry']), crs='EPSG:4326',
                                          geometry=gpd.points_from_xy(self.home_work['work_lng'],
                                                                      self.home_work['work_lat']))
        self.home_work = gpd.clip(self.home_work, self.boundary.convex_hull)
        self.home_work.drop(columns=['geometry'], inplace=True)
        # Print out how large the share of users are removed
        share_removed = len(self.home_work) / initial_number_users * 100
        print("Removing %.2f percentage of the top twitter users due to living/working outside Sweden." % share_removed)

        # Only keep the records from the valid users
        self.geotweets = self.geotweets[self.geotweets.user_name.isin(self.home_work.user_name)]

    def tweets_save(self):
        if os.path.exists(self.csv_geotweets):
            print('Geotweets file exists and will be overwritten.')
        self.geotweets.to_csv(self.csv_geotweets)

    def users_home_work_save(self):
        if os.path.exists(self.csv_home_work):
            print('Home and work file exists and will be overwritten.')
        self.home_work.to_csv(self.csv_home_work, index=False)