import glob
import os

import pandas as pd

from config import DATA_DIR


def extract_location(df_row):
    return None


def find_latest_csv(csv_glob_inp: str):
    csv_files = glob.glob(csv_glob_inp)
    # sort by creation time
    csv_files.sort(key=lambda x: os.path.getctime(x))
    latest_csv_file = csv_files[-1]
    return latest_csv_file


if __name__ == "__main__":
    loc_df = pd.read_csv(find_latest_csv("%s/Streets_*.csv" % DATA_DIR))
    posts_df = pd.read_csv(find_latest_csv("%s/Posts_*.csv" % DATA_DIR))

    posts_df['location'] = posts_df.apply(extract_location, axis=1)

    print(loc_df.head())
    print(posts_df.head())
