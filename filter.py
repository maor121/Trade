import glob
import os

import pandas as pd

from config import DATA_DIR


def extract_location(df_row):
    return None


if __name__ == "__main__":
    csv_files = glob.glob("%s/*.csv" % DATA_DIR)
    # sort by creation time
    csv_files.sort(key=lambda x: os.path.getctime(x))
    latest_csv_file = csv_files[-1]

    data_df = pd.read_csv(latest_csv_file)

    data_df['location'] = data_df.apply(extract_location, axis=1)

    print(data_df.head())