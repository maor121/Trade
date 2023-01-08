import glob
import os

import pandas as pd

from config import DATA_DIR
import numpy as np


def normalize_str(s):
    return s.replace(".", "").replace(",", "").\
            replace("\"", "").\
            replace("'", "").replace("׳","").strip()


class LocExtractor:
    def __init__(self, loc_df):
        self.loc_df = loc_df

        # Ramat Gan / Givataym
        city_df = loc_df[loc_df['city_code'].isin([6300, 8600])]

        self.street_names = set([normalize_str(c)
                                 for c in city_df['street_name']])
        self.street_names -= {"גבעתיים", "רמת גן"}

        con_words = ["ל", "ב"]
        self.street_names.update(
            [cw+c for cw in con_words for c in self.street_names]
        )

    def __call__(self, df_row):
        post_text = str(df_row['text'])

        post_text = normalize_str(post_text)
        post_words = np.array(post_text.split())  # use space as separator. Consider ntk

        # TODO: 2-3 words matches
        matches = set()
        for s_name in self.street_names:
            s_name_split = s_name.split()
            s_name_w0 = s_name_split[0]
            s_name_w_indices = np.argwhere(s_name_w0 == post_words)
            for s_name_w_idx in s_name_w_indices:
                is_match = all([
                    (i + s_name_w_idx < len(post_words)) and (
                                s_name_wi == post_words[i + s_name_w_idx])
                    for i, s_name_wi in enumerate(s_name_split)])
                if is_match:
                    matches.add(s_name)
        print("----")
        print("Text: %s" % post_text)
        if len(matches) > 0:
            print("Matches: %s" % matches)
        else:
            print("No Matches")
        print("----")
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

    posts_df['location'] = posts_df.apply(LocExtractor(loc_df), axis=1)

    print(loc_df.head())
    print(posts_df.head())
