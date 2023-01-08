import glob
import os

import pandas as pd

from config import DATA_DIR


class LocExtractor:
    def __init__(self, loc_df):
        self.loc_df = loc_df

        # Ramat Gan / Givataym
        city_df = loc_df[loc_df['city_code'].isin([6300, 8600])]

        self.street_names = set([c.strip() for c in city_df['street_name']])
        self.street_names -= {"גבעתיים"}

        con_words = ["ל", "ב"]
        self.street_names.update(
            [cw+c for cw in con_words for c in self.street_names]
        )

    def __call__(self, df_row):
        post_text = str(df_row['text'])

        post_text = post_text.replace(".", "").replace(",", "").\
            replace("\"", "").\
            replace("'", "").replace("׳","")
        post_words = post_text.split()  # use space as separator. Consider ntk

        # TODO: 2-3 words matches
        matches = []
        for w in post_words:
            if w in self.street_names:
               matches.append(w)

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
