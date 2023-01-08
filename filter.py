import glob
import os

import pandas as pd

from config import DATA_DIR
import numpy as np


def normalize_str(s):
    return s.replace(".", "").replace(",", "").\
            replace("\"", "").\
            replace("'", "").replace("׳","").replace(":","").\
            replace(")", "").replace("(", "").strip()


class LocExtractor:
    def __init__(self, loc_df):
        self.loc_df = loc_df

        # Ramat Gan / Givataym
        city_df = loc_df[loc_df['city_code'].isin([6300, 8600])]

        self.street_names = set([normalize_str(c)
                                 for c in city_df['street_name']])
        self.street_names -= {"גבעתיים", "רמת גן"}

    def __call__(self, df_row):
        post_text = str(df_row['text'])

        post_text = normalize_str(post_text)
        post_words = np.array(post_text.split())  # use space as separator. Consider ntk

        # TODO: 2-3 words matches
        matches = set()
        for s_name in self.street_names:
            for con_w in ["","ל","ב"]:
                s_name_con = con_w + s_name
                s_name_split = s_name_con.split()
                s_name_w0 = s_name_split[0]
                s_name_w_indices = np.argwhere(s_name_w0 == post_words)
                for s_name_w_idx in s_name_w_indices:
                    is_match = all([
                        (i + s_name_w_idx < len(post_words)) and (
                                    s_name_wi == post_words[i + s_name_w_idx])
                        for i, s_name_wi in enumerate(s_name_split)])
                    if is_match:
                        s = set(["רחוב", "ברחוב", "לרחוב", "רח", "ברח", "לרח", "כתובת", "הכתובת", "בכתובת", "מיקום", "המיקום"])
                        n = set(["בשכונת", "לשכונת"])

                        prev_w = post_words[s_name_w_idx-1][0] \
                            if s_name_w_idx >= 1 else None
                        next_w = post_words[s_name_w_idx+len(s_name_split)][0] \
                            if s_name_w_idx + len(s_name_split) < len(post_words) \
                            else None

                        if next_w and next_w.isnumeric() and len(next_w) <= 3:
                            a = "רחוב!"
                        elif prev_w and prev_w in s:
                            a = "רחוב?"
                        elif prev_w and prev_w in n:
                            a = "שכונה"
                        else:
                            a = None

                        if s_name == "דוד" and next_w and\
                                ("שמש" in next_w or "חשמל" in next_w):
                            continue

                        matches.add((a, s_name))
        matches0 = [("רחוב", m[1]) for m in matches if m[0] == "רחוב!"]
        matches1 = [("רחוב", m[1]) for m in matches if m[0] == "רחוב?"]
        matches2 = [("רחוב", m[1]) for m in matches if m[0] == None]
        matches3 = [("שכונה", m[1]) for m in matches if m[0] == "שכונה"]
        if len(matches0) > 0:
            filtered_matches = matches0
        elif len(matches1) > 0:
            filtered_matches = matches1
        elif len(matches2) > 0:
            filtered_matches = matches2
        elif len(matches3) > 0:
            filtered_matches = matches3
        else:
            filtered_matches = []
        print("----")
        print("Text: %s" % post_text)
        if len(matches) > 0:
            print("Matches: %s" % matches)
            if len(filtered_matches) > 0:
                print("Filtered matches: %s" % filtered_matches)
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
