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
    def __init__(self, loc_df, neighber_df):
        self.loc_df = loc_df

        # Ramat Gan / Givataym
        city_df = loc_df[loc_df['city_code'].isin([6300, 8600])]
        self.city_names = set([normalize_str(c) for c in city_df['city_name']])

        self.street_names = set([(normalize_str(city), normalize_str(street))
                                 for city, street in
                                 zip(city_df['city_name'],
                                     city_df['street_name'])
                                 if normalize_str(street) not in
                                 self.city_names])

        self.neighberhood_by_street = {}
        for __, row in neighber_df.iterrows():
            self.neighberhood_by_street[row['city'],
                normalize_str(row['street'])] = \
                normalize_str(row['neighberhood'])

    def __call__(self, df_row):
        post_text = str(df_row['text'])

        post_text = normalize_str(post_text)
        post_words = np.array(post_text.split())  # use space as separator. Consider ntk

        # TODO: 2-3 words matches
        matches = set()
        con_words = ["","ל","ב"]
        for c_name, s_name in self.street_names:
            for con_w in con_words:
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

                        matches.add((a, s_name, c_name,
                                     self.neighberhood_by_street.get(
                                         (c_name, s_name))))
        matches0 = [("רחוב", m[1], m[2], m[3]) for m in matches if m[0] == "רחוב!"]
        matches1 = [("רחוב", m[1], m[2], m[3]) for m in matches if m[0] == "רחוב?"]
        matches2 = [("רחוב", m[1], m[2], m[3]) for m in matches if m[0] == None]
        matches3 = [("שכונה", m[1], m[2], m[1]) for m in matches if m[0] == "שכונה"]
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

        # filter by cities
        found_cities = set()
        for c in self.city_names:
            for con_w in con_words:
                city_con = con_w + c
                if city_con in post_text:
                    found_cities.add(c)

        street_cities = set([m[2] for m in filtered_matches])
        final_cities = found_cities & street_cities
        if len(final_cities) > 0 and len(street_cities) > 0:
            filtered_matches = [m for m in filtered_matches if m[2] in
                                found_cities]

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

    neighber_giva_df = pd.read_csv("%s/Giva_Neighberhoods.csv" % DATA_DIR)
    neighber_giva_df['city'] = ["גבעתיים"] * len(neighber_giva_df)
    neighber_rg_df = pd.read_csv("%s/RG_Neighberhoods.csv" % DATA_DIR)
    neighber_rg_df['city'] = ["רמת גן"] * len(neighber_rg_df)
    neighber_df = pd.concat([neighber_rg_df, neighber_giva_df])

    posts_df['location'] = posts_df.apply(LocExtractor(loc_df,
                                                       neighber_df), axis=1)

    print(loc_df.head())
    print(posts_df.head())
