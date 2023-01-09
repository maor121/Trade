import glob
import os

import pandas as pd

from config import DATA_DIR
import numpy as np
import googlemaps

def normalize_str(s):
    return s.replace(".", "").replace(",", "").\
            replace("\"", "").\
            replace("'", "").replace("׳","").replace(":","").\
            replace(")", "").replace("(", "").replace("\u200b","").strip()


class Location:
    def __init__(self, city: str, street: str=None, street_num: int=None,
                 neighberhood: str=None):
        self.city = city
        self.street = street
        self.street_num = street_num
        self.neighberhood = neighberhood

    def __hash__(self):
        sorted_keys = sorted(self.__dict__.keys())
        return hash(
            tuple([self.__dict__[k] for k in sorted_keys]))

    def __eq__(self, other):
        if not isinstance(other, Location):
            return False
        for k in self.__dict__.keys():
            if self.__dict__[k] != other.__dict__[k]:
                return False
        return True

    def __str__(self):
        res = "%s, ישראל" % self.city
        if self.street and not self.street_num:
            res = "%s, %s" % ("רחוב " + self.street, res)
        elif self.street and self.street_num:
            res = "רחוב %s מספר %d, %s" % (self.street, self.street_num, res)
        else:
            res = "שכונת %s, %s" % (self.neighberhood, res)
        return res

    def __repr__(self):
        res = str(self)

        res = "Loc[%s]" % res
        return res


class LocExtractor:
    def __init__(self, loc_df):
        self.loc_df = loc_df

        # Ramat Gan / Givataym
        city_df = loc_df[loc_df['city_code'].isin([6300, 8600])]
        self.city_names = set([normalize_str(c) for c in city_df['city_name']])

        self.street_names = set([("S",normalize_str(city), normalize_str(street))
                                 for city, street in
                                 zip(city_df['city_name'],
                                     city_df['street_name'])
                                 if normalize_str(street) not in
                                 self.city_names and not
                                 normalize_str(street).startswith('שכ ')])

        self.neighberhood_names = set([("N",normalize_str(city), normalize_str(street).replace("שכ ",""))
                                 for city, street in
                                 zip(city_df['city_name'],
                                     city_df['street_name'])
                                 if normalize_str(street).startswith("שכ ")])

    def __call__(self, df_row):
        post_text = str(df_row['text'])

        post_text = normalize_str(post_text)
        post_words = np.array(post_text.split())  # use space as separator. Consider ntk

        # TODO: 2-3 words matches
        matches0, matches1, matches2 = set(), set(), set()
        matchesN = set()

        con_words = ["","ל","ב"]
        for t, c_name, s_name in self.street_names.union(self.neighberhood_names):

            is_neighberhood = t == "N"
            is_street = t == "S"

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
                        n = set(["בשכונת", "לשכונת", "שכונת"])

                        prev_w = post_words[s_name_w_idx-1][0] \
                            if s_name_w_idx >= 1 else None
                        next_w = post_words[s_name_w_idx+len(s_name_split)][0] \
                            if s_name_w_idx + len(s_name_split) < len(post_words) \
                            else None

                        if s_name == "דוד" and next_w and\
                                ("שמש" in next_w or "חשמל" in next_w):
                            continue

                        if is_neighberhood and prev_w and prev_w in n:
                            matchesN.add(Location(city=c_name,
                                                  neighberhood=s_name))

                        if not is_street:
                            continue

                        if next_w and next_w.isnumeric() and len(next_w) <= 3:
                            matches0.add(Location(city=c_name, street=s_name,
                                         street_num=int(next_w)))
                        elif prev_w and prev_w in s:
                            matches1.add(Location(city=c_name, street=s_name))
                        else:
                            matches2.add(Location(city=c_name, street=s_name))

        if len(matches0) > 0:
            filtered_matches = matches0
        elif len(matches1) > 0:
            filtered_matches = matches1
        elif len(matches2) > 0 and len(matchesN) == 0:
            filtered_matches = matches2
        else:
            filtered_matches = set()

        filtered_matches = filtered_matches.union(matchesN)

        # filter by cities
        found_cities = set()
        for c in self.city_names:
            for con_w in con_words:
                city_con = con_w + c
                if city_con in post_text:
                    found_cities.add(c)

        street_cities = set([m.city for m in filtered_matches])
        final_cities = found_cities & street_cities
        if len(final_cities) > 0 and len(street_cities) > 0:
            filtered_matches = set([m for m in filtered_matches if m.city in
                                    found_cities])

        print("----")
        print("Text: %s" % post_text)
        if len(filtered_matches) > 0:
            print("Matches0: %s" % matches0)
            print("Matches1: %s" % matches1)
            print("Matches2: %s" % matches2)
            print("MatchesN: %s" % matchesN)
            if len(filtered_matches) > 0:
                print("Filtered matches: %s" % filtered_matches)
        else:
            print("No Matches")
        print("----")
        return filtered_matches


def find_latest_csv(csv_glob_inp: str):
    csv_files = glob.glob(csv_glob_inp)
    # sort by creation time
    csv_files.sort(key=lambda x: os.path.getctime(x))
    latest_csv_file = csv_files[-1]
    return latest_csv_file


class DistExtractor:
    def __init__(self, API_KEY: str, org_st: str):
        self.gmaps = googlemaps.Client(key=API_KEY)
        self.org_loc = self.gmaps.geocode(org_st)[0]

    def __call__(self, df_row):
        post_locs = df_row['location']

        org_lat_lng = DistExtractor.extract_lat_lng(self.org_loc)
        dist_min = np.inf
        for post_loc in post_locs:
            post_loc_lat_lng = self.gmaps.geocode(post_loc)[0]
            post_loc_lat_lng = DistExtractor.extract_lat_lng(post_loc_lat_lng)
            dist = self.gmaps.distance_matrix(org_lat_lng, post_loc_lat_lng,
                                              mode='walking')
            dist_meters = dist['rows'][0]['elements'][0]['distance']['value']
            if dist_meters < dist_min:
                dist_min = dist_meters

        return dist_min

    @staticmethod
    def extract_lat_lng(gmap_loc):
        gmap_loc = gmap_loc['geometry']['location']
        return gmap_loc['lat'], gmap_loc['lng']


if __name__ == "__main__":
    GOOGLE_API_KEY = "KEY"

    loc_df = pd.read_csv(find_latest_csv("%s/Streets_*.csv" % DATA_DIR))
    posts_df = pd.read_csv(find_latest_csv("%s/Posts_*.csv" % DATA_DIR))
    posts_df['location'] = posts_df.apply(LocExtractor(loc_df), axis=1)
    posts_df['min_distance[m]'] = posts_df.apply(
        DistExtractor(GOOGLE_API_KEY, "רחוב כצלנסון, גבעתיים, ישראל"), axis=1)

    print(loc_df.head())
    print(posts_df.head())

    posts_df.to_csv("Result.csv", index=False)
