import datetime
import os

import lxml
import pandas as pd
import facebook_scraper as f
import requests
from lxml import etree

from config import DATA_DIR, MAX_DAYS_BACK, MAX_POSTS


def scrape_facebook_posts():
    os.makedirs(DATA_DIR, exist_ok=True)

    f.enable_logging()

    # TLV
    # group_id = '1738501973091783'   #public
    # group_id = '101875683484689'    #private

    # Givataym
    group_id = '1424244737803677'  # public
    # group_id = '186810449287215'    # public

    result_dict = {}
    posts_counter = 0

    all_keys = set()

    for post_num, post in enumerate(
            f.get_posts(group=group_id, options={"allow_extra_requests": False},
                        max_past_limit=2,
                        latest_date=datetime.datetime.now() -
                                    datetime.timedelta(days=MAX_DAYS_BACK))):
        print("---------------------------------------")
        print("[Post_Num] %d" % post_num)
        print("---------------------------------------")

        for k, v in list(post.items()):
            if k not in result_dict:
                result_dict[k] = [] + [None] * posts_counter
            result_dict[k].append(v)

        for k in all_keys - post.keys():
            result_dict[k] += [None]
        for k in post.keys() - all_keys:
            all_keys.add(k)

        posts_counter += 1
        if posts_counter == MAX_POSTS:
            break

    result_df = pd.DataFrame.from_dict(result_dict, orient='columns')
    basename = datetime.datetime.now().strftime("Posts_%m-%d-%Y_%H-%M-%S")
    basename = os.path.join(DATA_DIR, basename)
    result_df.to_csv('%s.csv' % basename)
    result_df.to_excel('%s.xlsx' % basename)


def scrape_street_names():
    # manually download from:
    # https://data.gov.il/dataset/israel-streets-synom

    # neigberhoods? http://www.diva-gis.org/datadown (note license)
    pass

    # Scraper: not working, since page load dynamiclly

    # from bs4 import BeautifulSoup
    # page_url = "https://data.gov.il/dataset/israel-streets-synom"
    # page_content = requests.get(page_url).text
    # soup = BeautifulSoup(page_content, "html.parser")
    # # feed_container = soup.find(id="m_group_stories_container").find_all(
    # #     "p")
    # # for i in feed_container:
    # #     print(i.text)
    # dom = etree.HTML(str(soup))
    # dom.xpath('//*[@id="dataset-resources"]')

def scrape_neighberhoods():
    # https://www.givatayim.muni.il/979/
    pass


if __name__ == '__main__':
    # scrape_facebook_posts()
    scrape_street_names()
