import datetime
import os

import numpy as np
import pandas as pd
import facebook_scraper as f


def scrape_facebook_post_iter(group_id: str,
                              max_past_limit=2,
                              max_days_back=100):
    f.enable_logging()

    for post_num, post in enumerate(
            f.get_posts(group=group_id, options={"allow_extra_requests": False},
                        max_past_limit=max_past_limit,
                        latest_date=datetime.datetime.now() -
                                    datetime.timedelta(days=max_days_back))):

        yield post_num, post


def scrape_facebook_posts(group_id: str, max_past_limit=2, max_days_back=100,
                          max_posts=np.inf):
    f.enable_logging()

    result_dict = {}
    posts_counter = 0

    all_keys = set()

    for post_num, post in enumerate(
            f.get_posts(group=group_id, options={"allow_extra_requests": False},
                        max_past_limit=max_past_limit,
                        latest_date=datetime.datetime.now() -
                                    datetime.timedelta(days=max_days_back))):
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
        if posts_counter == max_posts:
            break

    return pd.DataFrame.from_dict(result_dict, orient='columns')


def scrape_facebook_posts_csv(data_dir, **kwargs):
    os.makedirs(data_dir, exist_ok=True)

    result_df = scrape_facebook_posts(**kwargs)

    basename = datetime.datetime.now().strftime("Posts_%m-%d-%Y_%H-%M-%S")
    basename = os.path.join(data_dir, basename)
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
    # https://zips.co.il/city/%D7%A8%D7%9E%D7%AA-%D7%92%D7%9F/8600
    # https://www.kaggle.com/datasets/danofer/israel-census
    pass


if __name__ == '__main__':
    DATA_DIR = "./data"
    MAX_DAYS_BACK=100
    MAX_POSTS = np.inf

    # TLV
    # group_id = '1738501973091783'   #public
    # group_id = '101875683484689'    #private

    # Givataym
    group_id = '1424244737803677'  # public
    # group_id = '186810449287215'    # public

    # scrape_facebook_posts_csv()
    scrape_street_names()
