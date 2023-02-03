import logging
import threading
from datetime import datetime

import numpy as np
from flask.ctx import AppContext
from flask_apscheduler import APScheduler

from db.models import db, FBGroup, FBPost
from scrape import scrape


def thread_manage_jobs(scheduler: APScheduler, stop_event: threading.Event,
                       app_ctx: AppContext):
    scheduler.add_job(id='Facebook_scrape', func=facebook_scrape_1_iteration,
                      args=(app_ctx,),
                      trigger="interval", next_run_time=datetime.now(),
                      hours=1)
    scheduler.start()

    # TODO: loop, change job params when updated through website
    stop_event.wait()

    scheduler.shutdown()


def facebook_scrape_1_iteration(app_ctx: AppContext):
    logging.info("facebook_scrape_1_iteration")

    with app_ctx:
        fb_groups = db.session.query(FBGroup).filter(
            FBGroup.should_scrape is True).all()

        for fb_group in fb_groups:
            results_df = scrape.scrape_facebook_posts(
                fb_group.group_id,
                max_past_limit=2,
                max_days_back=4,
                max_posts=3
            )

            existing_posts = db.session.query(FBPost).filter(
                FBPost.post_id in set(results_df['post_id'])
            )
            existing_post_ids = set([p.post_id for p in existing_posts])

            for __, post in results_df.iterrows():
                post_id = post['post_id']
                post_text = post['post_text']

                if post_id in existing_post_ids:
                    continue

                new_post = FBPost(post_id=post_id, post_text=post_text,
                                  group=fb_group)

                db.session.add(new_post)

            db.session.commit()
