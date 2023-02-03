import logging
import threading
import datetime

import numpy as np
from flask.ctx import AppContext
from flask_apscheduler import APScheduler

from db.models import db, FBGroup, FBPost
from scrape import scrape


def thread_manage_jobs(scheduler: APScheduler, stop_event: threading.Event,
                       app_ctx: AppContext):
    scheduler.add_job(id='Facebook_scrape', func=facebook_scrape_1_iteration,
                      args=(app_ctx,),
                      trigger="interval",
                      next_run_time=datetime.datetime.now(),
                      hours=1)
    scheduler.start()

    # TODO: loop, change job params when updated through website
    stop_event.wait()

    scheduler.shutdown()


def facebook_scrape_1_iteration(app_ctx: AppContext):
    logging.info("facebook_scrape_1_iteration")

    with app_ctx:
        fb_groups = db.session.query(FBGroup).filter(
            FBGroup.should_scrape == True).all()

        for fb_group in fb_groups:
            max_days_back = 4 if fb_group.last_scrape_date is None else \
                ((datetime.datetime.now() -
                 fb_group.fb_group.last_scrape_date).days + 3)

            results_df = scrape.scrape_facebook_posts(
                fb_group.group_id,
                max_past_limit=2,
                max_days_back=max_days_back,
                max_posts=10
            )

            existing_posts = db.session.query(FBPost).filter(
                FBPost.post_id.in_(set(results_df['post_id']))).all()

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
