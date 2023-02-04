import logging
import threading
import datetime
from typing import List

from flask.ctx import AppContext
from flask_apscheduler import APScheduler

from db.models import db, FBGroup, FBPost, User
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

    new_posts = []

    with app_ctx:
        fb_groups = db.session.query(FBGroup).filter(
            FBGroup.should_scrape == True).all()

        max_past_limit = 5

        for fb_group in fb_groups:
            max_days_back = 4 if fb_group.last_scrape_date is None else \
                ((datetime.datetime.now() -
                 fb_group.fb_group.last_scrape_date).days + 3)

            past_limit_hits = 0

            for post_num, post_json in scrape.scrape_facebook_post_iter(
                    fb_group.group_id,
                    max_past_limit=max_past_limit,
                    max_days_back=max_days_back):

                post_id = int(post_json['post_id'])
                post_text = post_json['post_text']

                post_db = db.session.query(FBPost).filter(
                    FBPost.post_id == post_id).first()

                if post_db is not None:
                    # exists already
                    logging.info("Scraped post[%d]. Already exists" % post_id)
                    past_limit_hits += 1

                    if past_limit_hits >= max_past_limit:
                        # no need to scrape this group anymore
                        logging.info("Max past limit reached, "
                                     "group[%d] is up to date" %
                                     fb_group.group_id)
                        break
                    else:
                        continue

                new_post = FBPost(post_id=post_id, post_text=post_text,
                                  group=fb_group)

                db.session.add(new_post)

                new_posts.append(new_post)

            db.session.commit()

    notify_users_on_scrape_end(app_ctx, new_posts)


def notify_users_on_scrape_end(app_ctx: AppContext, new_posts: List[FBPost]):
    with app_ctx:
        all_users = db.session.query(User).all()

        # for user in all_users: