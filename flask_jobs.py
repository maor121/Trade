import threading

from flask_apscheduler import APScheduler

from db.models import db, FBGroup


def thread_manage_jobs(scheduler: APScheduler, stop_event: threading.Event):
    scheduler.add_job(id='Facebook_scrape', func=facebook_scrape_1_iteration,
                      trigger="interval", hours=1)
    scheduler.start()

    # TODO: loop, change job params when updated through website
    stop_event.wait()

    scheduler.shutdown()


def facebook_scrape_1_iteration():
    fb_groups = db.session.query(FBGroup).filter(FBGroup.should_scrape is True)\
        .all()

    for fb_group in fb_groups:
