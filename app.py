import logging
import threading
from threading import Thread
from typing import Set, Dict, Tuple

from flask.ctx import AppContext
from flask_apscheduler import APScheduler
from sqlalchemy import BigInteger

from app_factory import create_app
from config import RECREATE_DB
from db.models import db, FBGroup
from flask_jobs import thread_manage_jobs


def add_facebook_groups(app_ctx: AppContext):
    # RamatGan / Givataym (public groups)
    groups = [
        FBGroup(group_id=1424244737803677,
                is_private=False, _city_codes='6300|8600',
                should_scrape=True),
        FBGroup(group_id=186810449287215,
                is_private=False, _city_codes='6300|8600',
                should_scrape=True)
    ]

    with app_ctx:
        group_ids = set([g.group_id for g in groups])

        existing_groups = db.session.query(FBGroup).filter(
            FBGroup.group_id.in_(group_ids)).all()
        existing_group_ids = set([g.group_id for g in existing_groups])

        for group in groups:
            if group.group_id in existing_group_ids:
                continue

            db.session.add(group)

        db.session.commit()


if __name__ == "__main__":

    app = create_app()

    with app.app_context():
        if RECREATE_DB:
            logging.info("Dropping existing local DB")
            db.drop_all()
        logging.info("Updating\\Creating local DB schema")
        db.create_all()

    add_facebook_groups(app.app_context())

    stop_event = threading.Event()
    scheduler = APScheduler(app=app)

    thread_manage_jobs = Thread(target=thread_manage_jobs,
                                args=(scheduler, stop_event, app.app_context()))
    thread_manage_jobs.start()

    app.run(host="0.0.0.0", port=app.config['SERVER_PORT'])

    stop_event.set()
    thread_manage_jobs.join()

