import logging
import threading
from threading import Thread

from flask_apscheduler import APScheduler

from app_factory import create_app
from config import RECREATE_DB
from db.models import db
from flask_jobs import facebook_scrape_1_iteration, thread_manage_jobs

if __name__ == "__main__":

    app = create_app()

    with app.app_context():
        if RECREATE_DB:
            logging.info("Dropping existing local DB")
            db.drop_all()
        logging.info("Updating\\Creating local DB schema")
        db.create_all()

    stop_event = threading.Event()
    scheduler = APScheduler(app=app)

    thread_manage_jobs = Thread(target=thread_manage_jobs,
                                args=(scheduler, stop_event))
    thread_manage_jobs.start()

    app.run(host="0.0.0.0", port=app.config['SERVER_PORT'])

    stop_event.set()
    thread_manage_jobs.join()

