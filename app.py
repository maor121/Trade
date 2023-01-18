import logging

from app_factory import create_app
from config import RECREATE_DB
from db.models import db

if __name__ == "__main__":
    app = create_app()

    with app.app_context():
        if RECREATE_DB:
            logging.info("Dropping existing local DB")
            db.drop_all()
        logging.info("Updating\\Creating local DB schema")
        db.create_all()

    app.run(host="0.0.0.0", port=app.config['SERVER_PORT'])
