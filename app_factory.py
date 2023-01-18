import flask
from flask import Flask

from db.models import db
from config import SELECTED_CONFIG


def init_views(app):
    from flask_views import flask_view

    # Register blueprints
    app.register_blueprint(flask_view)


def create_app():
    app = Flask(__name__)
    app.config.from_object(SELECTED_CONFIG)

    db.init_app(app)

    init_views(app)

    # app.json_encoder = ...

    return app
