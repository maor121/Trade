from flask import Blueprint, render_template

import version

flask_view = Blueprint('rpi_web', __name__, template_folder='templates')


@flask_view.route("/")
def index():
    return render_template("index.html", app_version=version.__version__)