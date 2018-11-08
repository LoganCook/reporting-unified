import os
import sys
import logging
from flask import Flask
from flask_sqlalchemy import SQLAlchemy as SA

if 'APP_SETTINGS' not in os.environ:
    sys.exit('Missing APP_SETTINGS environment variable')

LOG_FORMAT = '%(asctime)s %(levelname)s %(module)s %(filename)s %(lineno)d: %(message)s'
SAN_MS_DATE = '%Y-%m-%d %H:%M:%S'
LOG_FORMATTER = logging.Formatter(LOG_FORMAT, SAN_MS_DATE)

app = Flask("app")
app.config.from_envvar('APP_SETTINGS')

# Apply pessimistic database connection checking provided by sqlalchemy, see:
# https://docs.sqlalchemy.org/en/latest/core/pooling.html#pool-disconnects-pessimistic
# https://github.com/mitsuhiko/flask-sqlalchemy/issues/589#issuecomment-361075700
class SQLAlchemy(SA):
    def apply_pool_defaults(self, app, options):
        SA.apply_pool_defaults(self, app, options)
        options["pool_pre_ping"] = True

db = SQLAlchemy(app)
