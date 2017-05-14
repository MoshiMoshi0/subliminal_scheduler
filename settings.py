"""Settings to override default settings."""

import logging

#
# Override settings
#
DEBUG = False

HTTP_PORT = 8888
HTTP_ADDRESS = '0.0.0.0'

#
# Set logging level
#
logging.getLogger().setLevel(logging.INFO)

JOB_CLASS_PACKAGES = ['subliminal_scheduler.jobs']
