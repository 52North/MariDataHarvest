import os

BASE_URL = os.getenv("BASE_URL", "http://localhost:8080/")

# https://docs.pylonsproject.org/projects/waitress/en/stable/arguments.html#arguments
URL_PREFIX = os.getenv("URL_PREFIX", "")

# 50 Mb limit
MAX_CONTENT_LENGTH = 50 * 1024 * 1024
