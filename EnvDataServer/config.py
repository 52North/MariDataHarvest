import os

BASE_URL = os.getenv("BASE_URL", "http://localhost:8080/")

# 50 Mb limit
MAX_CONTENT_LENGTH = 50 * 1024 * 1024
