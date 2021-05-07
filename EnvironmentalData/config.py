import logging
import os
import sys

from dotenv import dotenv_values

secrets_file = '../.env.secret'

logger = logging.getLogger(__name__)

if not os.path.exists(secrets_file) or not os.access(secrets_file, os.R_OK):
    logger.error('Could not find or read secrets file "%s" with service credentials -> exiting.', secrets_file)
    sys.exit(16)

config = {
    # load sensitive variables
    **dotenv_values(secrets_file),
    # override loaded values with environment variables
    **os.environ,
}
