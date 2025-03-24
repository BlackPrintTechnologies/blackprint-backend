from app import app
from logsmanager.logging_config import setup_logging
import logging

# Initialize logging
setup_logging()

# Retrieve the logger
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    logger.info("Starting the backend server...")
    app.run()
