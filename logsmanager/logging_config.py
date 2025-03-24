import os
import logging
import requests
from logging.config import dictConfig

# Replace with your Slack webhook URL
SLACK_WEBHOOK_URL = "https://hooks.slack.com/services/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M"

class SlackLogHandler(logging.Handler):
    def __init__(self, webhook_url):
        super().__init__()
        self.webhook_url = webhook_url

    def emit(self, record):
        log_entry = self.format(record)
        payload = {"text": f":rotating_light: *ERROR LOG* :rotating_light:\n```{log_entry}```"}
        try:
            requests.post(self.webhook_url, json=payload)
        except Exception as e:
            print(f"Failed to send log to Slack: {e}")

LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "verbose": {
            "format": "[%(asctime)s] %(levelname)s [%(module)s:%(funcName)s:%(lineno)d] - %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S",
        },
    },
    "handlers": {
        "console": {
            "level": os.environ.get("LOG_LEVEL", "DEBUG"),
            "class": "logging.StreamHandler",
            "formatter": "verbose",
        },
        "file": {
            "level": "ERROR",
            "class": "logging.FileHandler",
            "filename": "app.log",
            "formatter": "verbose",
        },
        "slack": {
            "level": "ERROR",
            "()": SlackLogHandler,  # Corrected handler class reference
            "webhook_url": SLACK_WEBHOOK_URL,
            "formatter": "verbose",
        },
    },
    "loggers": {
        "": {  # Root logger
            "handlers": ["console", "file", "slack"],  # Add "slack" handler
            "level": "DEBUG",
        },
    },
}

def setup_logging():
    """Apply logging configuration"""
    dictConfig(LOGGING_CONFIG)
    logging.getLogger(__name__).info("Logging is configured.")
    

# Example usage
if __name__ == "__main__":
    setup_logging()
    logger = logging.getLogger(__name__)
    logger.error("This is a test error message!")  # This should be sent to Slack
