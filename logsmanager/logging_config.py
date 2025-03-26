import os
import logging
import requests
from logging.config import dictConfig
from watchtower import CloudWatchLogHandler
import json
import boto3

# Load configuration from app.json
CONFIG_FILE = "app.json"
with open(CONFIG_FILE, "r") as f:
    config = json.load(f)
    
# AWS Configuration
AWS_ACCESS_KEY_ID = config.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = config.get("AWS_SECRET_ACCESS_KEY")
AWS_REGION = config.get("AWS_REGION", "us-west-1")
LOG_GROUP = config.get("CLOUDWATCH_LOG_GROUP", "blackprint-backend/logs")
LOG_STREAM = config.get("CLOUDWATCH_LOG_STREAM", "blackprint-backend-application")

def get_cloudwatch_handler():
    try:
        client = boto3.client(
            'logs',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )
        return CloudWatchLogHandler(
            log_group=LOG_GROUP,
            stream_name=LOG_STREAM,  # Fixed parameter name
            boto3_client=client,
            create_log_group=True
        )
    except Exception as e:
        logging.error(f"Failed to initialize CloudWatch logging: {e}")
        return None
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
            "format": "[%(asctime)s] %(levelname)s [%(filename)s:%(module)s:%(funcName)s:%(lineno)d] - %(message)s",
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
    logger = logging.getLogger()
    logging.getLogger(__name__).info("Logging is configured.")
    
    # Add CloudWatch handler to root logger
    cloudwatch_handler = get_cloudwatch_handler()
    if cloudwatch_handler:
        logger.addHandler(cloudwatch_handler)
        logger.info("CloudWatch logging configured successfully")
        # Ensure the handler uses the same formatter
        cloudwatch_handler.setFormatter(logging.Formatter(
            "[%(asctime)s] %(levelname)s [%(filename)s:%(module)s:%(funcName)s:%(lineno)d] - %(message)s",
            "%Y-%m-%d %H:%M:%S"
        ))

# Example usage
if __name__ == "__main__":
    setup_logging()
    logger = logging.getLogger(__name__)
    logger.error("This is a test error message!")  # This should be sent to Slack
