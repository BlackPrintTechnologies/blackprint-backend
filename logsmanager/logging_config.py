import os
import logging
import requests
from logging.config import dictConfig
from watchtower import CloudWatchLogHandler
import json
import boto3
from flask import g, request
from flask_restful import Resource, Api
import uuid

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
SLACK_WEBHOOK_URL = "https://hooks.slack.com/services/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"

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
            
from flask import has_request_context

class RequestIdFilter(logging.Filter):
    def filter(self, record):
        if has_request_context():  # Check if we are inside a Flask request context
            record.request_id = request.request_id if hasattr(request, 'request_id') else str(uuid.uuid4())
        else:
            record.request_id = str(uuid.uuid4())  # Generate a new request ID if no request context
        return True


LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "filters": {
        "request_id": {
            "()": RequestIdFilter
        }
    },
    "formatters": {
        "verbose": {
            "format": "[%(asctime)s] [%(request_id)s] %(levelname)s [%(filename)s:%(module)s:%(funcName)s:%(lineno)d] - %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S",
        },
    },
    "handlers": {
        "console": {
            "level": os.environ.get("LOG_LEVEL", "DEBUG"),
            "class": "logging.StreamHandler",
            "formatter": "verbose",
            "filters": ["request_id"]
        },
        "file": {
            "level": "ERROR",
            "class": "logging.FileHandler",
            "filename": "app.log",
            "formatter": "verbose",
            "filters": ["request_id"]
        },
        "slack": {
            "level": "ERROR",
            "()": SlackLogHandler,
            "webhook_url": SLACK_WEBHOOK_URL,
            "formatter": "verbose",
            "filters": ["request_id"]
        },
    },
    "loggers": {
        "": {
            "handlers": ["console", "file", "slack"],
            "level": "DEBUG",
        },
    },
}

def setup_logging():
    """Apply logging configuration"""
    dictConfig(LOGGING_CONFIG)
    logger = logging.getLogger()
    
    # Add request ID filter to root logger
    logger.addFilter(RequestIdFilter())
    
    # logging.getLogger(__name__).info("Logging is configured.")
    
    # Add CloudWatch handler to root logger
    cloudwatch_handler = get_cloudwatch_handler()
    if cloudwatch_handler:
        cloudwatch_handler.addFilter(RequestIdFilter())
        cloudwatch_handler.setFormatter(logging.Formatter(
            "[%(asctime)s] [%(request_id)s] %(levelname)s [%(filename)s:%(module)s:%(funcName)s:%(lineno)d] - %(message)s",
            "%Y-%m-%d %H:%M:%S"
        ))
        logger.addHandler(cloudwatch_handler)
        logger.info("CloudWatch logging configured successfully")

# Example usage
if __name__ == "__main__":
    setup_logging()
    logger = logging.getLogger(__name__)
    logger.error("This is a test error message!")  # This should be sent to Slack
