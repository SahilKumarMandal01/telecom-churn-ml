"""
Centralized logging configuration for the project.

This module configures application-wide logging with:
- Timestamped log files for traceability
- A dedicated logs directory
- A consistent log format suitable for debugging and monitoring

Intended to be imported once at application startup.
"""

import logging
import os
from datetime import datetime

# Generate a unique log file name using the current timestamp
LOG_FILE_NAME = datetime.now().strftime("%m_%d_%Y_%H_%M_%S") + ".log"

# Define the directory where log files will be stored
LOGS_DIR = os.path.join(os.getcwd(), "logs")

# Ensure the logs directory exists (idempotent operation)
os.makedirs(LOGS_DIR, exist_ok=True)

# Construct the absolute path to the log file
LOG_FILE_PATH = os.path.join(LOGS_DIR, LOG_FILE_NAME)

# Configure the root logger
logging.basicConfig(
    filename=LOG_FILE_PATH,
    level=logging.INFO,
    format="[%(asctime)s] %(lineno)d %(name)s - %(levelname)s - %(message)s",
)
