import logging
import os
from datetime import datetime

# Define log directory and filename
LOG_DIR = os.path.join(os.getcwd(), "logs")  # Only create the directory
os.makedirs(LOG_DIR, exist_ok=True)  # Ensure the directory exists

LOG_FILE = f"{datetime.now().strftime('%m_%d_%Y_%H_%M_%S')}.log"
LOG_FILE_PATH = os.path.join(LOG_DIR, LOG_FILE)  # Correct log file path

# Configure logging
logging.basicConfig(
    filename=LOG_FILE_PATH,
    format="[ %(asctime)s ] %(lineno)d %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)

logging.info("✅ Logging setup successfully!")  # Test log message
