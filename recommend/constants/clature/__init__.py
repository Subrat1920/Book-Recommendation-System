import os, sys
import numpy as np
import pandas as pd
from dotenv import load_dotenv

"""
DEFINING THE DATA INGESTION FOR THE BOOK RECOMMENDATION SYSTEM
"""

ARTIFACT_DIR: str = "Artifacts"

"""
DATA INGDSTION
"""
DATA_INGSTION_FILE_NAME: str = 'data_ingestion'
BOOKS_DB_NAME : str = 'books.db'
RATINGS_DB_NAME: str = 'ratings.db'
USER_DB_NAME: str = 'user.db'

"""
DATA VALIDATION
"""
REPORT_FILE_NAME: str = "report.txt"
DATA_VALIDATION_FILE_NAME :str = "data_validation"