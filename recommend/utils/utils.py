import os, sys
import sqlite3
from recommend.logging.logging import logging
from recommend.exception.exception import RecommenderException, error_message_details
import pandas as pd


def read_csv(file_path):
    try:
        df = pd.read_csv(file_path)
        return df
    except Exception as e:
        logging.info(error_message_details(e, sys))
        raise RecommenderException(e, sys)
    


def create_db(name_db, file_path):
