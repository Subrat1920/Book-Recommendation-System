import os
import sys
import pandas as pd
import sqlite3
from recommend.exception.exception import RecommenderException, error_message_details
from recommend.logging.logging import logging
from recommend.constants.clature import (
    BOOKS_DB_NAME, RATINGS_DB_NAME, USER_DB_NAME, ARTIFACT_DIR, DATA_INGSTION_FILE_NAME
)

class DataIngestion:
    def __init__(self):
        self.artifact_dir = ARTIFACT_DIR
        self.data_ingestion_path = os.path.join(self.artifact_dir, DATA_INGSTION_FILE_NAME)  
        os.makedirs(self.data_ingestion_path, exist_ok=True)  

        
        self.book_db_path = os.path.join(self.data_ingestion_path, BOOKS_DB_NAME)
        self.rating_db_path = os.path.join(self.data_ingestion_path, RATINGS_DB_NAME)
        self.user_db_path = os.path.join(self.data_ingestion_path, USER_DB_NAME)

    def read_csv_files(self, csv_file_path, db_path, table_name):
        try:
            logging.info(f"Loading {csv_file_path} into {db_path} ({table_name})")

            df = pd.read_csv(csv_file_path)

            with sqlite3.connect(db_path) as conn:
                df.to_sql(table_name, conn, if_exists="replace", index=False)

            logging.info(f"Successfully loaded {csv_file_path} into {db_path} ({table_name})")
        except Exception as e:
            logging.error(error_message_details(e, sys))
            raise RecommenderException(e, sys)

    def initiate_data_ingestion(self):
        
        try:
            datasets = {
                "books": ("Notebooks/Datasets/Books.csv", self.book_db_path),
                "ratings": ("Notebooks/Datasets/Ratings.csv", self.rating_db_path),
                "users": ("Notebooks/Datasets/Users.csv", self.user_db_path),
            }
            for table_name, (csv_path, db_path) in datasets.items():
                if not os.path.exists(db_path):
                    self.read_csv_files(csv_path, db_path, table_name)
                else:
                    logging.info(f"{db_path} already exists. Skipping {table_name} table creation.")

            logging.info("Data Ingestion Pipeline Completed Successfully!")
        except Exception as e:
            logging.error(error_message_details(e, sys))
            raise RecommenderException(e, sys)
