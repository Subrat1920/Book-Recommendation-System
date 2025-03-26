import os
import sys
import pandas as pd
import sqlite3
from recommend.exception.exception import RecommenderException, error_message_details
from recommend.logging.logging import logging
from recommend.constants.clature import (
    BOOKS_DB_NAME, RATINGS_DB_NAME, USER_DB_NAME, ARTIFACT_DIR, DATA_INGSTION_FILE_NAME, DATA_VALIDATION_FILE_NAME, REPORT_FILE_NAME
)

class DataValidation:
    def __init__(self):
        """Initialize paths for validation."""
        self.artifact_dir = ARTIFACT_DIR
        self.validation_dir = os.path.join(self.artifact_dir, DATA_VALIDATION_FILE_NAME)
        os.makedirs(self.validation_dir, exist_ok=True)

        self.report_file_path = os.path.join(self.validation_dir, REPORT_FILE_NAME)

        self.data_ingestion_path = os.path.join(self.artifact_dir, DATA_INGSTION_FILE_NAME)
        self.book_db_path = os.path.join(self.data_ingestion_path, BOOKS_DB_NAME)
        self.rating_db_path = os.path.join(self.data_ingestion_path, RATINGS_DB_NAME)
        self.user_db_path = os.path.join(self.data_ingestion_path, USER_DB_NAME)

        self.datasets = {
            "books": ("Notebooks/Datasets/Books.csv", self.book_db_path),
            "ratings": ("Notebooks/Datasets/Ratings.csv", self.rating_db_path),
            "users": ("Notebooks/Datasets/Users.csv", self.user_db_path),
        }

    def get_csv_shape(self, csv_path):
        """Returns the shape of the CSV file."""
        try:
            df = pd.read_csv(csv_path)
            return df.shape
        except Exception as e:
            logging.error(f"Error reading CSV file {csv_path}: {error_message_details(e, sys)}")
            raise RecommenderException(str(e), sys)

    def get_db_shape(self, db_path, table_name):
        """Returns the shape of the table in the SQLite database."""
        try:
            with sqlite3.connect(db_path) as conn:
                query_rows = f"SELECT COUNT(*) AS row_count FROM {table_name}"
                row_result = pd.read_sql(query_rows, conn)
                num_rows = row_result["row_count"][0]

                query_cols = f"PRAGMA table_info({table_name})"
                cols_info = pd.read_sql(query_cols, conn)
                num_cols = len(cols_info)

            return (num_rows, num_cols)
        except Exception as e:
            logging.error(f"Error reading database {db_path}: {error_message_details(e, sys)}")
            raise RecommenderException(str(e), sys)

    def validate_data(self):
        """Compares the shapes of CSV and database tables and writes results to a report."""
        try:
            with open(self.report_file_path, "w") as report:
                report.write("Data Validation Report\n")
                report.write("=" * 50 + "\n\n")

                for table_name, (csv_path, db_path) in self.datasets.items():
                    if os.path.exists(csv_path) and os.path.exists(db_path):
                        csv_shape = self.get_csv_shape(csv_path)
                        db_shape = self.get_db_shape(db_path, table_name)

                        report.write(f"{table_name.upper()}:\n")
                        report.write(f"CSV Shape: {csv_shape}\n")
                        report.write(f"DB Shape: {db_shape}\n")

                        if csv_shape == db_shape:
                            report.write("Validation Successful: Shapes Match\n\n")
                        else:
                            report.write("Warning: Shape Mismatch\n\n")
                    else:
                        report.write(f"{table_name.upper()}: Skipping validation (Missing CSV or DB file)\n\n")

                report.write("Data Validation Completed!\n")

            logging.info(f"Data validation report generated: {self.report_file_path}")

        except Exception as e:
            logging.error(error_message_details(str(e), sys))
            raise RecommenderException(str(e), sys)
