import pandas as pd
import numpy as np
import os, sys, sqlite3
from recommend.exception.exception import RecommenderException, error_message_details
from recommend.logging.logging import logging
from recommend.constants.clature import (
    ARTIFACT_DIR, DATA_INGSTION_FILE_NAME, BOOKS_DB_NAME, USER_DB_NAME, 
    RATINGS_DB_NAME, DATA_TRANSFORMATION_FILE_NAME, MERGED_MAIN_FILE_NAME, 
    POPULARITY_BASED_FILTERING_DB_NAME, POPULARITY_BASED_FILTERING_FILE_NAME
)

class PopularityBasedDataTransformation:
    def __init__(self):
        self.artifact_dir = ARTIFACT_DIR
        self.transformation_dir = os.path.join(ARTIFACT_DIR, DATA_TRANSFORMATION_FILE_NAME)
        self.popular_filtering = os.path.join(ARTIFACT_DIR, POPULARITY_BASED_FILTERING_FILE_NAME)

        os.makedirs(os.path.dirname(self.transformation_dir), exist_ok=True)
        os.makedirs(os.path.dirname(self.popular_filtering), exist_ok=True)

        self.data_ingestion_path = os.path.join(self.artifact_dir, DATA_INGSTION_FILE_NAME)
        self.book_db_path = os.path.join(self.data_ingestion_path, BOOKS_DB_NAME)
        self.rating_db_path = os.path.join(self.data_ingestion_path, RATINGS_DB_NAME)
        self.user_db_path = os.path.join(self.data_ingestion_path, USER_DB_NAME)

        self.datasets = {
            "books": self.book_db_path,
            "ratings": self.rating_db_path,
            "users": self.user_db_path
        }

    def create_db(self, dataframe, db_path, table_name):
        """Creates an SQLite database and stores the dataframe into it"""
        try:
            with sqlite3.connect(db_path) as conn:
                dataframe.to_sql(name=table_name, con=conn, if_exists='replace', index=False)
                logging.info(f"Table {table_name} successfully stored in {db_path}")
        except Exception as e:
            logging.error(error_message_details(str(e), sys))
            raise RecommenderException(str(e), sys)

    def read_db(self, db_path, table_name):
        """Reads a table from the SQLite database"""
        try:
            with sqlite3.connect(db_path) as conn:
                query = f"SELECT * FROM {table_name}"
                df = pd.read_sql_query(query, conn)
            return df
        except Exception as e:
            logging.error(error_message_details(str(e), sys))
            raise RecommenderException(str(e), sys)

   
    def popularity_filtering(self, books_df, ratings_df):
        """Applies popularity-based filtering to recommend books"""
        try:
            # Ensure numeric columns are properly typed
            ratings_df['Book-Rating'] = pd.to_numeric(ratings_df['Book-Rating'], errors='coerce')
            
            ratings_with_name = pd.merge(ratings_df, books_df, on='ISBN')

            total_ratings_books = ratings_with_name.groupby('Book-Title').count()['Book-Rating'].reset_index()
            total_ratings_books = total_ratings_books.rename(columns={'Book-Rating': 'num_ratings'})

            average_ratings_books = ratings_with_name.groupby('Book-Title')['Book-Rating'].mean().reset_index()
            average_ratings_books = average_ratings_books.rename(columns={'Book-Rating': 'average_ratings'})

            popular_books = pd.merge(total_ratings_books, average_ratings_books, on='Book-Title')

            popular_df = popular_books[popular_books['num_ratings'] >= 400].sort_values('average_ratings', ascending=False)

            details_of_popular_books = pd.merge(popular_df, books_df, on='Book-Title')

            trimmed_details_of_popular_books = details_of_popular_books.drop_duplicates('Book-Title')

            return trimmed_details_of_popular_books

        except Exception as e:
            logging.error(error_message_details(str(e), sys))
            raise RecommenderException(str(e), sys)

    def initiate_popularity_based_filtering(self):
        """Starts the popularity-based filtering process and stores the results"""
        try:
            books = self.read_db(self.book_db_path, "books")
            ratings = self.read_db(self.rating_db_path, "ratings")

            detailed_popular_books = self.popularity_filtering(books, ratings)

            
            popularity_based_filtering_dir = os.path.join(self.artifact_dir, 
                                                        DATA_TRANSFORMATION_FILE_NAME, 
                                                        POPULARITY_BASED_FILTERING_FILE_NAME)
            os.makedirs(popularity_based_filtering_dir, exist_ok=True)

            
            popular_db_path = os.path.join(popularity_based_filtering_dir, 
                                        POPULARITY_BASED_FILTERING_DB_NAME)
            self.create_db(detailed_popular_books, popular_db_path, "popular_books")

            logging.info(f"Popularity-based filtering successfully stored at {popular_db_path}")

        except Exception as e:
            logging.error(error_message_details(str(e), sys))
            raise RecommenderException(str(e), sys)