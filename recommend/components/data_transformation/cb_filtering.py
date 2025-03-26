# import os, sys, sqlite3
# import pandas as pd
# import numpy as np
# from recommend.exception.exception import RecommenderException, error_message_details
# from recommend.logging.logging import logging

# from recommend.constants.clature import (
#     ARTIFACT_DIR, DATA_INGSTION_FILE_NAME, BOOKS_DB_NAME, USER_DB_NAME, 
#     RATINGS_DB_NAME, DATA_TRANSFORMATION_FILE_NAME, MERGED_MAIN_FILE_NAME, 
#     POPULARITY_BASED_FILTERING_DB_NAME, POPULARITY_BASED_FILTERING_FILE_NAME, 
#     COLLABORATIVE_BASED_FILTERING_FILE_NAME, COLLABORATIVE_BASED_FILTERING_DB_NAME
# )

# class CollaborativeBasedFiltering:
#     def __init__(self):
#         self.artifact_dir = ARTIFACT_DIR
#         self.transformation_dir = os.path.join(ARTIFACT_DIR, DATA_TRANSFORMATION_FILE_NAME)
        
#         # Correct path structure - collaborative filtering under data_transformation
#         self.cf_filtering_dir = os.path.join(
#             self.artifact_dir, 
#             DATA_TRANSFORMATION_FILE_NAME, 
#             COLLABORATIVE_BASED_FILTERING_FILE_NAME
#         )

#         # Create all necessary directories
#         os.makedirs(self.transformation_dir, exist_ok=True)
#         os.makedirs(self.cf_filtering_dir, exist_ok=True)

#         self.data_ingestion_path = os.path.join(self.artifact_dir, DATA_INGSTION_FILE_NAME)
#         os.makedirs(self.data_ingestion_path, exist_ok=True)  # Ensure ingestion directory exists
        
#         self.book_db_path = os.path.join(self.data_ingestion_path, BOOKS_DB_NAME)
#         self.rating_db_path = os.path.join(self.data_ingestion_path, RATINGS_DB_NAME)
#         self.user_db_path = os.path.join(self.data_ingestion_path, USER_DB_NAME)

#         self.datasets = {
#             "books": self.book_db_path,
#             "ratings": self.rating_db_path,
#             "users": self.user_db_path
#         }
    
#     # def create_db(self, dataframe, db_path, table_name):
#     #     """Creates an SQLite database and stores the dataframe into it"""
#     #     try:
#     #         # Ensure directory exists
#     #         os.makedirs(os.path.dirname(db_path), exist_ok=True)
            
#     #         with sqlite3.connect(db_path) as conn:
#     #             dataframe.to_sql(name=table_name, con=conn, if_exists='replace', index=False)
#     #             logging.info(f"Table {table_name} successfully stored in {db_path}")
#     #     except Exception as e:
#     #         logging.error(error_message_details(str(e), sys))
#     #         raise RecommenderException(str(e), sys)
#     def create_db(self, dataframe, db_path, table_name):
#         """Creates an SQLite database and stores the dataframe into it with Book-Title as index"""
#         try:
#             # Ensure directory exists
#             os.makedirs(os.path.dirname(db_path), exist_ok=True)
            
#             # Set 'Book-Title' as the index before saving
#             dataframe.set_index('Book-Title', inplace=True)
            
#             with sqlite3.connect(db_path) as conn:
#                 dataframe.to_sql(name=table_name, con=conn, if_exists='replace', index=True)  # Ensure index=True
#                 logging.info(f"Table {table_name} successfully stored in {db_path} with 'Book-Title' as index")
        
#         except Exception as e:
#             logging.error(error_message_details(str(e), sys))
#             raise RecommenderException(str(e), sys)

    

    
#     def read_db(self, db_path, table_name):
#         """Reads a table from the SQLite database"""
#         try:
#             # Verify database file exists
#             if not os.path.exists(db_path):
#                 raise FileNotFoundError(f"Database file not found at {db_path}")
                
#             with sqlite3.connect(db_path) as conn:
#                 query = f"SELECT * FROM {table_name}"
#                 df = pd.read_sql_query(query, conn)
#             return df
#         except Exception as e:
#             logging.error(error_message_details(str(e), sys))
#             raise RecommenderException(str(e), sys)
        
#     def collaborative_filtering(self, books_df, ratings_df):
#         """Your exact notebook logic with minor robustness improvements"""
#         try:
#             # Your exact logic starts here
#             ratings_df['Book-Rating'] = pd.to_numeric(ratings_df['Book-Rating'], errors='coerce')

#             ratings_with_name = pd.merge(ratings_df, books_df, on="ISBN")
#             logging.info(f"columns in rating_with_name {ratings_with_name.columns}")

#             x = ratings_with_name.groupby('User-ID').count()['Book-Rating'] > 200
#             true_indexes = x[x].index

#             filtered_ratings = ratings_with_name[ratings_with_name['User-ID'].isin(true_indexes)]

#             y = filtered_ratings.groupby('Book-Title').count()['Book-Rating'] >= 50
#             famous_books = y[y].index

#             final_ratings = filtered_ratings[filtered_ratings['Book-Title'].isin(famous_books)]

#             rating_matrix = final_ratings.pivot_table(
#                 index='Book-Title',
#                 columns='User-ID',
#                 values='Book-Rating'
#             )

#             rating_matrix.fillna(0, inplace=True)
#             return rating_matrix

#         except Exception as e:
#             logging.error(error_message_details(str(e), sys))
#             raise RecommenderException(e, sys)
    
#     def initiate_collaborative_filtering(self):
#         try:
#             logging.info("Starting collaborative filtering process...")
            
#             # Read data
#             books = self.read_db(self.book_db_path, "books")
#             ratings = self.read_db(self.rating_db_path, "ratings")

#             # Apply your exact filtering logic
#             rating_matrix = self.collaborative_filtering(books, ratings)

#             # Ensure output directory exists
#             os.makedirs(self.cf_filtering_dir, exist_ok=True)

#             # Define output path
#             collaborative_db_path = os.path.join(
#                 self.cf_filtering_dir, 
#                 COLLABORATIVE_BASED_FILTERING_DB_NAME
#             )
            
#             # Save results
#             self.create_db(rating_matrix, collaborative_db_path, "rating_matrix")
            
#             logging.info(f"Collaborative filtering results saved to {collaborative_db_path}")
#             return True
            
#         except Exception as e:
#             logging.error(error_message_details(str(e), sys))
#             raise RecommenderException(e, sys)

import os
import sys
import sqlite3
import pandas as pd
import numpy as np
from recommend.exception.exception import RecommenderException, error_message_details
from recommend.logging.logging import logging
from recommend.constants.clature import (
    ARTIFACT_DIR, DATA_INGSTION_FILE_NAME, BOOKS_DB_NAME, USER_DB_NAME, 
    RATINGS_DB_NAME, DATA_TRANSFORMATION_FILE_NAME, MERGED_MAIN_FILE_NAME, 
    COLLABORATIVE_BASED_FILTERING_FILE_NAME, COLLABORATIVE_BASED_FILTERING_DB_NAME
)

class CollaborativeBasedFiltering:
    def __init__(self):
        self.artifact_dir = ARTIFACT_DIR
        self.transformation_dir = os.path.join(ARTIFACT_DIR, DATA_TRANSFORMATION_FILE_NAME)
        self.cf_filtering_dir = os.path.join(self.transformation_dir, COLLABORATIVE_BASED_FILTERING_FILE_NAME)
        
        # Ensure directories exist
        os.makedirs(self.cf_filtering_dir, exist_ok=True)

        self.data_ingestion_path = os.path.join(self.artifact_dir, DATA_INGSTION_FILE_NAME)
        os.makedirs(self.data_ingestion_path, exist_ok=True)
        
        self.book_db_path = os.path.join(self.data_ingestion_path, BOOKS_DB_NAME)
        self.rating_db_path = os.path.join(self.data_ingestion_path, RATINGS_DB_NAME)
        self.user_db_path = os.path.join(self.data_ingestion_path, USER_DB_NAME)

    def read_db(self, db_path, table_name):
        """Reads a table from SQLite database."""
        try:
            if not os.path.exists(db_path):
                raise FileNotFoundError(f"Database file not found: {db_path}")
            
            with sqlite3.connect(db_path) as conn:
                df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
            return df
        except Exception as e:
            logging.error(error_message_details(str(e), sys))
            raise RecommenderException(str(e), sys)
    
    def collaborative_filtering(self, books_df, ratings_df):
        """Apply collaborative filtering logic."""
        try:
            # Convert Book-Rating to numeric
            ratings_df['Book-Rating'] = pd.to_numeric(ratings_df['Book-Rating'], errors='coerce')
            
            # Merge datasets
            ratings_with_name = pd.merge(ratings_df, books_df, on="ISBN", how="inner")
            logging.info(f"Columns in ratings_with_name: {ratings_with_name.columns}")
            
            # Filter users with >200 ratings
            user_counts = ratings_with_name.groupby('User-ID')['Book-Rating'].count()
            active_users = user_counts[user_counts > 200].index
            
            filtered_ratings = ratings_with_name[ratings_with_name['User-ID'].isin(active_users)]
            
            # Filter books with >=50 ratings
            book_counts = filtered_ratings.groupby('Book-Title')['Book-Rating'].count()
            popular_books = book_counts[book_counts >= 50].index
            
            final_ratings = filtered_ratings[filtered_ratings['Book-Title'].isin(popular_books)]
            
            # Create user-item matrix
            rating_matrix = final_ratings.pivot_table(index='Book-Title', columns='User-ID', values='Book-Rating')
            rating_matrix.fillna(0, inplace=True)
            return rating_matrix
        except Exception as e:
            logging.error(error_message_details(str(e), sys))
            raise RecommenderException(e, sys)
    
    def initiate_collaborative_filtering(self):
        """Run collaborative filtering and save the matrix."""
        try:
            logging.info("Starting collaborative filtering process...")
            
            books = self.read_db(self.book_db_path, "books")
            ratings = self.read_db(self.rating_db_path, "ratings")
            
            rating_matrix = self.collaborative_filtering(books, ratings)
            
            # Save results
            collaborative_db_path = os.path.join(self.cf_filtering_dir, COLLABORATIVE_BASED_FILTERING_DB_NAME)
            
            with sqlite3.connect(collaborative_db_path) as conn:
                rating_matrix.to_sql(name="rating_matrix", con=conn, if_exists='replace', index=True)
            
            logging.info(f"Collaborative filtering results saved to {collaborative_db_path}")
            return True
        except Exception as e:
            logging.error(error_message_details(str(e), sys))
            raise RecommenderException(e, sys)
