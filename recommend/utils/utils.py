import os
import sys
import sqlite3
import pandas as pd
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from recommend.exception.exception import RecommenderException, error_message_details
from recommend.logging.logging import logging
from recommend.constants.clature import (
    ARTIFACT_DIR, 
    DATA_TRANSFORMATION_FILE_NAME,
    COLLABORATIVE_BASED_FILTERING_FILE_NAME,
    COLLABORATIVE_BASED_FILTERING_DB_NAME,
    BOOKS_DB_NAME,
    DATA_INGSTION_FILE_NAME
)

class BookRecommendationPredictor:
    def __init__(self):
        """Initialize predictor with paths to artifacts"""
        self.artifact_dir = ARTIFACT_DIR
        
        # Set up paths to required databases
        self.collaborative_db_path = os.path.join(
            self.artifact_dir,
            DATA_TRANSFORMATION_FILE_NAME,
            COLLABORATIVE_BASED_FILTERING_FILE_NAME,
            COLLABORATIVE_BASED_FILTERING_DB_NAME
        )
        
        self.books_db_path = os.path.join(
            self.artifact_dir,
            DATA_INGSTION_FILE_NAME,
            BOOKS_DB_NAME
        )
        
        # Initialize data holders
        self.rating_matrix = None
        self.similarity_matrix = None
        self.books_df = None
        self.book_indices = None

    def load_data(self):
        """Load required data from artifact databases"""
        try:
            # Load rating matrix
            self.rating_matrix = self.read_db(self.collaborative_db_path, "rating_matrix")
            if 'Book-Title' not in self.rating_matrix.columns:
                raise ValueError("Rating matrix missing 'Book-Title' column")
            self.rating_matrix.set_index('Book-Title', inplace=True)
            
            # Load books data
            self.books_df = self.read_db(self.books_db_path, "books")
            
            # Precompute similarity matrix
            self.similarity_matrix = cosine_similarity(self.rating_matrix.fillna(0))
            
            # Create book index mapping for faster lookups
            self.book_indices = {title: idx for idx, title in enumerate(self.rating_matrix.index)}
            
            logging.info("Data loaded successfully")
            
        except Exception as e:
            error_msg = f"Data loading failed: {str(e)}"
            logging.error(error_message_details(error_msg, sys))
            raise RecommenderException(error_msg, sys)

    def read_db(self, db_path, table_name):
        """Read data from SQLite database"""
        try:
            if not os.path.exists(db_path):
                raise FileNotFoundError(f"Database file not found at {db_path}")
            
            with sqlite3.connect(db_path) as conn:
                query = f"SELECT * FROM {table_name}"
                df = pd.read_sql_query(query, conn)
                
                if df.empty:
                    logging.warning(f"Table {table_name} in {db_path} is empty")
                
                return df
                
        except Exception as e:
            error_msg = f"Failed to read from {db_path}: {str(e)}"
            logging.error(error_message_details(error_msg, sys))
            raise RecommenderException(error_msg, sys)


    def recommend(self, book_name, top_n=10):
        try:
            # Ensure data is loaded
            if self.rating_matrix is None:
                self.load_data()
            
            # Find book index (case-sensitive exact match)
            book_idx = self.book_indices.get(book_name)
            if book_idx is None:
                available_books = list(self.rating_matrix.index)[:10]
                logging.warning(f"Book '{book_name}' not found. First 10 available books: {available_books}")
                return pd.DataFrame()
            
            # Compute similarity scores and sort in descending order
            distances = self.similarity_matrix[book_idx]
            sim_books = sorted(enumerate(distances), key=lambda x: x[1], reverse=True)
            
            # Extract recommended book titles (excluding the input book itself)
            top_rec_books = [self.rating_matrix.index[i[0]] for i in sim_books if i[0] != book_idx][:top_n]
            
            # Fetch book details
            detailed_books = self.books_df[
                self.books_df['Book-Title'].isin(top_rec_books)
            ][['ISBN', 'Book-Title', 'Book-Author', 'Year-Of-Publication', 'Publisher', 'Image-URL-S', 'Image-URL-M', 'Image-URL-L']] \
            .drop_duplicates(subset=['Book-Title'])

            return detailed_books if not detailed_books.empty else pd.DataFrame()
        
        except Exception as e:
            error_msg = f"Recommendation failed for {book_name}: {str(e)}"
            logging.error(error_message_details(error_msg, sys))
            raise RecommenderException(error_msg, sys)



# Example usage
if __name__ == "__main__":
    try:
        # Initialize predictor
        predictor = BookRecommendationPredictor()
        
        # Get recommendations for a book
        book_title = "Exclusive"
        recommended_books = predictor.recommend(book_title)
        
        # Print results
        # print(recommended_books)
        print(f'Columns of the recommended books {recommended_books.columns.tolist()}')
        print(recommended_books['Book-Title'].tolist())
        
    except Exception as e:
        logging.error(error_message_details(str(e), sys))
        print(f"Error: {str(e)}")

