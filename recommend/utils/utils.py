import os, sys, sqlite3
import pandas as pd
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from recommend.exception.exception import RecommenderException, error_message_details
from recommend.logging.logging import logging

from recommend.constants.clature import COLLABORATIVE_BASED_FILTERING_DB_NAME, COLLABORATIVE_BASED_FILTERING_FILE_NAME, BOOKS_DB_NAME, DATA_INGSTION_FILE_NAME, ARTIFACT_DIR


class Recommend:
    def __init__(self):
        self.collaborative_data_file_name = COLLABORATIVE_BASED_FILTERING_FILE_NAME
        self.collaborative_data_base_name = COLLABORATIVE_BASED_FILTERING_DB_NAME

        self.artifact_dir = ARTIFACT_DIR
        self.data_ingestion_dir = os.path.join(self.artifact_dir, DATA_INGSTION_FILE_NAME)
        self.book_db_name = os.path.join(self.data_ingestion_dir, BOOKS_DB_NAME)

    def read_db(self, db_path, table_name):
        try:
            if not os.path.exists(db_path):
                logging.error(error_message_details(e, sys))
                raise FileExistsError(f"File not found in path --> {db_path}")
            with sqlite3.connect(db_path) as conn:
                query = f"SELECT * FROM {table_name}"
                df = pd.read_sql_query(query, conn)
            return df
        except Exception as e:
            logging.error(error_message_details(e, sys))
            raise RecommenderException(e, sys)
        
    
    def get_similar_df(matrix):
        try:
            return cosine_similarity(matrix)
        except Exception as e:
            logging.error(error_message_details(e, sys))
            raise RecommenderException(e, sys)
    
    def get_recommendation(self, book_name, matrix, similar_matrix):
        try:
            get_index = np.where(matrix.index == book_name)[0][0]
            distance = similar_matrix[get_index]
            
            sim_books = sorted(list(enumerate(similar_matrix[get_index])), key=lambda x: x[1], reverse=True)[1:10]
            top_rec_books: list
            for i in sim_books:
                top_rec_books.append(matrix.index[i[0]])
            return top_rec_books
        except Exception as e:
            logging.error(error_message_details(e, sys))
            raise RecommenderException(e, sys)
    

    
    def get_recommended_books_details(self, full_books, rec_books):
        try:
            detailed_books = full_books[full_books['Book-Title'].isin(rec_books)].drop_duplicates('Book-Title')
            return detailed_books
        except Exception as e:
            logging.error(error_message_details(e, sys))
            raise RecommenderException(e, sys)

    def initiate_recomendation(self):
        try:
            books_df = self.read_db(self.book_db_name, "books")
            
        except Exception as e:
            logging.error(error_message_details(e, sys))
            raise RecommenderException(e, sys)


