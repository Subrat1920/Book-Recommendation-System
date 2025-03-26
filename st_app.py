# import streamlit as st
# import sqlite3
# import pandas as pd
# import os
# from recommend.utils.utils import BookRecommendationPredictor

# # Constants
# ARTIFACT_DIR = "Artifacts"
# POPULAR_BOOKS_DB = os.path.join(ARTIFACT_DIR, "data_transformation", "popularity_based_filtering", "details_of_popular_books.db")
# RATING_MATRIX_DB = os.path.join(ARTIFACT_DIR, "data_transformation", "collaborative_based_filtering", "rating_matrix.db")
# BOOKS_DB = os.path.join(ARTIFACT_DIR, "data_ingestion", "books.db")

# # Initialize recommendation predictor
# predictor = BookRecommendationPredictor()
# predictor.load_data()

# def load_popular_books():
#     conn = sqlite3.connect(POPULAR_BOOKS_DB)
#     query = "SELECT * FROM popular_books ORDER BY average_ratings DESC LIMIT 12"
#     popular_books = pd.read_sql_query(query, conn)
#     conn.close()
#     return popular_books

# def get_book_details(isbn):
#     conn = sqlite3.connect(BOOKS_DB)
#     query = f"SELECT * FROM books WHERE ISBN = '{isbn}'"
#     details = pd.read_sql_query(query, conn)
#     conn.close()
#     return details.iloc[0] if not details.empty else None

# def get_average_rating(isbn):
#     """Get rating from popular books database"""
#     conn = sqlite3.connect(POPULAR_BOOKS_DB)
#     query = f"SELECT average_ratings FROM popular_books WHERE ISBN = '{isbn}'"
#     result = pd.read_sql_query(query, conn)
#     conn.close()
#     return result['average_ratings'].iloc[0] if not result.empty else None

# def check_book_exists(book_title):
#     conn = sqlite3.connect(RATING_MATRIX_DB)
#     # Case-insensitive search with proper column quoting
#     query = f"""
#     SELECT "Book-Title" 
#     FROM rating_matrix 
#     WHERE LOWER("Book-Title") = LOWER('{book_title.strip()}')
#     """
#     result = pd.read_sql_query(query, conn)
#     conn.close()
#     return not result.empty

# def display_popular_grid(popular_books):
#     for i in range(0, 12, 4):
#         cols = st.columns(4)
#         for j in range(4):
#             idx = i + j
#             if idx < len(popular_books):
#                 with cols[j]:
#                     book = popular_books.iloc[idx]
#                     st.image(book['Image-URL-M'], use_container_width=True)
#                     st.markdown(f"**{book['Book-Title']}**")
#                     st.caption(f"by {book['Book-Author']}")
#                     if st.button("Select", key=f"pop_{book['ISBN']}_{i}_{j}"):
#                         st.session_state.selected_book = book['ISBN']
#                         st.rerun()
#             else:
#                 with cols[j]:
#                     st.empty()

# def display_recommendations(recommended_books, parent_isbn):
#     cols = st.columns(5)
#     recommended_books = recommended_books.head(5)
    
#     for idx in range(5):
#         if idx < len(recommended_books):
#             with cols[idx]:
#                 book = recommended_books.iloc[idx]
#                 st.image(book['Image-URL-M'], use_container_width=True)
#                 st.markdown(f"**{book['Book-Title']}**")
#                 st.caption(f"by {book['Book-Author']}")
#                 if st.button("Select", key=f"rec_{parent_isbn}_{book['ISBN']}"):
#                     st.session_state.selected_book = book['ISBN']
#                     st.rerun()
#         else:
#             with cols[idx]:
#                 st.empty()

# def main():
#     st.set_page_config(layout="wide")
    
#     # Persistent search bar
#     search_query = st.sidebar.text_input("Search for books").strip()
    
#     # Initialize session state
#     if 'selected_book' not in st.session_state:
#         st.session_state.selected_book = None

#     # Handle search
#     if search_query:
#         if check_book_exists(search_query):
#             conn = sqlite3.connect(BOOKS_DB)
#             # Case-insensitive search in books database
#             query = f"""
#             SELECT ISBN 
#             FROM books 
#             WHERE LOWER("Book-Title") = LOWER('{search_query}')
#             LIMIT 1
#             """
#             result = pd.read_sql_query(query, conn)
#             conn.close()
            
#             if not result.empty:
#                 st.session_state.selected_book = result.iloc[0]['ISBN']
#                 st.rerun()
#             else:
#                 st.error("Book not found in database")
#         else:
#             st.error("Book not found in our system")

#     # Show selected book details
#     if st.session_state.selected_book:
#         details = get_book_details(st.session_state.selected_book)
#         if details is not None:
#             # Display main book details
#             col1, col2 = st.columns([1, 3])
#             with col1:
#                 st.image(details['Image-URL-L'], width=300)
#             with col2:
#                 st.header(details['Book-Title'])
#                 st.subheader(f"by {details['Book-Author']}")
#                 st.markdown(f"**Year:** {details['Year-Of-Publication']}")
#                 st.markdown(f"**Publisher:** {details['Publisher']}")
                
#                 avg_rating = get_average_rating(st.session_state.selected_book)
#                 if avg_rating:
#                     st.markdown(f"**Average Rating:** {avg_rating:.2f}/10")
            
#             # Get recommendations
#             recommended_books = predictor.recommend(details['Book-Title'], top_n=5)
#             if not recommended_books.empty:
#                 st.subheader("Recommended Books")
#                 display_recommendations(recommended_books, st.session_state.selected_book)
#             else:
#                 st.write("No recommendations available for this book")
#     else:
#         # Show popular books grid
#         st.header("Most Popular Books")
#         popular_books = load_popular_books()
#         display_popular_grid(popular_books)

# if __name__ == "__main__":
#     main()






import streamlit as st
import sqlite3
import pandas as pd
import os
from recommend.utils.utils import BookRecommendationPredictor

# Constants
ARTIFACT_DIR = "Artifacts"
POPULAR_BOOKS_DB = os.path.join(ARTIFACT_DIR, "data_transformation", "popularity_based_filtering", "details_of_popular_books.db")
RATING_MATRIX_DB = os.path.join(ARTIFACT_DIR, "data_transformation", "collaborative_based_filtering", "rating_matrix.db")
BOOKS_DB = os.path.join(ARTIFACT_DIR, "data_ingestion", "books.db")

# Initialize recommendation predictor
predictor = BookRecommendationPredictor()
predictor.load_data()

def load_popular_books():
    conn = sqlite3.connect(POPULAR_BOOKS_DB)
    query = "SELECT * FROM popular_books ORDER BY average_ratings DESC LIMIT 12"
    popular_books = pd.read_sql_query(query, conn)
    conn.close()
    return popular_books

def get_book_details(isbn):
    try:
        conn = sqlite3.connect(BOOKS_DB)
        query = "SELECT * FROM books WHERE ISBN = ?"
        details = pd.read_sql_query(query, conn, params=(isbn,))
        return details.iloc[0] if not details.empty else None
    except Exception as e:
        st.error(f"Error fetching book details: {str(e)}")
        return None
    finally:
        if 'conn' in locals():
            conn.close()

def get_average_rating(isbn):
    try:
        conn = sqlite3.connect(POPULAR_BOOKS_DB)
        query = "SELECT average_ratings FROM popular_books WHERE ISBN = ?"
        result = pd.read_sql_query(query, conn, params=(isbn,))
        return result['average_ratings'].iloc[0] if not result.empty else None
    except Exception as e:
        st.error(f"Error fetching ratings: {str(e)}")
        return None
    finally:
        if 'conn' in locals():
            conn.close()

def check_book_exists(book_title):
    try:
        # First check rating_matrix database
        rating_conn = sqlite3.connect(RATING_MATRIX_DB)
        rating_query = "SELECT \"Book-Title\" FROM rating_matrix WHERE LOWER(\"Book-Title\") = LOWER(?) LIMIT 1"
        rating_result = pd.read_sql_query(rating_query, rating_conn, params=(book_title.strip(),))
        
        if not rating_result.empty:
            # Now check books database
            books_conn = sqlite3.connect(BOOKS_DB)
            books_query = "SELECT ISBN FROM books WHERE LOWER(\"Book-Title\") = LOWER(?) LIMIT 1"
            books_result = pd.read_sql_query(books_query, books_conn, params=(book_title.strip(),))
            
            if not books_result.empty:
                return books_result.iloc[0]['ISBN']
        return None
    except Exception as e:
        st.error(f"Search error: {str(e)}")
        return None
    finally:
        if 'rating_conn' in locals():
            rating_conn.close()
        if 'books_conn' in locals():
            books_conn.close()

def display_popular_grid(popular_books):
    for i in range(0, 12, 4):
        cols = st.columns(4)
        for j in range(4):
            idx = i + j
            if idx < len(popular_books):
                with cols[j]:
                    book = popular_books.iloc[idx]
                    st.image(book['Image-URL-M'], use_container_width=True)
                    st.markdown(f"**{book['Book-Title']}**")
                    st.caption(f"by {book['Book-Author']}")
                    if st.button("Select", key=f"pop_{book['ISBN']}_{i}_{j}"):
                        st.session_state.selected_book = book['ISBN']
                        st.rerun()
            else:
                with cols[j]:
                    st.empty()

def display_recommendations(recommended_books, parent_isbn):
    cols = st.columns(5)
    recommended_books = recommended_books.head(5)
    
    for idx in range(5):
        if idx < len(recommended_books):
            with cols[idx]:
                book = recommended_books.iloc[idx]
                st.image(book['Image-URL-M'], use_container_width=True)
                st.markdown(f"**{book['Book-Title']}**")
                st.caption(f"by {book['Book-Author']}")
                if st.button("Select", key=f"rec_{parent_isbn}_{book['ISBN']}"):
                    st.session_state.selected_book = book['ISBN']
                    st.rerun()
        else:
            with cols[idx]:
                st.empty()

def main():
    st.set_page_config(layout="wide")
    
    # Persistent search bar
    search_query = st.sidebar.text_input("Search for books").strip()
    
    # Initialize session state
    if 'selected_book' not in st.session_state:
        st.session_state.selected_book = None

    # Handle search
    if search_query:
        isbn = check_book_exists(search_query)
        if isbn:
            st.session_state.selected_book = isbn
            st.rerun()
        else:
            st.error("Book not found in our system")
            st.session_state.selected_book = None
            st.rerun()

    # Show selected book details
    if st.session_state.selected_book:
        details = get_book_details(st.session_state.selected_book)
        if details is not None:
            col1, col2 = st.columns([1, 3])
            with col1:
                st.image(details['Image-URL-L'], width=300)
            with col2:
                st.header(details['Book-Title'])
                st.subheader(f"by {details['Book-Author']}")
                st.markdown(f"**Year:** {details['Year-Of-Publication']}")
                st.markdown(f"**Publisher:** {details['Publisher']}")
                
                avg_rating = get_average_rating(st.session_state.selected_book)
                if avg_rating:
                    st.markdown(f"**Average Rating:** {avg_rating:.2f}/10")
            
            recommended_books = predictor.recommend(details['Book-Title'], top_n=5)
            if not recommended_books.empty:
                st.subheader("Recommended Books")
                display_recommendations(recommended_books, st.session_state.selected_book)
            else:
                st.write("No recommendations available for this book")
    else:
        st.header("Most Popular Books")
        popular_books = load_popular_books()
        display_popular_grid(popular_books)

if __name__ == "__main__":
    main()