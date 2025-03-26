import os, sys
from recommend.logging.logging import logging
from recommend.exception.exception import RecommenderException, error_message_details

from recommend.components.data_ingestion import DataIngestion
from recommend.components.data_validation import DataValidation
from recommend.components.data_transformation.popularity_based import PopularityBasedDataTransformation
from recommend.components.data_transformation.cb_filtering import CollaborativeBasedFiltering


if __name__ == "__main__":
    try:
        ## data ingestion
        ingestion = DataIngestion()
        ingestion.initiate_data_ingestion()
        ## data validation
        validation = DataValidation()
        validation.validate_data()
        ## popularity based filtering
        pop_transformation = PopularityBasedDataTransformation()
        pop_transformation.initiate_popularity_based_filtering()
        ## Collaborative Filtering
        collaborative_filtering = CollaborativeBasedFiltering()
        collaborative_filtering.initiate_collaborative_filtering()

    except Exception as e:
        logging.error(error_message_details(str(e)))
        raise RecommenderException(str(e), sys)
