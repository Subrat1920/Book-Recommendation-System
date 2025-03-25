import os, sys
from recommend.logging.logging import logging
from recommend.exception.exception import RecommenderException, error_message_details

from recommend.components.data_ingestion import DataIngestion
from recommend.components.data_validation import DataValidation


if __name__ == "__main__":
    ## data ingestion
    ingestion = DataIngestion()
    ingestion.initiate_data_ingestion()

    validation = DataValidation()
    validation.validate_data()