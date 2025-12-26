"""
ETL pipeline for the Customer Churn ML project.

This module is responsible for:
- Extracting raw customer churn data from an external source
- Applying minimal, schema-safe transformations
- Loading cleaned records into MongoDB for downstream ML pipelines

Designed for batch execution and repeatable ingestion.
"""

import os
import sys
from pathlib import Path
from typing import List, Dict

import certifi
import pandas as pd
import numpy as np
import pymongo
import kagglehub
from dotenv import load_dotenv

from src.churn_ml.utils.logging import logging
from src.churn_ml.utils.exceptions import CustomerChurnException

load_dotenv()


class CustomerChurnETL:
    """
    End-to-end ETL handler for customer churn data ingestion.
    """

    def __init__(self) -> None:
        """
        Initialize ETL configuration from environment variables
        and validate required settings.
        """
        try:
            self.mongodb_url: str = os.getenv("MONGODB_URL")
            self.data_source: str = os.getenv("DATA_SOURCE")
            self.database: str = os.getenv("MONGODB_DATABASE")
            self.collection: str = os.getenv("MONGODB_COLLECTION")
            self.ca_file: str = certifi.where()

            self._validate_env()
            logging.info("ETL initialized successfully")

        except Exception as e:
            logging.error("ETL initialization failed", exc_info=True)
            raise CustomerChurnException(e, sys)

    def _validate_env(self) -> None:
        """
        Validate presence of required environment variables.
        """
        required_vars = {
            "MONGODB_URL": self.mongodb_url,
            "DATA_SOURCE": self.data_source,
            "MONGODB_DATABASE": self.database,
            "MONGODB_COLLECTION": self.collection,
        }

        missing = [key for key, value in required_vars.items() if not value]
        if missing:
            raise ValueError(
                f"Missing required environment variables: {', '.join(missing)}"
            )

    def extract_data(self) -> pd.DataFrame:
        """
        Download and load raw churn data into a DataFrame.
        """
        try:
            logging.info("Starting data extraction")

            dataset_dir = Path(kagglehub.dataset_download(self.data_source))
            csv_files = list(dataset_dir.glob("*.csv"))

            if len(csv_files) != 1:
                raise ValueError("Expected exactly one CSV file in dataset directory")

            df = pd.read_csv(csv_files[0])

            if df.empty:
                raise ValueError("Extracted dataset is empty")

            logging.info("Data extraction completed successfully")
            return df

        except Exception as e:
            logging.error("Data extraction failed", exc_info=True)
            raise CustomerChurnException(e, sys)

    def transform_data(self, df: pd.DataFrame) -> List[Dict]:
        """
        Apply lightweight transformations and convert data
        into MongoDB-ready records.
        """
        try:
            logging.info("Starting data transformation")

            if df.empty:
                raise ValueError("Cannot transform an empty DataFrame")

            # Ensure one record per customer
            df = df.drop_duplicates(subset=["customerID"], keep="first")

            # Normalize binary categorical field
            df["SeniorCitizen"] = df["SeniorCitizen"].map({1: "Yes", 0: "No"})

            # Clean and coerce monetary column
            df["TotalCharges"] = (
                df["TotalCharges"]
                .astype(str)
                .str.strip()
                .replace("", np.nan)
            )

            df["TotalCharges"] = pd.to_numeric(
                df["TotalCharges"], errors="coerce"
            )

            records = df.to_dict(orient="records")

            logging.info(
                "Data transformation completed | records=%d",
                len(records),
            )
            return records

        except Exception as e:
            logging.error("Data transformation failed", exc_info=True)
            raise CustomerChurnException(e, sys)

    def load_data(self, records: List[Dict]) -> int:
        """
        Insert transformed records into MongoDB.
        """
        try:
            if not records:
                logging.warning("Load skipped | reason=no_records")
                return 0

            logging.info("Starting data load to MongoDB")

            client = pymongo.MongoClient(
                self.mongodb_url,
                tlsCAFile=self.ca_file,
                serverSelectionTimeoutMS=5000,
            )

            db = client[self.database]
            collection = db[self.collection]

            # temporary
            collection.delete_many({})

            # Enforce customer-level uniqueness
            collection.create_index("customerID", unique=True)

            result = collection.insert_many(records, ordered=False)
            inserted_count = len(result.inserted_ids)

            client.close()

            logging.info(
                "Data load completed | inserted_records=%d",
                inserted_count,
            )
            return inserted_count

        except Exception as e:
            logging.error("Data load failed", exc_info=True)
            raise CustomerChurnException(e, sys)

    def run_etl(self) -> None:
        """
        Execute the full ETL pipeline.
        """
        try:
            logging.info("ETL pipeline started")

            df = self.extract_data()
            records = self.transform_data(df)
            inserted = self.load_data(records)

            logging.info(
                "ETL pipeline completed successfully | total_inserted=%d",
                inserted,
            )

        except Exception as e:
            logging.critical("ETL pipeline failed", exc_info=True)
            raise CustomerChurnException(e, sys)
