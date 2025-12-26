"""
ETL job entrypoint for the Customer Churn ML project.

This script serves as the executable interface for running the
batch ETL pipeline, making it suitable for:
- Local execution
- Docker containers
- CI/CD jobs
- Workflow schedulers (e.g., cron, Airflow, GitHub Actions)
"""

import sys

from src.churn_ml.etl.customer_churn_etl import CustomerChurnETL
from src.churn_ml.utils.exceptions import CustomerChurnException
from src.churn_ml.utils.logging import logging


def main() -> None:
    """
    Orchestrate execution of the customer churn ETL pipeline.
    """
    try:
        logging.info("ETL job started")

        etl = CustomerChurnETL()
        etl.run_etl()

        logging.info("ETL job finished successfully")

    except Exception as e:
        logging.critical("ETL job failed", exc_info=True)
        raise CustomerChurnException(e, sys)


if __name__ == "__main__":
    main()
