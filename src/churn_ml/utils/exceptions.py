"""
Custom exception definitions for the Customer Churn ML project.

This module provides a project-specific exception class that enriches
errors with file name and line number information, improving debuggability
across pipelines, services, and APIs.
"""

import sys
from src.churn_ml.utils.logging import logging


class CustomerChurnException(Exception):
    """
    Custom exception class that captures contextual debugging information.

    Attributes:
        error_message (Exception): Original exception message.
        file_name (str | None): Source file where the exception occurred.
        lineno (int | None): Line number of the exception.
    """

    def __init__(self, error_message: Exception, error_details: sys):
        """
        Initialize the custom exception with traceback context.

        Args:
            error_message (Exception): The original raised exception.
            error_details (sys): sys module used to extract traceback details.
        """
        super().__init__(error_message)
        self.error_message = error_message

        _, _, exc_tb = error_details.exc_info()

        if exc_tb is not None:
            self.lineno = exc_tb.tb_lineno
            self.file_name = exc_tb.tb_frame.f_code.co_filename
        else:
            self.lineno = None
            self.file_name = None

    def __str__(self) -> str:
        """
        Return a human-readable error message with source context.
        """
        return (
            f"Error occurred in python script "
            f"[{self.file_name}] at line number "
            f"[{self.lineno}] with error message "
            f"[{self.error_message}]"
        )


if __name__ == "__main__":
    try:
        logging.info("Entered the try block")
        a = 1 / 0
        print("This will not be printed", a)

    except Exception as e:
        logging.error("Exception occurred", exc_info=True)
        raise CustomerChurnException(e, sys)
