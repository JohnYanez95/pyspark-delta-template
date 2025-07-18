"""
Logging configuration utilities for the PySpark Delta template.
"""

import logging
import sys
from typing import Optional


def setup_logging(
    log_level: str = "INFO",
    log_format: Optional[str] = None,
    include_spark_logs: bool = False
) -> None:
    """
    Setup logging configuration for the application.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_format: Custom log format string
        include_spark_logs: Whether to include Spark logs
    """
    if log_format is None:
        log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    
    # Configure root logger
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format=log_format,
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    # Reduce Spark logging noise unless explicitly requested
    if not include_spark_logs:
        logging.getLogger("pyspark").setLevel(logging.WARNING)
        logging.getLogger("py4j").setLevel(logging.WARNING)
    
    # Set our application loggers
    logging.getLogger("pyspark_delta_template").setLevel(getattr(logging, log_level.upper()))


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger with the specified name.
    
    Args:
        name: Logger name
        
    Returns:
        Configured logger
    """
    return logging.getLogger(name)