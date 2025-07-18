"""
Main orchestrator for PySpark Delta Lake processing pipeline.
Coordinates data reading, processing, and writing operations.
"""

import logging
import sys
import argparse
from pathlib import Path
from typing import Optional

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

from pyspark_delta_template.config.loader import ConfigLoader
from pyspark_delta_template.data.reader import DataReader
from pyspark_delta_template.data.writer import DataWriter
from pyspark_delta_template.processors.example import ExampleProcessor
from pyspark_delta_template.utils.logging_config import setup_logging
from pyspark_delta_template.utils.spark_utils import SparkSessionManager


def main(config_path: Optional[str] = None, write_mode: str = "merge") -> None:
    """
    Main orchestrator function that coordinates the entire pipeline.
    
    Args:
        config_path: Path to configuration file
        write_mode: Write mode for output ('merge', 'overwrite', 'append')
    """
    # Initialize logging
    setup_logging(log_level="INFO")
    logger = logging.getLogger(__name__)
    
    logger.info("Starting PySpark Delta Lake processing pipeline...")
    
    spark_session = None
    processor = None
    
    try:
        # Load configuration
        logger.info("Loading configuration...")
        config = ConfigLoader(config_path)
        
        # Setup logging level from config
        log_level = config.get('logging.level', 'INFO')
        setup_logging(log_level=log_level)
        
        # Create Spark session
        logger.info("Creating Spark session...")
        spark_manager = SparkSessionManager(config)
        spark_session = spark_manager.get_spark_session()
        
        # Configure for environment
        if config.is_development():
            spark_manager.configure_for_local_development()
        else:
            spark_manager.configure_for_databricks()
        
        # Log session info
        session_info = spark_manager.get_session_info()
        logger.info(f"Spark session info: {session_info}")
        
        # Initialize components
        logger.info("Initializing pipeline components...")
        data_reader = DataReader(spark_session, config)
        data_writer = DataWriter(spark_session, config)
        processor = ExampleProcessor(spark_session, config)
        
        # Read input data
        logger.info("Reading input data...")
        input_df = data_reader.read_sources()
        
        # Validate input data
        logger.info("Validating input data...")
        input_df = data_reader.validate_data(input_df)
        
        # Process data
        logger.info("Processing data...")
        processed_df = processor.process(input_df)
        
        # Write output data
        logger.info(f"Writing output data with mode: {write_mode}")
        data_writer.write_data(processed_df, mode=write_mode)
        
        # Get output statistics
        output_config = config.get_output_destination()
        output_table = output_config['table']
        stats = data_writer.get_table_stats(output_table)
        logger.info(f"Output table statistics: {stats}")
        
        logger.info("Pipeline completed successfully!")
        
    except Exception as e:
        logger.error(f"Pipeline failed with error: {str(e)}")
        raise
    
    finally:
        # Clean up resources
        if processor:
            processor.cleanup()
        
        if spark_session:
            spark_manager.stop_spark_session()
        
        logger.info("Pipeline cleanup completed")


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="PySpark Delta Lake Processing Pipeline"
    )
    
    parser.add_argument(
        "--config",
        type=str,
        help="Path to configuration file (default: uses environment-based config)"
    )
    
    parser.add_argument(
        "--write-mode",
        type=str,
        choices=["merge", "overwrite", "append"],
        default="merge",
        help="Write mode for output data (default: merge)"
    )
    
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run pipeline without writing output (validation only)"
    )
    
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_arguments()
    
    if args.dry_run:
        print("Running in dry-run mode (no output will be written)")
        # For dry-run, we could implement a validation-only mode
        # This would read and process data but skip the write step
        
    main(config_path=args.config, write_mode=args.write_mode)