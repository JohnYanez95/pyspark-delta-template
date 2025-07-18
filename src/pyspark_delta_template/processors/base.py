"""
Base processor class for PySpark applyInPandas workflows.
Handles serialization-safe patterns and broadcast variables.
"""

import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, Iterator, Callable
import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType
from pyspark.broadcast import Broadcast
from ..config.loader import ConfigLoader


logger = logging.getLogger(__name__)


class BaseProcessor(ABC):
    """
    Base class for PySpark applyInPandas processors.
    Provides serialization-safe patterns and config access.
    """
    
    def __init__(self, spark: SparkSession, config: ConfigLoader):
        self.spark = spark
        self.config = config
        self._broadcast_config = None
    
    def process(self, df: DataFrame) -> DataFrame:
        """
        Main processing method that handles broadcast variables and applies transformations.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Processed DataFrame
        """
        # Create broadcast variables for serialization-safe access
        self._create_broadcast_variables()
        
        # Apply preprocessing if needed
        df = self._preprocess(df)
        
        # Apply the main transformation using applyInPandas
        result_df = self._apply_transformation(df)
        
        # Apply postprocessing if needed
        result_df = self._postprocess(result_df)
        
        return result_df
    
    def _create_broadcast_variables(self) -> None:
        """Create broadcast variables for config and other shared data."""
        # Broadcast the processing parameters to avoid serialization issues
        processing_params = self.config.get_processing_parameters()
        self._broadcast_config = self.spark.sparkContext.broadcast(processing_params)
        
        logger.info("Created broadcast variables for processing parameters")
    
    def _preprocess(self, df: DataFrame) -> DataFrame:
        """
        Preprocessing step before main transformation.
        Override in subclasses for specific preprocessing logic.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Preprocessed DataFrame
        """
        return df
    
    def _postprocess(self, df: DataFrame) -> DataFrame:
        """
        Postprocessing step after main transformation.
        Override in subclasses for specific postprocessing logic.
        
        Args:
            df: Transformed DataFrame
            
        Returns:
            Postprocessed DataFrame
        """
        return df
    
    @abstractmethod
    def _apply_transformation(self, df: DataFrame) -> DataFrame:
        """
        Apply the main transformation using applyInPandas.
        Must be implemented by subclasses.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Transformed DataFrame
        """
        pass
    
    @abstractmethod
    def get_output_schema(self) -> StructType:
        """
        Get the output schema for the transformation.
        Must be implemented by subclasses.
        
        Returns:
            Output schema
        """
        pass
    
    def _create_pandas_udf(self) -> Callable[[Iterator[pd.DataFrame]], Iterator[pd.DataFrame]]:
        """
        Create a pandas UDF function that can access broadcast variables.
        This method ensures serialization safety.
        
        Returns:
            Pandas UDF function
        """
        # Capture broadcast variable reference
        broadcast_config = self._broadcast_config
        
        def pandas_udf(iterator: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
            # Access broadcast config safely within the UDF
            config_dict = broadcast_config.value
            
            for batch in iterator:
                # Apply transformation to each batch
                result_batch = self._transform_batch(batch, config_dict)
                yield result_batch
        
        return pandas_udf
    
    @abstractmethod
    def _transform_batch(self, batch: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        """
        Transform a single batch of data.
        Must be implemented by subclasses.
        
        Args:
            batch: Input pandas DataFrame batch
            config: Processing configuration dictionary
            
        Returns:
            Transformed pandas DataFrame
        """
        pass
    
    def _get_partition_columns(self) -> list:
        """
        Get partition columns for groupBy operations.
        Override in subclasses for specific partitioning logic.
        
        Returns:
            List of partition column names
        """
        return self.config.get('processing_parameters.partition_columns', [])
    
    def _log_processing_stats(self, df: DataFrame, stage: str) -> None:
        """
        Log processing statistics.
        
        Args:
            df: DataFrame to analyze
            stage: Processing stage name
        """
        try:
            row_count = df.count()
            column_count = len(df.columns)
            logger.info(f"{stage}: {row_count} rows, {column_count} columns")
        except Exception as e:
            logger.warning(f"Could not log stats for {stage}: {e}")
    
    def cleanup(self) -> None:
        """Clean up broadcast variables and other resources."""
        if self._broadcast_config:
            self._broadcast_config.unpersist()
            self._broadcast_config = None
            logger.info("Cleaned up broadcast variables")