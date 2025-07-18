"""
DataReader class for reading data from Delta Lake sources.
Supports multiple table sources, joins, and data validation.
"""

import logging
from typing import List, Dict, Any, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit
from ..config.loader import ConfigLoader


logger = logging.getLogger(__name__)


class DataReader:
    """
    DataReader for fetching data from multiple Delta Lake sources.
    Supports Unity Catalog format and various read patterns.
    """
    
    def __init__(self, spark: SparkSession, config: ConfigLoader):
        self.spark = spark
        self.config = config
    
    def read_sources(self) -> DataFrame:
        """
        Read data from all configured input sources.
        
        Returns:
            Combined DataFrame from all sources
        """
        sources = self.config.get_input_sources()
        
        if not sources:
            raise ValueError("No input sources configured")
        
        dataframes = []
        for source_config in sources:
            df = self._read_single_source(source_config)
            dataframes.append(df)
        
        if len(dataframes) == 1:
            return dataframes[0]
        
        # Combine multiple sources based on strategy
        combine_strategy = self.config.get('input_sources_strategy', 'union')
        
        if combine_strategy == 'union':
            return self._union_sources(dataframes)
        elif combine_strategy == 'join':
            return self._join_sources(dataframes, sources)
        else:
            raise ValueError(f"Unknown combine strategy: {combine_strategy}")
    
    def _read_single_source(self, source_config: Dict[str, Any]) -> DataFrame:
        """
        Read data from a single Delta Lake source.
        
        Args:
            source_config: Source configuration dictionary
            
        Returns:
            DataFrame from the source
        """
        table_name = source_config['table']
        logger.info(f"Reading from table: {table_name}")
        
        # Read from Delta table
        df = self.spark.read.format("delta").table(table_name)
        
        # Apply filters if specified
        if 'filters' in source_config:
            for filter_expr in source_config['filters']:
                df = df.filter(filter_expr)
                logger.info(f"Applied filter: {filter_expr}")
        
        # Select specific columns if specified
        if 'columns' in source_config:
            df = df.select(*source_config['columns'])
            logger.info(f"Selected columns: {source_config['columns']}")
        
        # Add source identifier column
        if 'source_name' in source_config:
            df = df.withColumn("source", lit(source_config['source_name']))
        
        # Apply incremental read if specified
        if 'incremental' in source_config:
            df = self._apply_incremental_read(df, source_config['incremental'])
        
        return df
    
    def _apply_incremental_read(self, df: DataFrame, incremental_config: Dict[str, Any]) -> DataFrame:
        """
        Apply incremental read logic based on configuration.
        
        Args:
            df: Source DataFrame
            incremental_config: Incremental read configuration
            
        Returns:
            Filtered DataFrame for incremental processing
        """
        if 'timestamp_column' in incremental_config:
            timestamp_col = incremental_config['timestamp_column']
            
            if 'start_time' in incremental_config:
                start_time = incremental_config['start_time']
                df = df.filter(col(timestamp_col) >= start_time)
                logger.info(f"Applied incremental filter: {timestamp_col} >= {start_time}")
            
            if 'end_time' in incremental_config:
                end_time = incremental_config['end_time']
                df = df.filter(col(timestamp_col) < end_time)
                logger.info(f"Applied incremental filter: {timestamp_col} < {end_time}")
        
        return df
    
    def _union_sources(self, dataframes: List[DataFrame]) -> DataFrame:
        """
        Union multiple DataFrames together.
        
        Args:
            dataframes: List of DataFrames to union
            
        Returns:
            Unioned DataFrame
        """
        logger.info(f"Unioning {len(dataframes)} DataFrames")
        
        result = dataframes[0]
        for df in dataframes[1:]:
            result = result.unionByName(df, allowMissingColumns=True)
        
        return result
    
    def _join_sources(self, dataframes: List[DataFrame], sources: List[Dict[str, Any]]) -> DataFrame:
        """
        Join multiple DataFrames based on configuration.
        
        Args:
            dataframes: List of DataFrames to join
            sources: Source configurations containing join information
            
        Returns:
            Joined DataFrame
        """
        if len(dataframes) < 2:
            return dataframes[0]
        
        logger.info(f"Joining {len(dataframes)} DataFrames")
        
        # Start with first DataFrame
        result = dataframes[0]
        
        # Join remaining DataFrames
        for i, df in enumerate(dataframes[1:], 1):
            source_config = sources[i]
            
            if 'join_config' not in source_config:
                raise ValueError(f"join_config required for source {i} when using join strategy")
            
            join_config = source_config['join_config']
            join_keys = join_config.get('keys', [])
            join_type = join_config.get('type', 'inner')
            
            if not join_keys:
                raise ValueError(f"join_keys required for source {i}")
            
            result = result.join(df, join_keys, join_type)
            logger.info(f"Joined source {i} on keys: {join_keys} with type: {join_type}")
        
        return result
    
    def validate_data(self, df: DataFrame) -> DataFrame:
        """
        Perform basic data validation on the DataFrame.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Validated DataFrame
        """
        # Check for empty DataFrame
        if df.count() == 0:
            raise ValueError("DataFrame is empty after reading sources")
        
        # Log basic statistics
        logger.info(f"DataFrame has {df.count()} rows and {len(df.columns)} columns")
        logger.info(f"Columns: {df.columns}")
        
        # Check for required columns if specified
        required_columns = self.config.get('data_validation.required_columns', [])
        missing_columns = set(required_columns) - set(df.columns)
        
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        # Check for null values in key columns if specified
        key_columns = self.config.get('data_validation.no_null_columns', [])
        for col_name in key_columns:
            if col_name in df.columns:
                null_count = df.filter(col(col_name).isNull()).count()
                if null_count > 0:
                    raise ValueError(f"Found {null_count} null values in required column: {col_name}")
        
        return df
    
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """
        Get metadata information about a Delta table.
        
        Args:
            table_name: Name of the Delta table
            
        Returns:
            Dictionary containing table metadata
        """
        try:
            # Get table description
            describe_df = self.spark.sql(f"DESCRIBE TABLE {table_name}")
            
            # Get table history
            history_df = self.spark.sql(f"DESCRIBE HISTORY {table_name}")
            
            return {
                'schema': describe_df.collect(),
                'latest_version': history_df.first()['version'] if history_df.count() > 0 else None,
                'table_name': table_name
            }
        
        except Exception as e:
            logger.warning(f"Could not get table info for {table_name}: {e}")
            return {'table_name': table_name, 'error': str(e)}