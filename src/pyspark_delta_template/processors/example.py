"""
Example processor implementation showing applyInPandas patterns.
Demonstrates aggregation and calculation transformations.
"""

import logging
from typing import Iterator, Dict, Any
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from pyspark.sql.functions import col, current_timestamp
from .base import BaseProcessor


logger = logging.getLogger(__name__)


class ExampleProcessor(BaseProcessor):
    """
    Example processor that demonstrates applyInPandas patterns.
    Performs aggregations and calculations on grouped data.
    """
    
    def _apply_transformation(self, df: DataFrame) -> DataFrame:
        """
        Apply transformation using applyInPandas with proper grouping.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Transformed DataFrame
        """
        # Get partition columns from config
        partition_columns = self._get_partition_columns()
        
        if not partition_columns:
            # If no partition columns specified, use a default grouping
            partition_columns = ['default_partition']
            df = df.withColumn('default_partition', lit('all'))
        
        # Log processing stats
        self._log_processing_stats(df, "Before transformation")
        
        # Create pandas UDF
        pandas_udf = self._create_pandas_udf()
        
        # Apply transformation using applyInPandas
        result_df = (df
                    .groupBy(*partition_columns)
                    .applyInPandas(pandas_udf, self.get_output_schema()))
        
        # Remove default partition column if it was added
        if 'default_partition' in result_df.columns and partition_columns == ['default_partition']:
            result_df = result_df.drop('default_partition')
        
        # Log processing stats
        self._log_processing_stats(result_df, "After transformation")
        
        return result_df
    
    def _transform_batch(self, batch: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        """
        Transform a single batch of data with aggregations and calculations.
        
        Args:
            batch: Input pandas DataFrame batch
            config: Processing configuration dictionary
            
        Returns:
            Transformed pandas DataFrame
        """
        if batch.empty:
            return pd.DataFrame(columns=self._get_output_columns())
        
        # Get processing parameters from config
        aggregation_method = config.get('aggregation_method', 'mean')
        calculation_multiplier = config.get('calculation_multiplier', 1.0)
        
        # Example aggregation: group by a key column and aggregate numeric columns
        key_columns = config.get('key_columns', [])
        numeric_columns = config.get('numeric_columns', [])
        
        if not key_columns:
            # If no key columns, treat entire batch as one group
            key_columns = ['batch_key']
            batch['batch_key'] = 'all'
        
        # Ensure key columns exist in batch
        existing_key_columns = [col for col in key_columns if col in batch.columns]
        if not existing_key_columns:
            existing_key_columns = ['batch_key']
            batch['batch_key'] = 'all'
        
        # Ensure numeric columns exist in batch
        if not numeric_columns:
            numeric_columns = batch.select_dtypes(include=[float, int]).columns.tolist()
        
        existing_numeric_columns = [col for col in numeric_columns if col in batch.columns]
        
        if not existing_numeric_columns:
            logger.warning("No numeric columns found for aggregation")
            return self._create_empty_result(batch, existing_key_columns)
        
        # Perform aggregation
        if aggregation_method == 'mean':
            agg_data = batch.groupby(existing_key_columns)[existing_numeric_columns].mean().reset_index()
        elif aggregation_method == 'sum':
            agg_data = batch.groupby(existing_key_columns)[existing_numeric_columns].sum().reset_index()
        elif aggregation_method == 'count':
            agg_data = batch.groupby(existing_key_columns)[existing_numeric_columns].count().reset_index()
        else:
            logger.warning(f"Unknown aggregation method: {aggregation_method}, using mean")
            agg_data = batch.groupby(existing_key_columns)[existing_numeric_columns].mean().reset_index()
        
        # Apply calculations
        for col in existing_numeric_columns:
            if col in agg_data.columns:
                agg_data[f'{col}_calculated'] = agg_data[col] * calculation_multiplier
        
        # Add metadata columns
        agg_data['record_count'] = batch.groupby(existing_key_columns).size().values
        agg_data['processing_timestamp'] = pd.Timestamp.now()
        
        # Add batch statistics
        agg_data['batch_min'] = batch[existing_numeric_columns].min().min()
        agg_data['batch_max'] = batch[existing_numeric_columns].max().max()
        
        return agg_data
    
    def _create_empty_result(self, batch: pd.DataFrame, key_columns: list) -> pd.DataFrame:
        """
        Create an empty result DataFrame with proper schema.
        
        Args:
            batch: Input batch (for reference)
            key_columns: Key columns to preserve
            
        Returns:
            Empty DataFrame with correct schema
        """
        result_columns = self._get_output_columns()
        empty_result = pd.DataFrame(columns=result_columns)
        
        # Add at least one row to maintain schema
        if key_columns:
            empty_row = {col: None for col in result_columns}
            for key_col in key_columns:
                if key_col in batch.columns and not batch.empty:
                    empty_row[key_col] = batch[key_col].iloc[0]
            
            empty_result = pd.DataFrame([empty_row])
        
        return empty_result
    
    def _get_output_columns(self) -> list:
        """Get list of output column names."""
        return [
            'batch_key',
            'record_count',
            'processing_timestamp',
            'batch_min',
            'batch_max'
        ]
    
    def get_output_schema(self) -> StructType:
        """
        Get the output schema for the transformation.
        
        Returns:
            Output schema
        """
        return StructType([
            StructField("batch_key", StringType(), True),
            StructField("record_count", IntegerType(), True),
            StructField("processing_timestamp", TimestampType(), True),
            StructField("batch_min", DoubleType(), True),
            StructField("batch_max", DoubleType(), True)
        ])
    
    def _preprocess(self, df: DataFrame) -> DataFrame:
        """
        Preprocessing step - add any required columns or filters.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Preprocessed DataFrame
        """
        # Add processing timestamp
        df = df.withColumn("input_timestamp", current_timestamp())
        
        # Apply any filters from config
        filters = self.config.get('processing_parameters.filters', [])
        for filter_expr in filters:
            df = df.filter(filter_expr)
            logger.info(f"Applied preprocessing filter: {filter_expr}")
        
        return df
    
    def _postprocess(self, df: DataFrame) -> DataFrame:
        """
        Postprocessing step - add final calculations or cleanup.
        
        Args:
            df: Transformed DataFrame
            
        Returns:
            Postprocessed DataFrame
        """
        # Add final processing timestamp
        df = df.withColumn("output_timestamp", current_timestamp())
        
        # Add any business logic flags
        business_rules = self.config.get('processing_parameters.business_rules', {})
        
        if business_rules.get('add_quality_flag', False):
            df = df.withColumn("quality_flag", 
                              col("record_count") > business_rules.get('min_record_count', 0))
        
        return df