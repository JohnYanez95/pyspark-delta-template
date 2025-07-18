"""
DataWriter class for writing data to Delta Lake with Unity Catalog support.
Handles primary key merge logic and table properties.
"""

import logging
from typing import Dict, Any, List, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, current_timestamp
from delta.tables import DeltaTable
from ..config.loader import ConfigLoader


logger = logging.getLogger(__name__)


class DataWriter:
    """
    DataWriter for writing data to Delta Lake with Unity Catalog support.
    Handles merge/upsert operations and table properties.
    """
    
    def __init__(self, spark: SparkSession, config: ConfigLoader):
        self.spark = spark
        self.config = config
    
    def write_data(self, df: DataFrame, mode: str = "merge") -> None:
        """
        Write DataFrame to Delta Lake destination.
        
        Args:
            df: DataFrame to write
            mode: Write mode - 'merge', 'overwrite', 'append'
        """
        output_config = self.config.get_output_destination()
        table_name = output_config['table']
        
        logger.info(f"Writing to table: {table_name} with mode: {mode}")
        
        # Add audit columns if configured
        if self.config.get('output_destination.add_audit_columns', False):
            df = self._add_audit_columns(df)
        
        if mode == "merge":
            self._merge_data(df, table_name)
        elif mode == "overwrite":
            self._overwrite_data(df, table_name)
        elif mode == "append":
            self._append_data(df, table_name)
        else:
            raise ValueError(f"Unsupported write mode: {mode}")
        
        # Optimize table if configured
        if self.config.get('output_destination.auto_optimize', False):
            self._optimize_table(table_name)
    
    def _merge_data(self, df: DataFrame, table_name: str) -> None:
        """
        Merge data into existing Delta table using primary keys.
        
        Args:
            df: DataFrame to merge
            table_name: Target table name
        """
        primary_keys = self.config.get_primary_keys()
        
        if not primary_keys:
            raise ValueError("Primary keys required for merge operation")
        
        # Check if table exists
        if not self._table_exists(table_name):
            logger.info(f"Table {table_name} does not exist, creating with initial data")
            self._create_table(df, table_name)
            return
        
        # Build merge condition
        merge_condition = " AND ".join([f"target.{key} = source.{key}" for key in primary_keys])
        
        logger.info(f"Merging data with condition: {merge_condition}")
        
        # Load existing table
        delta_table = DeltaTable.forName(self.spark, table_name)
        
        # Get all columns except primary keys for update
        update_columns = [col for col in df.columns if col not in primary_keys]
        
        # Build update and insert dictionaries
        update_dict = {col: f"source.{col}" for col in update_columns}
        insert_dict = {col: f"source.{col}" for col in df.columns}
        
        # Perform merge
        (delta_table.alias("target")
         .merge(df.alias("source"), merge_condition)
         .whenMatchedUpdate(set=update_dict)
         .whenNotMatchedInsert(values=insert_dict)
         .execute())
        
        logger.info(f"Merge operation completed for table: {table_name}")
    
    def _overwrite_data(self, df: DataFrame, table_name: str) -> None:
        """
        Overwrite data in Delta table.
        
        Args:
            df: DataFrame to write
            table_name: Target table name
        """
        writer = df.write.format("delta").mode("overwrite")
        
        # Add table properties
        table_properties = self.config.get_table_properties()
        if table_properties:
            for key, value in table_properties.items():
                writer = writer.option(f"delta.{key}", value)
        
        # Add partition columns
        partition_columns = self.config.get_partition_columns()
        if partition_columns:
            writer = writer.partitionBy(*partition_columns)
        
        writer.saveAsTable(table_name)
        logger.info(f"Overwrite operation completed for table: {table_name}")
    
    def _append_data(self, df: DataFrame, table_name: str) -> None:
        """
        Append data to Delta table.
        
        Args:
            df: DataFrame to append
            table_name: Target table name
        """
        if not self._table_exists(table_name):
            logger.info(f"Table {table_name} does not exist, creating with initial data")
            self._create_table(df, table_name)
            return
        
        df.write.format("delta").mode("append").saveAsTable(table_name)
        logger.info(f"Append operation completed for table: {table_name}")
    
    def _create_table(self, df: DataFrame, table_name: str) -> None:
        """
        Create new Delta table with proper configuration.
        
        Args:
            df: DataFrame to create table from
            table_name: Target table name
        """
        logger.info(f"Creating new table: {table_name}")
        
        writer = df.write.format("delta").mode("overwrite")
        
        # Add table properties
        table_properties = self.config.get_table_properties()
        if table_properties:
            for key, value in table_properties.items():
                writer = writer.option(f"delta.{key}", value)
        
        # Add partition columns
        partition_columns = self.config.get_partition_columns()
        if partition_columns:
            writer = writer.partitionBy(*partition_columns)
        
        writer.saveAsTable(table_name)
        
        # Set additional table properties using SQL if needed
        additional_properties = self.config.get('output_destination.additional_properties', {})
        if additional_properties:
            for key, value in additional_properties.items():
                self.spark.sql(f"ALTER TABLE {table_name} SET TBLPROPERTIES ('{key}' = '{value}')")
        
        logger.info(f"Table {table_name} created successfully")
    
    def _table_exists(self, table_name: str) -> bool:
        """
        Check if Delta table exists.
        
        Args:
            table_name: Table name to check
            
        Returns:
            True if table exists, False otherwise
        """
        try:
            self.spark.sql(f"DESCRIBE TABLE {table_name}")
            return True
        except Exception:
            return False
    
    def _add_audit_columns(self, df: DataFrame) -> DataFrame:
        """
        Add audit columns to DataFrame.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with audit columns added
        """
        audit_columns = self.config.get('output_destination.audit_columns', {})
        
        if audit_columns.get('add_timestamp', False):
            df = df.withColumn("processed_timestamp", current_timestamp())
        
        if audit_columns.get('add_version', False):
            version = self.config.get('processing_parameters.version', 'unknown')
            df = df.withColumn("processing_version", col("version").cast("string"))
        
        return df
    
    def _optimize_table(self, table_name: str) -> None:
        """
        Optimize Delta table (compact small files, Z-order).
        
        Args:
            table_name: Table name to optimize
        """
        logger.info(f"Optimizing table: {table_name}")
        
        # Basic optimize
        self.spark.sql(f"OPTIMIZE {table_name}")
        
        # Z-order optimization if specified
        zorder_columns = self.config.get('output_destination.zorder_columns', [])
        if zorder_columns:
            zorder_cols = ", ".join(zorder_columns)
            self.spark.sql(f"OPTIMIZE {table_name} ZORDER BY ({zorder_cols})")
            logger.info(f"Z-order optimization completed for columns: {zorder_columns}")
        
        logger.info(f"Table optimization completed for: {table_name}")
    
    def get_table_stats(self, table_name: str) -> Dict[str, Any]:
        """
        Get statistics for a Delta table.
        
        Args:
            table_name: Table name
            
        Returns:
            Dictionary containing table statistics
        """
        try:
            # Get table details
            details_df = self.spark.sql(f"DESCRIBE DETAIL {table_name}")
            details = details_df.collect()[0].asDict()
            
            # Get row count
            count_df = self.spark.sql(f"SELECT COUNT(*) as row_count FROM {table_name}")
            row_count = count_df.collect()[0]['row_count']
            
            return {
                'table_name': table_name,
                'row_count': row_count,
                'size_bytes': details.get('sizeInBytes', 0),
                'num_files': details.get('numFiles', 0),
                'last_modified': details.get('lastModified'),
                'table_version': details.get('version', 0)
            }
        
        except Exception as e:
            logger.warning(f"Could not get table stats for {table_name}: {e}")
            return {'table_name': table_name, 'error': str(e)}
    
    def vacuum_table(self, table_name: str, retention_hours: int = 168) -> None:
        """
        Vacuum Delta table to remove old files.
        
        Args:
            table_name: Table name to vacuum
            retention_hours: Retention period in hours (default: 7 days)
        """
        logger.info(f"Vacuuming table: {table_name} with retention: {retention_hours} hours")
        
        # Set retention check to false for shorter retention periods
        if retention_hours < 168:
            self.spark.sql("SET spark.databricks.delta.retentionDurationCheck.enabled = false")
        
        self.spark.sql(f"VACUUM {table_name} RETAIN {retention_hours} HOURS")
        
        logger.info(f"Vacuum completed for table: {table_name}")