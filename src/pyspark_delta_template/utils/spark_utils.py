"""
Spark utilities for session management and configuration.
"""

import logging
from typing import Dict, Any, Optional
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from ..config.loader import ConfigLoader


logger = logging.getLogger(__name__)


class SparkSessionManager:
    """
    Manages Spark session creation and configuration.
    """
    
    def __init__(self, config: ConfigLoader):
        self.config = config
        self._spark = None
    
    def get_spark_session(self) -> SparkSession:
        """
        Get or create Spark session with proper configuration.
        
        Returns:
            Configured SparkSession
        """
        if self._spark is None:
            self._spark = self._create_spark_session()
        
        return self._spark
    
    def _create_spark_session(self) -> SparkSession:
        """
        Create Spark session with Delta Lake and Unity Catalog support.
        
        Returns:
            Configured SparkSession
        """
        logger.info("Creating Spark session...")
        
        # Get Spark configuration from config
        spark_config = self.config.get_spark_config()
        
        # Build Spark configuration
        conf = SparkConf()
        
        # Default configurations for Delta Lake
        default_configs = {
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true"
        }
        
        # Add Unity Catalog support if configured
        if self.config.get('unity_catalog.enabled', False):
            unity_configs = {
                "spark.sql.catalog.unity": "com.databricks.sql.io.delta.catalog.DeltaCatalog",
                "spark.sql.catalog.unity.type": "unity"
            }
            default_configs.update(unity_configs)
        
        # Merge with user-provided configurations
        all_configs = {**default_configs, **spark_config}
        
        # Apply configurations
        for key, value in all_configs.items():
            conf.set(key, value)
            logger.debug(f"Set Spark config: {key} = {value}")
        
        # Create session
        app_name = self.config.get('spark_config.app_name', 'PySpark Delta Template')
        
        spark = (SparkSession.builder
                .appName(app_name)
                .config(conf=conf)
                .enableHiveSupport()
                .getOrCreate())
        
        # Set additional SQL configurations
        sql_configs = self.config.get('spark_config.sql_configs', {})
        for key, value in sql_configs.items():
            spark.sql(f"SET {key} = {value}")
            logger.debug(f"Set SQL config: {key} = {value}")
        
        logger.info(f"Spark session created successfully: {spark.version}")
        return spark
    
    def configure_for_local_development(self) -> None:
        """
        Apply local development specific configurations.
        """
        if self.config.is_development():
            logger.info("Applying local development configurations...")
            
            # Local development configs
            local_configs = {
                "spark.master": "local[*]",
                "spark.sql.warehouse.dir": "./spark-warehouse",
                "spark.sql.streaming.forceDeleteTempCheckpointLocation": "true"
            }
            
            if self._spark:
                for key, value in local_configs.items():
                    self._spark.conf.set(key, value)
                    logger.debug(f"Set local config: {key} = {value}")
    
    def configure_for_databricks(self) -> None:
        """
        Apply Databricks specific configurations.
        """
        logger.info("Applying Databricks configurations...")
        
        # Databricks configs
        databricks_configs = {
            "spark.databricks.delta.preview.enabled": "true",
            "spark.databricks.delta.optimizeWrite.enabled": "true",
            "spark.databricks.delta.autoCompact.enabled": "true"
        }
        
        if self._spark:
            for key, value in databricks_configs.items():
                self._spark.conf.set(key, value)
                logger.debug(f"Set Databricks config: {key} = {value}")
    
    def stop_spark_session(self) -> None:
        """Stop the Spark session."""
        if self._spark:
            self._spark.stop()
            self._spark = None
            logger.info("Spark session stopped")
    
    def get_session_info(self) -> Dict[str, Any]:
        """
        Get information about the current Spark session.
        
        Returns:
            Dictionary containing session information
        """
        if not self._spark:
            return {"status": "not_created"}
        
        return {
            "status": "active",
            "version": self._spark.version,
            "app_name": self._spark.conf.get("spark.app.name"),
            "master": self._spark.conf.get("spark.master"),
            "executor_memory": self._spark.conf.get("spark.executor.memory", "not_set"),
            "driver_memory": self._spark.conf.get("spark.driver.memory", "not_set"),
            "executor_instances": self._spark.conf.get("spark.executor.instances", "not_set")
        }