"""
ConfigLoader class for loading and validating YAML configurations.
Implements singleton pattern to avoid re-loading and provides typed access to config values.
"""

import os
import yaml
from typing import Any, Dict, Optional, List, Union
from pathlib import Path


class ConfigLoader:
    """
    Singleton ConfigLoader for managing YAML configurations.
    Provides safe access to nested config values and environment-specific overrides.
    """
    
    _instance = None
    _config = None
    
    def __new__(cls, config_path: Optional[str] = None):
        if cls._instance is None:
            cls._instance = super(ConfigLoader, cls).__new__(cls)
        return cls._instance
    
    def __init__(self, config_path: Optional[str] = None):
        if self._config is None:
            self._load_config(config_path)
    
    def _load_config(self, config_path: Optional[str] = None):
        """Load configuration from YAML file with environment-specific overrides."""
        if config_path is None:
            env = os.getenv('ENVIRONMENT', 'dev')
            config_path = f"configs/{env}/config.yaml"
        
        config_file = Path(config_path)
        if not config_file.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        
        with open(config_file, 'r') as f:
            self._config = yaml.safe_load(f)
        
        self._validate_config()
    
    def _validate_config(self):
        """Validate required configuration fields."""
        required_fields = [
            'input_sources',
            'output_destination',
            'processing_parameters'
        ]
        
        for field in required_fields:
            if field not in self._config:
                raise ValueError(f"Required configuration field missing: {field}")
        
        # Validate input sources structure
        if not isinstance(self._config['input_sources'], list):
            raise ValueError("input_sources must be a list")
        
        for source in self._config['input_sources']:
            if 'table' not in source:
                raise ValueError("Each input source must have a 'table' field")
        
        # Validate output destination
        output = self._config['output_destination']
        if 'table' not in output:
            raise ValueError("output_destination must have a 'table' field")
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get configuration value by key, supporting nested keys with dot notation.
        
        Args:
            key: Configuration key (supports dot notation for nested values)
            default: Default value if key not found
            
        Returns:
            Configuration value or default
        """
        keys = key.split('.')
        value = self._config
        
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
        
        return value
    
    def get_input_sources(self) -> List[Dict[str, Any]]:
        """Get list of input source configurations."""
        return self._config.get('input_sources', [])
    
    def get_output_destination(self) -> Dict[str, Any]:
        """Get output destination configuration."""
        return self._config.get('output_destination', {})
    
    def get_processing_parameters(self) -> Dict[str, Any]:
        """Get processing parameters configuration."""
        return self._config.get('processing_parameters', {})
    
    def get_primary_keys(self) -> List[str]:
        """Get primary key columns for table writes."""
        return self.get('output_destination.primary_keys', [])
    
    def get_table_properties(self) -> Dict[str, str]:
        """Get Delta table properties."""
        return self.get('output_destination.table_properties', {})
    
    def get_partition_columns(self) -> List[str]:
        """Get partition columns for Delta table."""
        return self.get('output_destination.partition_columns', [])
    
    def is_development(self) -> bool:
        """Check if running in development environment."""
        return self.get('environment', 'dev') == 'dev'
    
    def get_spark_config(self) -> Dict[str, str]:
        """Get Spark configuration settings."""
        return self.get('spark_config', {})
    
    def reload(self, config_path: Optional[str] = None):
        """Reload configuration from file."""
        self._config = None
        self._load_config(config_path)