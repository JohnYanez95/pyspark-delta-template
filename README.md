# PySpark Delta Lake Template

A production-ready Python repository template for PySpark `applyInPandas` workflows with Delta Lake integration, optimized for Databricks Runtime 15.4 LTS.

## Features

- **Class-based design** for wrapping `applyInPandas` operations
- **YAML configuration management** with environment-specific overrides
- **Serialization-safe patterns** using broadcast variables
- **Delta Lake integration** with Unity Catalog support
- **Merge/upsert capabilities** for primary key handling
- **Comprehensive logging** and error handling
- **Local and Databricks compatibility**

## Project Structure

```
pyspark-delta-template/
├── src/pyspark_delta_template/
│   ├── config/
│   │   ├── __init__.py
│   │   └── loader.py              # ConfigLoader class
│   ├── data/
│   │   ├── __init__.py
│   │   ├── reader.py              # DataReader class
│   │   └── writer.py              # DataWriter class
│   ├── processors/
│   │   ├── __init__.py
│   │   ├── base.py                # BaseProcessor abstract class
│   │   └── example.py             # Example processor implementation
│   ├── utils/
│   │   ├── __init__.py
│   │   ├── logging_config.py      # Logging utilities
│   │   └── spark_utils.py         # Spark session management
│   └── __init__.py
├── configs/
│   ├── dev/
│   │   └── config.yaml            # Development configuration
│   └── prod/
│       └── config.yaml            # Production configuration
├── tests/                         # Test directory
├── main.py                        # Main orchestrator
├── requirements.txt               # Python dependencies
└── README.md                      # This file
```

## Quick Start

### 1. Installation

For local development:
```bash
pip install -r requirements.txt
```

For Databricks: Upload the repository to your workspace and install only the additional requirements:
```bash
pip install PyYAML>=6.0.1
```

### 2. Configuration

Set your environment:
```bash
export ENVIRONMENT=dev  # or prod
```

Edit the configuration files in `configs/dev/` or `configs/prod/` to match your data sources and destinations.

### 3. Running the Pipeline

#### On Databricks:
```python
# Upload the repository to your Databricks workspace
# Run in a notebook or job
%run ./main.py --config configs/dev/config.yaml --write-mode merge
```

#### Locally:
```bash
python main.py --config configs/dev/config.yaml --write-mode merge
```

#### Command Line Options:
- `--config`: Path to configuration file (optional, uses environment-based config by default)
- `--write-mode`: Write mode for output data (`merge`, `overwrite`, `append`)
- `--dry-run`: Run pipeline without writing output (validation only)

## Configuration

### Input Sources

Define multiple Delta Lake tables as input sources:

```yaml
input_sources:
  - table: "catalog.database.table_name"
    source_name: "transactions"
    columns: ["col1", "col2", "col3"]
    filters:
      - "amount > 0"
      - "date >= '2024-01-01'"
    incremental:
      timestamp_column: "updated_at"
```

### Output Destination

Configure Delta Lake output with Unity Catalog:

```yaml
output_destination:
  table: "catalog.database.output_table"
  storage_path: "s3://bucket/path/"
  primary_keys: ["id"]
  partition_columns: ["date"]
  table_properties:
    "autoOptimize.optimizeWrite": "true"
    "autoOptimize.autoCompact": "true"
```

### Processing Parameters

Define processing logic parameters:

```yaml
processing_parameters:
  aggregation_method: "sum"
  calculation_multiplier: 1.5
  key_columns: ["user_id", "category"]
  numeric_columns: ["amount"]
  partition_columns: ["category"]
```

## Creating Custom Processors

Extend the `BaseProcessor` class to create custom processing logic:

```python
from pyspark_delta_template.processors.base import BaseProcessor

class CustomProcessor(BaseProcessor):
    def _apply_transformation(self, df: DataFrame) -> DataFrame:
        # Your implementation here
        pandas_udf = self._create_pandas_udf()
        return df.groupBy(*self._get_partition_columns()).applyInPandas(
            pandas_udf, self.get_output_schema()
        )
    
    def _transform_batch(self, batch: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
        # Your pandas transformation logic here
        return batch
    
    def get_output_schema(self) -> StructType:
        # Define your output schema
        return StructType([...])
```

## Key Features

### Serialization-Safe Patterns

The template uses broadcast variables to avoid pickle/serialization issues:

```python
# Config is broadcast to all executors
self._broadcast_config = spark.sparkContext.broadcast(processing_params)

# Access in pandas UDF
def pandas_udf(iterator):
    config = broadcast_config.value
    # Use config safely within UDF
```

### Delta Lake Integration

- **Unity Catalog support**: Full `catalog.database.table` naming
- **Merge operations**: Upsert based on primary keys
- **Table properties**: Optimization and compaction settings
- **Partitioning**: Efficient data organization
- **Z-ordering**: Query performance optimization

### Configuration Management

- **Environment-specific configs**: Separate dev/prod configurations
- **Validation**: Required field validation
- **Nested access**: Dot notation for nested values
- **Defaults**: Sensible default values

## Environment Compatibility

### Databricks Runtime 15.4 LTS

This template is optimized for Databricks Runtime 15.4 LTS, which includes:
- PySpark 3.5.0
- Delta Lake 3.0.0
- Python 3.10.12
- Pre-installed data science libraries

### Local Development

For local development, uncomment the PySpark and Delta Lake dependencies in `requirements.txt`:

```txt
pyspark==3.5.0
delta-spark==3.0.0
```

## Best Practices

1. **Use broadcast variables** for configuration access in UDFs
2. **Implement proper error handling** in all transformation methods
3. **Log processing statistics** at each stage
4. **Clean up resources** using the `cleanup()` method
5. **Test with small datasets** before production deployment
6. **Monitor performance** and optimize partition strategies

## Example Usage

```python
# Initialize components
config = ConfigLoader('configs/prod/config.yaml')
spark_manager = SparkSessionManager(config)
spark = spark_manager.get_spark_session()

# Create processor
processor = ExampleProcessor(spark, config)

# Process data
result_df = processor.process(input_df)

# Clean up
processor.cleanup()
spark_manager.stop_spark_session()
```

## Troubleshooting

### Common Issues

1. **Serialization errors**: Ensure all UDF logic uses broadcast variables
2. **Schema mismatches**: Verify output schema matches actual data
3. **Performance issues**: Check partition strategies and cluster configuration
4. **Configuration errors**: Validate YAML syntax and required fields

### Debugging

Enable debug logging:
```yaml
logging:
  level: "DEBUG"
  include_spark_logs: true
```

Use dry-run mode for testing:
```bash
python main.py --dry-run
```

## Contributing

1. Follow the existing code patterns
2. Add proper error handling and logging
3. Update configuration examples
4. Add tests for new functionality
5. Update documentation

## License

This template is provided as-is for educational and development purposes.
