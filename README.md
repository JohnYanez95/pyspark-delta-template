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

## Getting Started

### Using This Template

This repository is designed as a template for your own PySpark Delta Lake projects. Here are the recommended approaches:

#### Option 1: Clone and Customize (Recommended)
```bash
# Clone the repository
git clone https://github.com/your-username/pyspark-delta-template.git
cd pyspark-delta-template

# Remove git history to start fresh
rm -rf .git

# Initialize your own git repository
git init
git add .
git commit -m "Initial commit from PySpark Delta Lake template"

# Add your own remote origin
git remote add origin https://github.com/your-username/your-project-name.git
git push -u origin main
```

#### Option 2: Fork and Modify
```bash
# Fork this repository on GitHub, then clone your fork
git clone https://github.com/your-username/pyspark-delta-template.git
cd pyspark-delta-template

# Rename the project structure to match your use case
mv src/pyspark_delta_template src/your_project_name

# Update imports and references throughout the codebase
# Update main.py to import from your new module name
```

#### Option 3: Copy Individual Components
If you only need specific components, you can copy them individually:

```bash
# Copy just the configuration management
cp -r src/pyspark_delta_template/config/ your_project/
cp configs/dev/config.yaml your_project/configs/

# Copy just the data I/O classes
cp -r src/pyspark_delta_template/data/ your_project/

# Copy just the processor framework
cp -r src/pyspark_delta_template/processors/ your_project/
```

### Customization Checklist

After cloning, customize the template for your project:

- [ ] **Rename the package**: Change `pyspark_delta_template` to your project name
- [ ] **Update configurations**: Edit `configs/dev/config.yaml` and `configs/prod/config.yaml` with your data sources
- [ ] **Modify table names**: Update Unity Catalog references (`catalog.database.table`) to match your environment
- [ ] **Create custom processors**: Extend `BaseProcessor` for your specific transformations
- [ ] **Update requirements**: Add any additional dependencies your project needs
- [ ] **Modify main.py**: Adjust the orchestrator to use your custom processors
- [ ] **Update documentation**: Replace this README with your project-specific documentation

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

## Databricks Integration

### Setting Up Databricks CLI

The Databricks CLI allows you to interact with Databricks from your local machine or CI/CD pipelines.

#### Installation
```bash
# Install via pip
pip install databricks-cli

# Or via conda
conda install -c conda-forge databricks-cli
```

#### Authentication
```bash
# Configure authentication (choose one method)

# Method 1: Personal Access Token (Recommended)
databricks configure --token

# Method 2: OAuth (for interactive use)
databricks configure --oauth

# Method 3: Service Principal (for automation)
databricks configure --service-principal
```

When prompted, enter:
- **Databricks Host**: Your workspace URL (e.g., `https://your-workspace.cloud.databricks.com`)
- **Token**: Your personal access token (generate from User Settings > Developer > Access Tokens)

#### Upload Your Template
```bash
# Upload the entire project to Databricks workspace
databricks workspace import-dir . /Users/your-email@company.com/pyspark-delta-template

# Or upload specific files
databricks workspace import main.py /Users/your-email@company.com/pyspark-delta-template/main.py
```

### Setting Up Databricks with VS Code

The Databricks VS Code extension provides seamless integration for development and deployment.

#### Installation
1. Install the **Databricks** extension from the VS Code marketplace
2. Or install via command line:
   ```bash
   code --install-extension databricks.databricks
   ```

#### Configuration
1. **Open Command Palette** (`Ctrl+Shift+P` / `Cmd+Shift+P`)
2. Run **"Databricks: Configure Databricks"**
3. Choose authentication method:
   - **Personal Access Token** (recommended for development)
   - **OAuth** (for interactive workflows)
   - **Service Principal** (for automated deployments)

#### Authentication Setup
```bash
# Create .databrickscfg file in your home directory
# Windows: C:\Users\YourName\.databrickscfg
# Mac/Linux: ~/.databrickscfg

[DEFAULT]
host = https://your-workspace.cloud.databricks.com
token = your-personal-access-token

# For multiple environments
[dev]
host = https://dev-workspace.cloud.databricks.com
token = dev-token

[prod]
host = https://prod-workspace.cloud.databricks.com
token = prod-token
```

#### VS Code Workflow
1. **Connect to Databricks**:
   - Open Command Palette
   - Run **"Databricks: Connect"**
   - Select your workspace

2. **Upload and Sync Files**:
   - Right-click on files/folders
   - Select **"Databricks: Upload"**
   - Or use **"Databricks: Sync"** for automatic synchronization

3. **Run Code**:
   - Open a Python file
   - Use **"Databricks: Run File"** to execute on cluster
   - Or create a notebook and run cells interactively

#### Project Structure for Databricks
```
your-project/
├── databricks/
│   ├── notebooks/
│   │   └── main.py              # Main pipeline notebook
│   ├── jobs/
│   │   └── job_config.json      # Job configuration
│   └── clusters/
│       └── cluster_config.json  # Cluster configuration
├── src/
│   └── your_project/            # Your Python modules
├── configs/
│   ├── dev/
│   └── prod/
└── requirements.txt
```

### Deploying to Databricks

#### Method 1: Using Databricks CLI
```bash
# Create a job using the CLI
databricks jobs create --json-file databricks/jobs/job_config.json

# Upload files to DBFS
databricks fs cp configs/prod/config.yaml dbfs:/FileStore/configs/prod/config.yaml
```

#### Method 2: Using VS Code Extension
1. Right-click on your project folder
2. Select **"Databricks: Upload to Workspace"**
3. Choose destination path in workspace
4. Files will be synced automatically

#### Method 3: Using Databricks Repos (Git Integration)
```bash
# Connect your GitHub repo to Databricks
# In Databricks workspace:
# 1. Go to Repos
# 2. Click "Add Repo"
# 3. Enter your GitHub repository URL
# 4. Clone and sync automatically
```

### Running the Template on Databricks

#### Option 1: As a Notebook
1. Upload `main.py` to your Databricks workspace
2. Convert to notebook format
3. Run cells interactively or schedule as a job

#### Option 2: As a Job
Create a job configuration file:
```json
{
  "name": "PySpark Delta Lake Pipeline",
  "new_cluster": {
    "spark_version": "15.4.x-scala2.12",
    "node_type_id": "i3.xlarge",
    "num_workers": 2,
    "runtime_engine": "PHOTON"
  },
  "python_wheel_task": {
    "python_file": "main.py",
    "parameters": ["--config", "configs/prod/config.yaml", "--write-mode", "merge"]
  },
  "libraries": [
    {"pypi": {"package": "PyYAML>=6.0.1"}}
  ]
}
```

#### Option 3: As a Wheel Package
```bash
# Create a wheel package
python setup.py bdist_wheel

# Upload to Databricks
databricks fs cp dist/your_package-1.0.0-py3-none-any.whl dbfs:/FileStore/wheels/

# Install on cluster
databricks libraries install --cluster-id YOUR_CLUSTER_ID --whl dbfs:/FileStore/wheels/your_package-1.0.0-py3-none-any.whl
```

### Best Practices for Databricks Development

1. **Use Databricks Repos** for version control and collaboration
2. **Parameterize your jobs** using Databricks widgets or command-line arguments
3. **Use secrets** for sensitive configuration (database passwords, API keys)
4. **Monitor job runs** using Databricks job monitoring and alerts
5. **Optimize cluster configuration** for your workload size and performance requirements
6. **Use Unity Catalog** for centralized data governance and access control

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
