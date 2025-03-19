"""
Initialization module for the tests package.

This module marks the `tests` directory as a package, allowing for organized
test discovery and execution. It ensures that test modules within subdirectories
can be imported and executed properly.

Structure
---------
The `tests` package is structured as follows:

- `dags/`: Contains tests for DAGs in Airflow.
- `include/etl/`: Includes ETL-related tests, such as transformers and loaders.
- `include/extractors/`: Tests for data extraction components.
- `include/utils/`: Utility function tests, covering AWS, Azure, and file tools.
"""
