"""
Full Load Task Group for Airflow DAGs.

This module defines the `FullLoadModel` class, a custom TaskGroup in Apache
Airflow. It orchestrates a full extract, transform, and load (ETL) process
for any data source implemented with the standard extractor, transformer,
silver loader, and gold loader.

The ETL follows a **medallion architecture** with the following steps:
    - Extract data using a dynamically loaded extractor.
    - Store raw (bronze) data in a storage.
    - Transform bronze data into a structured (silver) format.
    - Load silver data into a storage.
    - Create and load gold layer files.
    - Cleanup temporary extraction folders.

Tasks within this TaskGroup dynamically load extractors, transformers, and
loaders specified in the previous task. This allows flexible scaling
for multiple data sources.
"""

import importlib
import json
import os
from typing import Dict

from airflow.decorators import task
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup

from include.utils.file_tools import (
    create_temp_folder,
    delete_temp_folder,
    list_temp_folder,
    storage_path,
)
from include.utils.log_tools import logger


class FullLoadModel(TaskGroup):
    """
    TaskGroup for a full extract, transform, and load (ETL) process.

    This TaskGroup is designed for scalability and defines a custom `TaskGroup`
    in Apache Airflow that automates the ETL workflow for a data source.
    It dynamically loads extractors, transformers, and loaders based on the
    parameters set in the preceding task (`setup_params`).

    The ETL process follows a **medallion architecture**:
        - **Bronze**: Extract raw data and store it in a storage layer.
        - **Silver**: Transform raw data into a structured format.
        - **Gold**: Load processed data for analytical use.

    This TaskGroup is designed for **scalability**, allowing the same ETL structure
    to be used for multiple data sources without modifying DAG definitions.

    Parameters
    ----------
    group_id : str
        Identifier for the TaskGroup in the DAG.
    prefix_group_id : bool, optional
        Whether to prefix task IDs with the `group_id` (default is True).
    **kwargs : dict
        Additional keyword arguments for `TaskGroup`.

    Example
    -------
    To use `FullLoadModel` in a DAG:

    >>> import json
    >>> from typing import Dict
    >>>
    >>> from airflow.decorators import dag, task
    >>>
    >>> from include.dag_models.full_load import FullLoadModel
    >>>
    >>> @dag(dag_display_name="Example DAG")
    >>> def example_dag():
    >>>
    >>>     @task()
    >>>     def setup_params():
    >>>         params = {
    >>>             "source_name": "example",
    >>>             "source_surname": "dataset",
    >>>             "storage_container": "example-container",
    >>>             "storage_client": "include.utils.storage.ExampleStorageClient",
    >>>             "extractor_class": "include.extractors.example.ExampleExtractor",
    >>>             "params_to_query": {"include_platform": "true"},
    >>>             "transformer_class": "include.transformers.example.ExampleTransformer",
    >>>             "silver_loader_class": "include.loaders.example.ExampleSilverLoader",
    >>>             "gold_loader_class": "include.loaders.example.ExampleGoldLoader",
    >>>         }
    >>>         return json.dumps(params)
    >>>
    >>>     full_load_etl = FullLoadModel(group_id="full_load_etl")
    >>>
    >>>     setup_params() >> full_load_etl
    >>>
    >>> example_dag()

    Notes
    -----
    - The `setup_params` task must be executed before using this TaskGroup.
    - Extractors, transformers, and loaders must follow the standard
      implementation pattern for this TaskGroup to function correctly.
    - The params_to_query will be delivered to the extractor class. This
      dictionary must be passed even if empty.
    """

    def __init__(self, group_id, **kwargs):
        """Instantiate a FullLoadModel."""
        super().__init__(group_id=group_id, prefix_group_id=False, **kwargs)

        self.tmp_dirs: Dict = {}

        @task(task_group=self)
        def create_extraction_temp_folder(ti) -> str:
            """Create a temporary folder for storing extracted data."""
            params = ti.xcom_pull(task_ids="setup_params")
            params = json.loads(params)
            source_surname = params["source_surname"]
            temp_folder_for_extraction = create_temp_folder(source_surname)
            self.tmp_dirs[temp_folder_for_extraction.name] = temp_folder_for_extraction
            return temp_folder_for_extraction.name

        @task(task_group=self)
        def extract_data(ti):
            """Extract data using a dynamically loaded extractor class."""
            params = ti.xcom_pull(task_ids="setup_params")
            params = json.loads(params)
            load_to_folder = ti.xcom_pull(task_ids="create_extraction_temp_folder")

            module_name, class_name = params["extractor_class"].rsplit(".", 1)
            extractor_module = importlib.import_module(module_name)
            extractor_class = getattr(extractor_module, class_name)

            data_extractor = extractor_class()
            params_to_query = params["params_to_query"]
            data_extractor.start(params_to_query, load_to_folder)

        @task(task_group=self)
        def list_extract_files(ti):
            """List extracted files in the temporary folder."""
            load_to_folder = ti.xcom_pull(task_ids="create_extraction_temp_folder")

            files = list_temp_folder(load_to_folder)
            logger.info(files)
            return files

        @task(task_group=self)
        def load_extract_files_to_bronze(ti):
            """Load extracted files into Storage."""
            params = ti.xcom_pull(task_ids="setup_params")
            params = json.loads(params)
            load_to_folder = ti.xcom_pull(task_ids="create_extraction_temp_folder")
            list_files = ti.xcom_pull(task_ids="list_extract_files")

            list_file_path = [f"{load_to_folder}/{file}" for file in list_files]
            load_to_storage = storage_path(
                "bronze", params["source_name"], params["source_surname"]
            )

            module_name, class_name = params["storage_client"].rsplit(".", 1)
            extractor_module = importlib.import_module(module_name)
            storage_class = getattr(extractor_module, class_name)

            os.environ["AZURE_STORAGE_CONNECTION_STRING"] = Variable.get(
                "AZURE_STORAGE_CONNECTION_STRING"
            )

            storage_client = storage_class(params["storage_container"])
            storage_client.upload_files(list_file_path, load_to_storage)

        @task(task_group=self)
        def transform_bronze_to_silver(ti):
            """Transform bronze data into silver using a dynamically loaded transformer."""
            params = ti.xcom_pull(task_ids="setup_params")
            params = json.loads(params)
            load_to_folder = ti.xcom_pull(task_ids="create_extraction_temp_folder")

            module_name, class_name = params["transformer_class"].rsplit(".", 1)
            transformer_module = importlib.import_module(module_name)
            transformer_class = getattr(transformer_module, class_name)

            data_transformer = transformer_class()
            data_transformer.start(load_to_folder)

        @task(task_group=self)
        def create_silver_layer_file(ti):
            """Create silver layer files using a dynamically loaded loader."""
            params = ti.xcom_pull(task_ids="setup_params")
            params = json.loads(params)
            load_to_folder = ti.xcom_pull(task_ids="create_extraction_temp_folder")

            module_name, class_name = params["silver_loader_class"].rsplit(".", 1)
            loader_module = importlib.import_module(module_name)
            loader_class = getattr(loader_module, class_name)

            os.environ["AZURE_STORAGE_CONNECTION_STRING"] = Variable.get(
                "AZURE_STORAGE_CONNECTION_STRING"
            )

            data_loader = loader_class()
            silver_data = data_loader.start(
                load_to_folder, False, params["storage_container"]
            )
            silver_garbage_data = data_loader.start(
                load_to_folder, True, params["storage_container"]
            )

            return [silver_data, silver_garbage_data]

        @task(task_group=self)
        def load_silver_layer_files(ti):
            """Load silver layer files into storage."""
            params = ti.xcom_pull(task_ids="setup_params")
            params = json.loads(params)
            silver_layer_files = ti.xcom_pull(task_ids="create_silver_layer_file")

            load_to_storage = storage_path("silver", params["source_name"], None)

            module_name, class_name = params["storage_client"].rsplit(".", 1)
            extractor_module = importlib.import_module(module_name)
            storage_class = getattr(extractor_module, class_name)

            os.environ["AZURE_STORAGE_CONNECTION_STRING"] = Variable.get(
                "AZURE_STORAGE_CONNECTION_STRING"
            )

            storage_client = storage_class(params["storage_container"])
            storage_client.upload_files(silver_layer_files, load_to_storage)

        @task(task_group=self)
        def create_gold_layer_file(ti):
            """Create gold layer file using a dynamically loaded loader."""
            params = ti.xcom_pull(task_ids="setup_params")
            params = json.loads(params)
            load_to_folder = ti.xcom_pull(task_ids="create_extraction_temp_folder")

            module_name, class_name = params["gold_loader_class"].rsplit(".", 1)
            loader_module = importlib.import_module(module_name)
            loader_class = getattr(loader_module, class_name)

            os.environ["AZURE_STORAGE_CONNECTION_STRING"] = Variable.get(
                "AZURE_STORAGE_CONNECTION_STRING"
            )

            data_loader = loader_class()
            gold_data = data_loader.start(load_to_folder, params["storage_container"])

            return gold_data

        @task(task_group=self)
        def load_gold_layer_files(ti):
            """Load gold layer files into Azure Blob Storage."""
            params = ti.xcom_pull(task_ids="setup_params")
            params = json.loads(params)
            gold_layer_files = ti.xcom_pull(task_ids="create_gold_layer_file")

            load_to_storage = storage_path("gold", params["source_name"], None)

            module_name, class_name = params["storage_client"].rsplit(".", 1)
            extractor_module = importlib.import_module(module_name)
            storage_class = getattr(extractor_module, class_name)

            os.environ["AZURE_STORAGE_CONNECTION_STRING"] = Variable.get(
                "AZURE_STORAGE_CONNECTION_STRING"
            )

            storage_client = storage_class(params["storage_container"])
            storage_client.upload_file(gold_layer_files, load_to_storage)

        @task(task_group=self, trigger_rule="all_done")
        def remove_temp_folder_for_extraction(ti):
            """Remove the temporary folder used for extraction."""
            load_to_folder = ti.xcom_pull(task_ids="create_extraction_temp_folder")
            delete_temp_folder(load_to_folder)

        (
            create_extraction_temp_folder()
            >> extract_data()
            >> list_extract_files()
            >> load_extract_files_to_bronze()
            >> transform_bronze_to_silver()
            >> create_silver_layer_file()
            >> load_silver_layer_files()
            >> create_gold_layer_file()
            >> load_gold_layer_files()
            >> remove_temp_folder_for_extraction()
        )
