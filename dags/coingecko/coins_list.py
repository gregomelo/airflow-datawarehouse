"""
DAG for extracting and processing CoinGecko Coin List data.

This DAG follows a **medallion architecture** to extract, transform, and load
cryptocurrency coin list data from the CoinGecko API into storage
(**Azure Blob Storage**).

Tasks:
    - **Create** a temporary folder for data extraction.
    - **Extract** data from the CoinGecko API.
    - **List** extracted files.
    - **Load** extracted files into the **bronze** layer (Azure Blob Storage).
    - **Transform** bronze data into **silver** layer.
    - **Create and load** silver and gold layer files.
    - **Remove** the temporary folder after processing.

Dynamic Components:
    - Extractor
    - Transformer
    - Silver and Gold Loaders
    - Storage Client

Dependencies:
    - `include.extractors.coingecko.CoinGeckoCoinsListExtractor`
    - `include.transformers.coingecko.CoinGeckoCoinsListBronze`
    - `include.loaders.coingecko.CoinGeckoBaseCoinsListSilverLoader`
    - `include.loaders.coingecko.CoinGeckoBaseCoinsListGoldLoader`
    - `include.utils.azure_tools.AzureBlobClient`
    - `include.utils.file_tools`
    - `include.utils.log_tools`
"""

import importlib
import json
import os
from typing import Dict

from airflow.decorators import dag, task
from airflow.models import Variable

from include.utils.file_tools import (
    create_temp_folder,
    delete_temp_folder,
    list_temp_folder,
    storage_path,
)
from include.utils.log_tools import logger


@dag(dag_display_name="CoinGecko - Coins List")
def coingecko_coinslist():
    """
    Extract, transform, and load CoinGecko coin list data into Azure Blob Storage.

    This DAG dynamically loads the required extractor, transformer, and loader
    classes to ensure scalability for multiple data sources.

    More information about this data, see at
    'https://docs.coingecko.com/reference/coins-list'.
    """
    tmp_dirs: Dict = {}

    @task()
    def setup_objects():
        """Set up the configuration for extractors, transformers, loaders, and storage."""
        source_name: str = "coingecko"
        source_surname: str = "coins_list"
        storage_container: str = "airflow-datawarehouse"
        prefix_etl_class: str = "CoinGeckoCoinsList"
        prefix_storage_class: str = "AzureBlob"

        params: Dict = {
            "source_name": source_name,
            "source_surname": source_surname,
            "storage_container": storage_container,
            "storage_client": (
                f"include.utils.azure_tools." f"{prefix_storage_class}Client"
            ),
            "extractor_class": (
                f"include.extractors."
                f"{source_name}.{source_surname}.{prefix_etl_class}Extractor"
            ),
            "transformer_class": (
                f"include.transformers."
                f"{source_name}.{source_surname}.{prefix_etl_class}Bronze"
            ),
            "silver_loader_class": (
                f"include.loaders."
                f"{source_name}.{source_surname}.{prefix_etl_class}SilverLoader"
            ),
            "gold_loader_class": (
                f"include.loaders."
                f"{source_name}.{source_surname}.{prefix_etl_class}GoldLoader"
            ),
        }

        return json.dumps(params)

    @task()
    def create_extraction_temp_folder(ti) -> str:
        """Create a temporary folder for storing extracted data."""
        params = ti.xcom_pull(task_ids="setup_objects")
        params = json.loads(params)
        source_surname = params["source_surname"]
        temp_folder_for_extraction = create_temp_folder(temp_name=source_surname)
        tmp_dirs[temp_folder_for_extraction.name] = temp_folder_for_extraction
        return temp_folder_for_extraction.name

    @task()
    def extract_data(ti):
        """Extract data using a dynamically loaded extractor class."""
        params = ti.xcom_pull(task_ids="setup_objects")
        params = json.loads(params)
        load_to_folder = ti.xcom_pull(task_ids="create_extraction_temp_folder")

        module_name, class_name = params["extractor_class"].rsplit(".", 1)
        extractor_module = importlib.import_module(module_name)
        extractor_class = getattr(extractor_module, class_name)

        data_extractor = extractor_class()
        params_to_query = {"include_platform": "true"}
        data_extractor.start(params_query=params_to_query, load_to=load_to_folder)

    @task()
    def list_extract_files(ti):
        """List extracted files in the temporary folder."""
        load_to_folder = ti.xcom_pull(task_ids="create_extraction_temp_folder")
        files = list_temp_folder(load_to_folder)
        logger.info(files)
        return files

    @task()
    def load_extract_files_to_bronze(ti):
        """Load extracted files into Storage."""
        params = ti.xcom_pull(task_ids="setup_objects")
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
        storage_client.upload_files(
            upload_list_file_path=list_file_path, load_folder=load_to_storage
        )

    @task()
    def transform_bronze_to_silver(ti):
        """Transform bronze data into silver using a dynamically loaded transformer."""
        params = ti.xcom_pull(task_ids="setup_objects")
        params = json.loads(params)
        load_to_folder = ti.xcom_pull(task_ids="create_extraction_temp_folder")

        module_name, class_name = params["transformer_class"].rsplit(".", 1)
        transformer_module = importlib.import_module(module_name)
        transformer_class = getattr(transformer_module, class_name)

        data_transformer = transformer_class()
        data_transformer.start(load_to_folder)

    @task()
    def create_silver_layer_file(ti):
        """Create silver layer files using a dynamically loaded loader."""
        params = ti.xcom_pull(task_ids="setup_objects")
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

    @task()
    def load_silver_layer_files(ti):
        """Load silver layer files into storage."""
        params = ti.xcom_pull(task_ids="setup_objects")
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
        storage_client.upload_files(
            upload_list_file_path=silver_layer_files, load_folder=load_to_storage
        )

    @task()
    def create_gold_layer_file(ti):
        """Create gold layer file using a dynamically loaded loader."""
        params = ti.xcom_pull(task_ids="setup_objects")
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

    @task()
    def load_gold_layer_files(ti):
        """Load gold layer files into Azure Blob Storage."""
        params = ti.xcom_pull(task_ids="setup_objects")
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
        storage_client.upload_file(
            upload_file_path=gold_layer_files, load_folder=load_to_storage
        )

    @task(trigger_rule="all_done")
    def remove_temp_folder_for_extraction(ti):
        """Remove the temporary folder used for extraction."""
        load_to_folder = ti.xcom_pull(task_ids="create_extraction_temp_folder")
        delete_temp_folder(load_to_folder)

    (
        setup_objects()
        >> create_extraction_temp_folder()
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


coingecko_coinslist()
