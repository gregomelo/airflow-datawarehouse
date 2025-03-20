"""
DAG for extracting and processing CoinGecko coin list data.

This DAG follows a **medallion architecture** to extract, transform, and load
cryptocurrency coin list data from the CoinGecko API into **Azure Blob Storage**.

Tasks:
    - **Create** a temporary folder for data extraction.
    - **Extract** data from the CoinGecko API.
    - **List** extracted files.
    - **Load** extracted files into the **bronze** layer (Azure Blob Storage).
    - **Transform** bronze data into **silver** layer.
    - **Create and load** silver and gold layer files.
    - **Remove** the temporary folder after processing.

Dependencies:
    - `include.extractors.coingecko.CoinGeckoCoinsListExtractor`
    - `include.transformers.coingecko.CoinGeckoCoinsListBronze`
    - `include.loaders.coingecko.CoinGeckoBaseCoinsListSilverLoader`
    - `include.loaders.coingecko.CoinGeckoBaseCoinsListGoldLoader`
    - `include.utils.azure_tools.AzureBlobClient`
    - `include.utils.file_tools`
    - `include.utils.log_tools`
"""

import os
from tempfile import TemporaryDirectory
from typing import Dict, List

from airflow.decorators import dag, task
from airflow.models import Variable

from include.extractors.coingecko.coins_list import CoinGeckoCoinsListExtractor
from include.loaders.coingecko.coins_list import (
    CoinGeckoCoinsListGoldLoader,
    CoinGeckoCoinsListSilverLoader,
)
from include.transformers.coingecko.coins_list import CoinGeckoCoinsListBronze
from include.utils.azure_tools import AzureBlobClient
from include.utils.file_tools import (
    create_temp_folder,
    delete_temp_folder,
    list_temp_folder,
    storage_path,
)
from include.utils.log_tools import logger


@dag(dag_display_name="CoinGecko - Coins List")
def coingecko_coins_list():
    """
    Extract, transform, and load CoinGecko coin list data into Azure Blob Storage.

    This DAG dynamically loads the required extractor, transformer, and loader
    classes to ensure scalability for multiple data sources.

    More information about this data, see at
    'https://docs.coingecko.com/reference/coins-list'.
    """
    data_extractor: CoinGeckoCoinsListExtractor = CoinGeckoCoinsListExtractor()
    source_name: str = data_extractor.source_name
    source_surname: str = data_extractor.source_surname

    tmp_dirs: Dict = {}

    os.environ["AZURE_STORAGE_CONNECTION_STRING"] = Variable.get(
        "AZURE_STORAGE_CONNECTION_STRING"
    )

    storage_container = "airflow-datawarehouse"
    storage_client = AzureBlobClient(storage_container)

    @task()
    def create_extraction_temp_folder() -> str:
        """Create a temporary folder for storing extracted data."""
        temp_folder_for_extraction: TemporaryDirectory = create_temp_folder(
            temp_name=source_surname
        )
        tmp_dirs[temp_folder_for_extraction.name] = temp_folder_for_extraction
        return temp_folder_for_extraction.name

    @task()
    def extract_data(ti) -> None:
        """Extract data from the CoinGecko API and saves it to a temporary folder."""
        load_to_folder = ti.xcom_pull(task_ids="create_extraction_temp_folder")

        params_to_query = {"include_platform": "true"}
        data_extractor.start(params_query=params_to_query, load_to=load_to_folder)

    @task()
    def list_extract_files(ti) -> List[str]:
        """List extracted files in the temporary folder."""
        load_to_folder = ti.xcom_pull(task_ids="create_extraction_temp_folder")

        files = list_temp_folder(load_to_folder)
        logger.info(files)
        return files

    @task()
    def load_extract_files_to_bronze(ti):
        """Load extracted files into storage."""
        load_to_folder = ti.xcom_pull(task_ids="create_extraction_temp_folder")
        list_files = ti.xcom_pull(task_ids="list_extract_files")

        list_file_path: List[str] = [f"{load_to_folder}/{file}" for file in list_files]

        load_to_storage: str = storage_path("bronze", source_name, source_surname)

        storage_client.upload_files(
            upload_list_file_path=list_file_path, load_folder=load_to_storage
        )

    @task()
    def transform_bronze_to_silver(ti):
        """Transform bronze data into silver."""
        load_to_folder = ti.xcom_pull(task_ids="create_extraction_temp_folder")

        data_transformer_bronze_to_silver = CoinGeckoCoinsListBronze()
        data_transformer_bronze_to_silver.start(load_to_folder)

    @task()
    def create_silver_layer_file(ti):
        """Create silver layer files."""
        load_to_folder = ti.xcom_pull(task_ids="create_extraction_temp_folder")

        data_loader_silver = CoinGeckoCoinsListSilverLoader()

        silver_data = data_loader_silver.start(load_to_folder, False, storage_container)

        silver_gargabe_data = data_loader_silver.start(
            load_to_folder, True, storage_container
        )

        return [silver_data, silver_gargabe_data]

    @task()
    def load_silver_layer_files(ti):
        """Load silver layer files into storage."""
        silver_layer_files = ti.xcom_pull(task_ids="create_silver_layer_file")

        load_to_storage: str = storage_path("silver", source_name, None)

        storage_client.upload_files(
            upload_list_file_path=silver_layer_files, load_folder=load_to_storage
        )

    @task()
    def create_gold_layer_file(ti):
        """Create gold layer file."""
        load_to_folder = ti.xcom_pull(task_ids="create_extraction_temp_folder")

        data_loader_gold = CoinGeckoCoinsListGoldLoader()

        gold_data = data_loader_gold.start(load_to_folder, storage_container)

        return gold_data

    @task()
    def load_gold_layer_files(ti):
        """Load gold layer files into storage."""
        gold_layer_files = ti.xcom_pull(task_ids="create_gold_layer_file")

        load_to_storage: str = storage_path("gold", source_name, None)

        storage_client.upload_file(
            upload_file_path=gold_layer_files, load_folder=load_to_storage
        )

    @task(trigger_rule="all_done")
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


coingecko_coins_list()
