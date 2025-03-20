"""
DAG for extracting and processing CoinGecko Coin List data.

Dependencies:
    - `include.extractors.coingecko.CoinGeckoCoinsListExtractor`
    - `include.transformers.coingecko.CoinGeckoCoinsListBronze`
    - `include.loaders.coingecko.CoinGeckoBaseCoinsListSilverLoader`
    - `include.loaders.coingecko.CoinGeckoBaseCoinsListGoldLoader`
    - `include.utils.azure_tools.AzureBlobClient`
    - `include.utils.file_tools`
    - `include.utils.log_tools`
"""

import json
from typing import Dict

from airflow.decorators import dag, task

from include.dag_models.full_load import FullLoadModel


@dag(dag_display_name="CoinGecko - Coins List")
def coingecko_coinslist():
    """
    Extract, transform, and load CoinGecko coin list data into Azure Blob Storage.

    More information about this data, see at
    'https://docs.coingecko.com/reference/coins-list'.
    """

    @task()
    def setup_params():
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

    full_load_etl = FullLoadModel(group_id="my_task_group")

    (setup_params() >> full_load_etl)


coingecko_coinslist()
