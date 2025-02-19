"""
DAG for extracting CoinGecko coin list and storing it in Azure Blob Storage.

This DAG extracts a list of cryptocurrency coins from the CoinGecko API,
stores the data in a temporary folder, and uploads the extracted files
to an Azure Blob Storage container.

Tasks:
    - Create a temporary folder for extraction
    - Extract data from the CoinGecko API
    - List extracted files
    - Load extracted files into Azure Blob Storage
    - Remove the temporary extraction folder

Dependencies:
    - `include.extractors.api.CoinGecko.CoinGeckoCoinsList`
    - `include.utils.azure_tools.AzureBlobClient`
    - `include.utils.file_tools`
    - `include.utils.log_tools`

"""

from airflow.decorators import dag, task

from include.extractors.api.CoinGecko import CoinGeckoCoinsList
from include.utils.azure_tools import AzureBlobClient
from include.utils.file_tools import (
    create_temp_folder,
    delete_temp_folder,
    list_temp_folder,
)
from include.utils.log_tools import logger


@dag()
def coingecko_coins_list():
    """Extract CoinGecko coin list and stores it in Azure Blob Storage."""
    extractor_api = CoinGeckoCoinsList()
    source_name = extractor_api.source_name
    source_surname = extractor_api.source_surname

    tmp_dirs = {}

    @task()
    def create_temp_folder_for_extraction():
        """Create a temporary folder for storing extracted data.

        Returns
        -------
        str
            Path to the created temporary folder.
        """
        temp_folder_for_extraction = create_temp_folder(temp_name="coin_list")
        tmp_dirs[temp_folder_for_extraction.name] = temp_folder_for_extraction
        return temp_folder_for_extraction.name

    @task()
    def extract_data_from_api(**kwargs):
        """Extract data from the CoinGecko API and saves it to a temporary folder.

        Parameters
        ----------
        **kwargs : dict
            Keyword arguments, including XCom pull for retrieving folder path.
        """
        ti = kwargs["ti"]
        load_to_folder = ti.xcom_pull(task_ids="create_temp_folder_for_extraction")

        params_to_query = {"include_platform": "true"}
        extractor_api.start(params_query=params_to_query, load_to=load_to_folder)

    @task()
    def list_extract_files(**kwargs):
        """List extracted files in the temporary folder.

        Parameters
        ----------
        **kwargs : dict
            Keyword arguments, including XCom pull for retrieving folder path.

        Returns
        -------
        list of str
            List of extracted file names.
        """
        ti = kwargs["ti"]
        load_to_folder = ti.xcom_pull(task_ids="create_temp_folder_for_extraction")

        files = list_temp_folder(load_to_folder)
        logger.info(files)
        return files

    @task()
    def load_extract_files(**kwargs):
        """Load extracted files into Azure Blob Storage.

        Parameters
        ----------
        **kwargs : dict
            Keyword arguments, including XCom pull for retrieving file list and folder.
        """
        ti = kwargs["ti"]
        load_to_folder = ti.xcom_pull(task_ids="create_temp_folder_for_extraction")
        list_files = ti.xcom_pull(task_ids="list_extract_files")

        storage_container = "airflow-datawarehouse"
        storage_client = AzureBlobClient(storage_container)

        for file in list_files:
            local_file_path = f"{load_to_folder}/{file}"
            logger.info(f"Local path: {local_file_path}")

            load_to_storage = f"Bronze/{source_name}/{source_surname}"
            logger.info(f"Storage path: {load_to_storage}")

            storage_client.upload_file(
                upload_file_path=local_file_path, load_folder=load_to_storage
            )
            logger.info(f"Uploaded {load_to_storage}/{file}")

    @task(trigger_rule="all_done")
    def remove_temp_folder_for_extraction(**kwargs):
        """Remove the temporary folder used for extraction.

        Parameters
        ----------
        **kwargs : dict
            Keyword arguments, including XCom pull for retrieving folder path.
        """
        ti = kwargs["ti"]
        load_to_folder = ti.xcom_pull(task_ids="create_temp_folder_for_extraction")
        delete_temp_folder(load_to_folder)

    t1 = create_temp_folder_for_extraction()
    t2 = extract_data_from_api()
    t3 = list_extract_files()
    t4 = load_extract_files()
    t5 = remove_temp_folder_for_extraction()

    t1 >> t2 >> t3 >> t4 >> t5


coingecko_coins_list()
