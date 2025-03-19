"""
Base module for data loading.

This module defines the `LoaderSilverBase` abstract class, which provides a framework
for ingesting, validating, and loading transformed data.

Subclasses must implement data extraction logic to customize the transformation
and loading process.
"""

import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Union

import duckdb
from dotenv import load_dotenv

from include.utils.file_tools import storage_path
from include.utils.log_tools import logger

# Load environment variables
load_dotenv(override=True)


class LoaderSilverBase(ABC):
    """
    Base class for loading data from the Bronze to Silver layer using DuckDB.

    This class handles:
    - Loading incremental data from the Bronze layer.
    - Fetching existing Silver data from Azure Blob Storage.
    - Merging the new and existing datasets.

    Attributes
    ----------
    source_name : str
        The source's primary identifier.
    source_surname : str
        The source's secondary identifier.
    """

    source_name: str
    source_surname: str

    def __init__(self):
        """
        Initialize the LoaderSilverBase class.

        This method sets up a connection to Azure Blob Storage for data storage
        and retrieval. It installs and loads the DuckDB Azure extension and creates
        a secret for authentication.

        Subclasses must define `source_name` and `source_surname` attributes.

        Raises
        ------
        duckdb.IOException
            If the Azure extension fails to install or load.
        """
        env_var = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

        # Install and load Azure extension in DuckDB
        duckdb.sql("INSTALL azure; LOAD azure;")
        duckdb.sql(
            f"""
            CREATE OR REPLACE SECRET secret1 (
                TYPE azure,
                CONNECTION_STRING '{env_var}'
            );
        """  # nosec
        )

    def start(self, load_from: Union[str, Path], garbage: bool, container: str) -> str:
        """
        Execute the data loading process.

        This method:
        1. Reads incremental data from the Bronze layer.
        2. Fetches existing Silver data from Azure storage (if available).
        3. Merges the incremental and existing datasets and stores the result.

        Parameters
        ----------
        load_from : Union[str, Path]
            Path to the directory containing the Bronze dataset.
        garbage : bool
            Whether the dataset includes invalid data.
        container : str
            Name of the Azure Blob Storage container where the Silver data is stored.

        Returns
        -------
        str
            The full path where the final Silver dataset is stored.
        """
        # Defining filename parameters
        self._load_from: Union[str, Path] = str(load_from)
        self._garbage: bool = garbage
        self._container: str = container

        self._file_name: str = f"silver_{self.source_name}_{self.source_surname}"

        if self._garbage:
            self._file_name += "_garbage.parquet"
        else:
            self._file_name += ".parquet"

        self._file_from: str = f"{self._load_from}/{self._file_name}"

        # Running loader process
        df_incremental = self._get_incremental_data()
        df_exists = self._get_exists_data()
        df_final = self._fetch_data(df_incremental, df_exists)
        self._load_data(df_final)

        return self._file_from

    def _get_incremental_data(self) -> duckdb.DuckDBPyRelation:
        """
        Retrieve incremental data from the Bronze layer.

        Reads a Parquet file containing the new data to be merged into Silver.

        Returns
        -------
        duckdb.DuckDBPyRelation
            A DuckDB relation containing the incremental data.

        Raises
        ------
        duckdb.IOException
            If the Parquet file cannot be read (e.g., missing or corrupted).
        """
        sql_incremental = f"SELECT * FROM read_parquet('{self._file_from}')"  # nosec
        df_incremental = duckdb.sql(sql_incremental)
        return df_incremental

    def _get_exists_data(self) -> Union[duckdb.DuckDBPyRelation, None]:
        """
        Retrieve existing Silver data from Azure storage.

        If the dataset is not found (e.g., on the first execution), logs a message
        and returns `None`.

        Returns
        -------
        Union[duckdb.DuckDBPyRelation, None]
            - A DuckDB relation containing the existing Silver dataset.
            - `None` if no previous data is found.

        Raises
        ------
        duckdb.IOException
            If the file cannot be accessed or read due to corruption or permissions.
        """
        file_exists: str = (
            f"az://{self._container}/"
            f"{storage_path('silver', self.source_name, None)}/"
            f"{self._file_name}"
        )

        sql_exists = f"SELECT * FROM read_parquet('{file_exists}')"  # nosec
        try:
            df_exists = duckdb.sql(sql_exists)
            return df_exists
        except duckdb.IOException:
            # This exception handles cases where no Silver file exists yet
            logger.info(f"Silver parquet file ('{file_exists} does not exist.')")
            return None

    def _fetch_data(
        self,
        df_incremental: duckdb.DuckDBPyRelation,
        df_exists: Union[duckdb.DuckDBPyRelation, None],
    ) -> duckdb.DuckDBPyRelation:
        """
        Merge incremental data with existing Silver data.

        If existing Silver data is available, it appends new data using `UNION ALL`.

        Parameters
        ----------
        df_incremental : duckdb.DuckDBPyRelation
            The newly extracted incremental dataset.
        df_exists : Union[duckdb.DuckDBPyRelation, None]
            The existing Silver dataset, or `None` if it's the first execution.

        Returns
        -------
        duckdb.DuckDBPyRelation
            The merged dataset to be written to the Silver layer.
        """
        if df_exists:
            df_final = duckdb.sql(
                "SELECT * FROM df_incremental UNION ALL SELECT * FROM df_exists"  # nosec
            )
        else:
            df_final = df_incremental

        return df_final

    def _load_data(self, df_final: duckdb.DuckDBPyRelation) -> None:
        """
        Write the final merged dataset to the Silver layer.

        Saves the transformed data as a Parquet file.

        Parameters
        ----------
        df_final : duckdb.DuckDBPyRelation
            The final dataset to be stored.

        Raises
        ------
        duckdb.IOException
            If the dataset cannot be written to the destination path.
        """
        sql_load: str = (
            f"COPY df_final TO '{self._file_from}' (FORMAT parquet);"  # nosec
        )
        duckdb.sql(sql_load)


class LoaderGoldBase(ABC):
    """
    Base class for loading data from the Silver to Gold layer using DuckDB.

    This class handles:
    - Query Silver data from Azure Blob Storage.
    - Create Gold data parquet file.

    Attributes
    ----------
    source_name : str
        The source's primary identifier.
    source_surname : str
        The source's secondary identifier.
    """

    source_name: str
    source_surname: str
    _sql_gold: str

    def __init__(self):
        """
        Initialize the LoaderGoldBase class.

        This method sets up a connection to Azure Blob Storage for data storage
        and retrieval. It installs and loads the DuckDB Azure extension and creates
        a secret for authentication.

        Raises
        ------
        duckdb.IOException
            If the Azure extension fails to install or load.
        """
        env_var = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

        # Install and load Azure extension in DuckDB
        duckdb.sql("INSTALL azure; LOAD azure;")
        duckdb.sql(
            f"""
            CREATE OR REPLACE SECRET secret1 (
                TYPE azure,
                CONNECTION_STRING '{env_var}'
            );
        """  # nosec
        )

    def start(self, load_to: Union[str, Path], container: str) -> str:
        """
        Execute the data loading process.

        This method:
        1. Reads data from the Silver layer.
        2. Create Gold data parquet file.

        Parameters
        ----------
        load_to : Union[str, Path]
            Path to the directory containing the Bronze dataset.
        container : str
            Name of the Azure Blob Storage container where the Silver data is stored.

        Returns
        -------
        str
            The file path where the final Gold dataset is stored.
        """
        # Defining filename parameters
        self._load_to: Union[str, Path] = str(load_to)
        self._container: str = container

        self._file_name_silver: str = (
            f"silver_{self.source_name}_{self.source_surname}.parquet"
        )
        self._file_name_gold: str = (
            f"gold_{self.source_name}_{self.source_surname}.parquet"
        )
        self._file_path_gold: str = f"{self._load_to}/{self._file_name_gold}"

        # Running loader process
        self._get_sql_gold()
        df_gold = self._get_data()
        self._check_data_quality(df_gold)
        self._load_data(df_gold)
        return self._file_path_gold

    @abstractmethod
    def _get_sql_gold(self):
        """
        Construct the SQL query to retrieve Silver data.

        This method must be implemented by subclasses to define how data
        should be extracted from the Silver layer.

        Examples
        --------
        Example implementation for extracting Silver data:

        >>> file_silver = (
        >>>     f"az://{self._container}/"
        >>>     f"{storage_path('silver', self.source_name)}"
        >>>     f"{self._file_name}"
        >>> )
        >>> self._sql_gold = f"SELECT * FROM read_parquet('{file_silver}')"

        Raises
        ------
        NotImplementedError
            If the subclass does not implement this method.
        """
        raise NotImplementedError("Subclasses must implement `_get_query`.")

    def _get_data(self) -> duckdb.DuckDBPyRelation:
        """
        Retrieve existing Silver data from Azure storage.

        If the dataset does not exist (e.g., on the first execution), returns `None`.

        Returns
        -------
        Union[duckdb.DuckDBPyRelation, None]
            - A DuckDB relation containing the existing Silver dataset.
            - `None` if no previous data is found.

        Raises
        ------
        duckdb.IOException
            If the file cannot be accessed or read.
        """
        print(self._sql_gold)
        df_gold = duckdb.sql(self._sql_gold)
        return df_gold

    @abstractmethod
    def _check_data_quality(self, df_gold: duckdb.DuckDBPyRelation) -> None:
        """
        Perform data quality checks on the Gold dataset.

        Subclasses must implement this method to define validation rules,
        ensuring that the transformed data meets quality requirements.

        Parameters
        ----------
        df_gold : duckdb.DuckDBPyRelation
            The dataset to be validated.

        Raises
        ------
        ValueError
            If the data fails quality checks.
        NotImplementedError
            If the subclass does not implement this method.
        """
        raise NotImplementedError("Subclasses must implement `_check_data_quality`.")

    def _load_data(self, df_gold: duckdb.DuckDBPyRelation) -> None:
        """
        Write the final Gold layer.

        Saves the transformed data as a Parquet file.

        Parameters
        ----------
        df_gold : duckdb.DuckDBPyRelation
            The final dataset to be stored.

        Raises
        ------
        duckdb.IOException
            If the dataset cannot be written to the destination path.
        """
        sql_load: str = (
            f"COPY df_gold TO '{self._file_path_gold}' (FORMAT parquet);"  # nosec
        )
        duckdb.sql(sql_load)
