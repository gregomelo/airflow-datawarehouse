"""
Base module for data loading.

This module defines the `LoaderSilverBase` abstract class, which provides a framework
for ingesting, validating, and loading transformed data. Subclasses must implement
data extraction logic to customize the transformation and loading process.
"""

import os
from abc import ABC
from pathlib import Path
from typing import Union

import duckdb
from dotenv import load_dotenv

from include.utils.file_tools import storage_path

# Load environment variables
load_dotenv(override=True)


class LoaderSilverBase(ABC):
    """
    Base class for loading data from the Bronze to Silver layer using DuckDB.

    This class is responsible for:
    - Loading incremental data from the Bronze layer.
    - Fetching existing data from the Silver layer.
    - Merging both datasets.
    - Saving the final dataset back to the Silver layer.

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

        This method sets up the Azure Storage connection by installing
        and loading the Azure extension in DuckDB and creating a secret.
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

    def start(self, load_from: Union[str, Path], gargabe: bool, container: str) -> None:
        """
        Execute the data loading process.

        This method reads incremental data, fetches existing Silver data,
        merges both, and writes the final dataset back to the Silver layer.

        Parameters
        ----------
        load_from : Union[str, Path]
            Path to the Bronze data source.
        gargabe : bool
            Whether the dataset includes garbage data.
        container : str
            Name of the Azure container where the Silver data is stored.
        """
        # Defining filename parameters
        self._load_from: Union[str, Path] = str(load_from)
        self._gargabe: bool = gargabe
        self._container: str = container

        self._file_name: str = f"silver_{self.source_name}_{self.source_surname}"

        if self._gargabe:
            self._file_name += "_gargabe.parquet"
        else:
            self._file_name += ".parquet"

        self._file_from: str = f"{self._load_from}/{self._file_name}"

        # Running loader process
        df_incremental = self._get_incremental_data()
        df_exists = self._get_exists_data()
        df_final = self._fetch_data(df_incremental, df_exists)
        self._load_data(df_final)

    def _get_incremental_data(self) -> duckdb.DuckDBPyRelation:
        """
        Retrieve the incremental data from the Bronze layer.

        This method reads a Parquet file from the given path.

        Returns
        -------
        duckdb.DuckDBPyRelation
            A DuckDB relation containing the incremental data.
        """
        sql_incremental = f"SELECT * FROM read_parquet('{self._file_from}')"  # nosec
        df_incremental = duckdb.sql(sql_incremental)
        return df_incremental

    def _get_exists_data(self) -> Union[duckdb.DuckDBPyRelation, None]:
        """
        Retrieve existing data from the Silver layer.

        If the data does not exist (e.g., first execution), it returns None.

        Returns
        -------
        Union[duckdb.DuckDBPyRelation, None]
            A DuckDB relation containing the existing Silver data, or None if unavailable.
        """
        file_exists: str = (
            f"az://{self._container}/"
            f"{storage_path('silver', self.source_name, self.source_surname)}"
            f"{self._file_name}"
        )

        sql_exists = f"SELECT * FROM read_parquet('{file_exists}')"  # nosec
        try:
            df_exists = duckdb.sql(sql_exists)
            return df_exists
        except duckdb.IOException:
            # This exception exists to deal with the first load
            return None

    def _fetch_data(
        self,
        df_incremental: duckdb.DuckDBPyRelation,
        df_exists: Union[duckdb.DuckDBPyRelation, None],
    ) -> duckdb.DuckDBPyRelation:
        """
        Merge incremental data with existing Silver data.

        If existing data is available, it performs a UNION ALL operation.

        Parameters
        ----------
        df_incremental : duckdb.DuckDBPyRelation
            The newly extracted incremental data.
        df_exists : Union[duckdb.DuckDBPyRelation, None]
            The existing Silver data or None if it's the first execution.

        Returns
        -------
        duckdb.DuckDBPyRelation
            The final merged dataset to be written to the Silver layer.
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
        Write the final dataset back to the Silver layer.

        Parameters
        ----------
        df_final : duckdb.DuckDBPyRelation
            The final merged dataset.
        """
        sql_load: str = (
            f"COPY df_final TO '{self._file_from}' (FORMAT parquet);"  # nosec
        )
        duckdb.sql(sql_load)
