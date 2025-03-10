"""
Base module for data transformation.

This module defines the `TransformerBase` abstract class, which provides a framework
for ingesting, validating, and storing transformed data. Subclasses must implement
data extraction logic to customize the transformation process.
"""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Union

import duckdb
import pandas as pd
import pandera as pa
from pandera import DataFrameModel

from include.utils.log_tools import logger


class TransformerBase(ABC):
    """
    Abstract base class for data transformation pipelines.

    This class defines the structure for data ingestion, validation,
    and storage following a transformation process.
    """

    source_name: str
    source_surname: str
    layer_from: str
    layer_to: str
    data_contract: DataFrameModel

    @abstractmethod
    def _get_query(self) -> str:
        """
        Abstract method to retrieve the SQL query for data extraction.

        Returns
        -------
        str
            SQL query string to be executed.
        """
        raise NotImplementedError("Subclasses must implement `_get_query`.")

    def _ingest_raw_data(self) -> pd.DataFrame:
        """
        Execute the SQL query and retrieves raw data from the source.

        Returns
        -------
        pd.DataFrame
            Raw data retrieved from the data source as Pandas DataFrame.
        """
        sql_load: str = self._get_query()
        pre_process_data: pd.DataFrame = duckdb.sql(query=sql_load).to_df()

        return pre_process_data

    def _check_data_quality(
        self, pre_process_data: pd.DataFrame
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Validate the data against the defined contract.

        Parameters
        ----------
        pre_process_data : pd.DataFrame
            Raw data retrieved from the source.

        Returns
        -------
        tuple[pd.DataFrame, pd.DataFrame]
            A tuple containing:
            - The valid data as a Pandas DataFrame.
            - The invalid data as a Pandas DataFrame.
        """
        try:
            # Validating data
            df_valid_data = self.data_contract.validate(pre_process_data, lazy=True)
            df_invalid_data = pd.DataFrame(
                columns=self.data_contract.to_schema().columns.keys()
            )

        except pa.errors.SchemaErrors as e:
            # Get failure cases (invalid rows)
            failure_cases = e.failure_cases
            invalid_indexes = failure_cases["index"].unique()

            # Splitting data between valid and invalid
            df_valid_data = pre_process_data.drop(index=invalid_indexes)
            df_invalid_data = pre_process_data.iloc[invalid_indexes]

        logger.info(
            f"Valid processed rows = {len(df_valid_data)}."
            f"Invalid processed rows = {len(df_invalid_data)}."
        )

        return df_valid_data, df_invalid_data

    def _load_data(self, df_valid_data: pd.DataFrame, df_invalid_data: pd.DataFrame):
        """
        Save the processed data as Parquet files.

        Parameters
        ----------
        df_valid_data : pd.DataFrame
            Data that passed validation checks.
        df_invalid_data : pd.DataFrame
            Data that failed validation checks.
        """
        valid_data_path: str = (
            f"{self.load_from}/"
            f"{self.layer_to}_"
            f"{self.source_name}_"
            f"{self.source_surname}.parquet"
        )

        invalid_data_path: str = (
            f"{self.load_from}/"
            f"{self.layer_to}_"
            f"{self.source_name}_"
            f"{self.source_surname}_garbage.parquet"
        )

        df_valid_data.to_parquet(valid_data_path, index=False)
        df_invalid_data.to_parquet(invalid_data_path, index=False)

    def start(self, load_from: Union[str, Path]):
        """
        Run the data transformation pipeline.

        Parameters
        ----------
        load_from : Union[str, Path]
            Directory where the processed files will be saved.
        """
        self.load_from: Union[str, Path] = str(load_from)

        pre_process_data = self._ingest_raw_data()

        df_valid_data, df_invalid_data = self._check_data_quality(pre_process_data)

        self._load_data(df_valid_data, df_invalid_data)
