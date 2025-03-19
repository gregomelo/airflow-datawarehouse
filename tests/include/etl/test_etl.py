"""
Test suite for data pipeline ETL.

This module contains integration tests for data transformation classes,
ensuring the correct processing of raw input data into structured formats.

The test suite is designed to be adaptable, allowing easy addition of new
transformers with minimal changes. Tests are parameterized using pytest
fixtures, dynamically associating transformers with predefined test cases.

Validations include:
- Correct classification of valid and invalid data.
- Proper creation of expected output files.
- Schema and structural integrity of transformed data.
- Handling of edge cases and missing values.
- Loading data to silver layer.

Current transformers tested:
- CoinGecko CoinsList

New transformers can be added by extending `TRANSFORMERS_TEST_CASES` with
the appropriate class and test cases.
"""

import json
from pathlib import Path
from typing import Any, Dict, Generator, List, Tuple
from unittest.mock import patch

import pandas as pd
import pytest

from include.loaders.loader_base import LoaderGoldBase, LoaderSilverBase
from include.transformers.transformer_base import TransformerBase
from tests.include.etl.coingecko.etl_test_cases_coinslist import COINSLIST_TEST_CASES

TRANSFORMERS_TEST_CASES: Dict[str, Dict[str, Any]] = {**COINSLIST_TEST_CASES}


@pytest.fixture(
    scope="class",
    params=[
        (name, case_name, case_data)
        for name, details in TRANSFORMERS_TEST_CASES.items()
        for case_name, case_data in details["test_cases"]
    ],
    ids=lambda param: f"{param[0]}-{param[1]}",
)
def transformer_with_data(
    request: pytest.FixtureRequest, tmp_path_factory: pytest.TempPathFactory
) -> Generator[
    Tuple[TransformerBase, LoaderSilverBase, LoaderGoldBase, Path, str], None, None
]:
    """
    Provide a fixture that matches each transformer with its respective test cases.

    This fixture dynamically assigns a transformer, its corresponding test case data,
    and creates a temporary test file for data processing.

    Parameters
    ----------
    request : pytest.FixtureRequest
        Pytest fixture request object containing parameterized test case data.
    tmp_path_factory : pytest.TempPathFactory
        Temporary path provided by pytest for storing test files.

    Yields
    ------
    tuple of (TransformerBase, LoaderSilverBase, LoaderGoldBase, Path, str)
        - Instantiated transformer.
        - Instantiated silver loader.
        - Instantiated gold loader.
        - Temporary directory path.
        - Test case name.
    """
    transformer_name: str
    test_case_name: str
    test_case_data: List[Dict[str, Any]]

    transformer_name, test_case_name, test_case_data = request.param

    # Instantiate the transformer
    transformer = TRANSFORMERS_TEST_CASES[transformer_name]["transformer"]()
    loader_silver = TRANSFORMERS_TEST_CASES[transformer_name]["loader_silver"]()
    loader_gold = TRANSFORMERS_TEST_CASES[transformer_name]["loader_gold"]()

    # Create a temporary input file
    tmp_path = tmp_path_factory.mktemp(f"test_case_{test_case_name}")
    file_path: Path = (
        tmp_path / f"test_data_{test_case_name}_16-02-25T00_39_21_001.json"
    )
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(test_case_data, f)

    yield transformer, loader_silver, loader_gold, tmp_path, test_case_name


class TestTransformerIntegration:
    """Integration tests for ETL transformers and loaders."""

    def test_files_exist(self, transformer_with_data) -> None:
        """
        Verify that valid and invalid output files are created.

        Ensures that after transformation, both valid and invalid Parquet files
        are correctly generated.

        Parameters
        ----------
        transformer_with_data : tuple
            Tuple containing transformer, loaders, temp path, and test case name.
        """
        transformer, _, _, tmp_path, test_case_name = transformer_with_data
        transformer.start(tmp_path)

        valid_file = (
            tmp_path
            / f"silver_{transformer.source_name}_{transformer.source_surname}.parquet"
        )
        invalid_file = tmp_path / (
            f"silver_{transformer.source_name}_"
            f"{transformer.source_surname}_garbage.parquet"
        )

        assert valid_file.exists(), f"Valid data file should exist for {test_case_name}"
        assert (
            invalid_file.exists()
        ), f"Invalid data file should exist for {test_case_name}"

    def test_valid_data(self, transformer_with_data) -> None:
        """
        Validate that the output file for valid data is correctly created.

        Ensures that:
        - The valid Parquet file exists.
        - It contains the expected structure and data.

        Parameters
        ----------
        transformer_with_data : tuple
            Tuple containing transformer, loaders, temp path, and test case name.
        """
        transformer, _, _, tmp_path, test_case_name = transformer_with_data
        transformer.start(tmp_path)

        valid_file = (
            tmp_path
            / f"silver_{transformer.source_name}_{transformer.source_surname}.parquet"
        )
        df_valid = pd.read_parquet(valid_file)

        if "only_invalid" in test_case_name:
            assert df_valid.empty, f"Valid data should be empty for {test_case_name}"
        else:
            assert (
                not df_valid.empty
            ), f"Valid data should not be empty for {test_case_name}"
            assert "id" in df_valid.columns, "Valid data should have 'id' column"

    def test_invalid_data(self, transformer_with_data) -> None:
        """
        Validate that the output file for invalid data is correctly created.

        Ensures that:
        - The invalid Parquet file exists.
        - It contains the expected structure and data.

        Parameters
        ----------
        transformer_with_data : tuple
            Tuple containing transformer, loaders, temp path, and test case name.
        """
        transformer, _, _, tmp_path, test_case_name = transformer_with_data
        transformer.start(tmp_path)

        invalid_file = tmp_path / (
            f"silver_{transformer.source_name}_"
            f"{transformer.source_surname}_garbage.parquet"
        )
        df_invalid = pd.read_parquet(invalid_file)

        if "only_valid" in test_case_name:
            assert (
                df_invalid.empty
            ), f"Invalid data should be empty for {test_case_name}"
        else:
            assert (
                not df_invalid.empty
            ), f"Invalid data should not be empty for {test_case_name}"
            assert "id" in df_invalid.columns, "Invalid data should have 'id' column"

    def test_transformer_loader_metadata(self, transformer_with_data) -> None:
        """
        Verify that the metadata in the transformer and loaders match.

        Ensures that:
        - The source name and surname of the silver and gold loaders match the
          transformer.

        Parameters
        ----------
        transformer_with_data : tuple
            Tuple containing transformer, loaders, temp path, and test case name.
        """
        transformer, loader_silver, loader_gold, _, _ = transformer_with_data

        assert (
            loader_silver.source_name == transformer.source_name
        ), "Silver loader source_name should match transformer source_name"
        assert (
            loader_silver.source_surname == transformer.source_surname
        ), "Silver loader source_surname should match transformer source_surname"
        assert (
            loader_gold.source_name == transformer.source_name
        ), "Gold loader source_name should match transformer source_name"
        assert (
            loader_gold.source_surname == transformer.source_surname
        ), "Gold loader source_surname should match transformer source_surname"

    def test_loader_silver_calls(self, transformer_with_data) -> None:
        """
        Verify method calls in the silver loader.

        This test mocks internal method calls to ensure that:
        - `_get_incremental_data` is called once.
        - `_get_exists_data` is called once.
        - `_fetch_data` is called with the correct parameters.
        - `_load_data` is called once.

        Parameters
        ----------
        transformer_with_data : tuple
            Tuple containing transformer, loaders, temp path, and test case name.
        """
        _, loader_silver, _, tmp_path, _ = transformer_with_data

        with (
            patch.object(
                loader_silver, "_get_incremental_data"
            ) as mock_get_incremental,
            patch.object(loader_silver, "_get_exists_data") as mock_get_exists,
            patch.object(loader_silver, "_fetch_data") as mock_fetch,
            patch.object(loader_silver, "_load_data") as mock_load,
        ):
            mock_get_incremental.return_value = "mock_incremental_data"
            mock_get_exists.return_value = "mock_existing_data"
            mock_fetch.return_value = "mock_final_data"

            loader_silver.start(
                load_from=tmp_path, garbage=False, container="test-container"
            )

            mock_get_incremental.assert_called_once()
            mock_get_exists.assert_called_once()
            mock_fetch.assert_called_once_with(
                "mock_incremental_data", "mock_existing_data"
            )
            mock_load.assert_called_once_with("mock_final_data")

    def test_loader_gold_calls(self, transformer_with_data) -> None:
        """
        Verify method calls in the gold loader.

        This test mocks internal method calls to ensure that:
        - `_get_sql_gold` is called once.
        - `_get_data` is called once.
        - `_check_data_quality` is called with the correct parameters.
        - `_load_data` is called once.

        Parameters
        ----------
        transformer_with_data : tuple
            Tuple containing transformer, loaders, temp path, and test case name.
        """
        _, _, loader_gold, tmp_path, _ = transformer_with_data

        with (
            patch.object(loader_gold, "_get_sql_gold") as mock_get_sql_gold,
            patch.object(loader_gold, "_get_data") as mock_get_data,
            patch.object(loader_gold, "_check_data_quality") as mock_check_quality,
            patch.object(loader_gold, "_load_data") as mock_load,
        ):
            mock_get_data.return_value = "mock_get_data"

            loader_gold.start(load_to=tmp_path, container="test-container")

            mock_get_sql_gold.assert_called_once()
            mock_get_data.assert_called_once()
            mock_check_quality.assert_called_once_with("mock_get_data")
            mock_load.assert_called_once_with("mock_get_data")
