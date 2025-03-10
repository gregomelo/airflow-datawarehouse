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

from include.loaders.coingecko.coins_list import CoinGeckoBaseCoinsListSilverLoader
from include.loaders.loader_base import LoaderSilverBase
from include.transformers.coingecko.coins_list import CoinGeckoCoinsListBronze
from include.transformers.transformer_base import TransformerBase

# Define transformers and their test cases
TRANSFORMERS_TEST_CASES: Dict[str, Dict[str, Any]] = {
    "CoinGeckCoinsList": {
        "transformer": CoinGeckoCoinsListBronze,
        "loader_silver": CoinGeckoBaseCoinsListSilverLoader,
        "test_cases": [
            (
                "valid_invalid",
                [
                    {
                        "id": "bitcoin",
                        "symbol": "btc",
                        "name": "Bitcoin",
                        "platforms": {"ethereum": "0xbtc"},
                    },
                    {
                        "id": "ethereum",
                        "symbol": "eth",
                        "name": "Ethereum",
                        "platforms": {"binance-smart-chain": "0xeth"},
                    },
                    {
                        "id": "invalid_coin",
                        "symbol": None,
                        "name": None,
                        "platforms": {},
                    },
                ],
            ),
            (
                "only_valid",
                [
                    {
                        "id": "bitcoin",
                        "symbol": "btc",
                        "name": "Bitcoin",
                        "platforms": {"ethereum": "0xbtc"},
                    },
                    {
                        "id": "ethereum",
                        "symbol": "eth",
                        "name": "Ethereum",
                        "platforms": {"binance-smart-chain": "0xeth"},
                    },
                ],
            ),
            (
                "only_invalid",
                [
                    {
                        "id": "invalid_coin",
                        "symbol": None,
                        "name": None,
                        "platforms": {},
                    },
                ],
            ),
        ],
    },
}


@pytest.fixture(
    params=[
        (name, case_name, case_data)
        for name, details in TRANSFORMERS_TEST_CASES.items()
        for case_name, case_data in details["test_cases"]
    ],
    ids=lambda param: f"{param[0]}-{param[1]}",
)
def transformer_with_data(
    request: pytest.FixtureRequest, tmp_path: Path
) -> Generator[Tuple[TransformerBase, LoaderSilverBase, Path, str], None, None]:
    """
    Fixture to match each transformer with its respective test cases.

    This fixture dynamically assigns a transformer and its corresponding test case
    data.

    Parameters
    ----------
    request : pytest.FixtureRequest
        Pytest fixture request object containing parameterized test case data.
    tmp_path : Path
        Temporary path provided by pytest for storing test files.

    Yields
    ------
    Tuple[TransformerBase, LoaderSilverBase, Path, str]
        The instantiated transformer, instantiated loader, the temporary path,
        and the test case name.
    """
    transformer_name: str
    test_case_name: str
    test_case_data: List[Dict[str, Any]]

    transformer_name, test_case_name, test_case_data = request.param

    # Instantiate the transformer
    transformer = TRANSFORMERS_TEST_CASES[transformer_name]["transformer"]()
    loader_silver = TRANSFORMERS_TEST_CASES[transformer_name]["loader_silver"]()

    # Create a temporary input file
    file_path: Path = (
        tmp_path / f"test_data_{test_case_name}_16-02-25T00_39_21_001.json"
    )
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(test_case_data, f)

    yield transformer, loader_silver, tmp_path, test_case_name


def test_transformer_integration(
    transformer_with_data: Tuple[TransformerBase, LoaderSilverBase, Path, str],
) -> None:
    """
    Test transformers end-to-end using dynamically assigned test cases.

    This test ensures that the transformer correctly processes data,
    producing expected valid and invalid outputs.

    Parameters
    ----------
    transformer_with_data : Tuple[TransformerBase, LoaderSilverBase, Path, str]
        The instantiated transformer, The instantiated loader, the temporary path,
        and the test case name.

    Raises
    ------
    AssertionError
        If the expected output files are not created or if data validation fails.
    """
    transformer, loader_silver, tmp_path, test_case_name = transformer_with_data

    # Run the transformer pipeline
    transformer.start(tmp_path)

    # Expected output files
    valid_file: Path = tmp_path / (
        f"silver_{transformer.source_name}_{transformer.source_surname}.parquet"
    )
    invalid_file: Path = tmp_path / (
        f"silver_{transformer.source_name}_"
        f"{transformer.source_surname}_garbage.parquet"
    )

    # Assert that output files are created
    assert valid_file.exists(), f"Valid data file should exist for {test_case_name}"
    assert invalid_file.exists(), f"Invalid data file should exist for {test_case_name}"

    # Read and validate valid data
    df_valid: pd.DataFrame = pd.read_parquet(valid_file)
    if "only_invalid" in test_case_name:
        assert df_valid.empty, f"Valid data should be empty for {test_case_name}"
    else:
        assert (
            not df_valid.empty
        ), f"Valid data should not be empty for {test_case_name}"
        assert "id" in df_valid.columns, "Valid data should have 'id' column"

    # Read and validate invalid data
    df_invalid: pd.DataFrame = pd.read_parquet(invalid_file)
    if "only_valid" in test_case_name:
        assert df_invalid.empty, f"Invalid data should be empty for {test_case_name}"
    else:
        assert (
            not df_invalid.empty
        ), f"Invalid data should not be empty for {test_case_name}"
        assert "id" in df_invalid.columns, "Invalid data should have 'id' column"

    # Transformer and loaders as pairs
    assert (
        loader_silver.source_name == transformer.source_name
    ), "Loader source_name should match transformer source_name"
    assert (
        loader_silver.source_surname == transformer.source_surname
    ), "Loader source_surname should match transformer source_surname"

    # Ensure the loader_silver call all methods
    with (
        patch.object(loader_silver, "_get_incremental_data") as mock_get_incremental,
        patch.object(loader_silver, "_get_exists_data") as mock_get_exists,
        patch.object(loader_silver, "_fetch_data") as mock_fetch,
        patch.object(loader_silver, "_load_data") as mock_load,
    ):

        # Set return values for methods
        mock_get_incremental.return_value = "mock_incremental_data"
        mock_get_exists.return_value = "mock_existing_data"
        mock_fetch.return_value = "mock_final_data"

        # Execute loader
        loader_silver.start(
            load_from=tmp_path, gargabe=False, container="test-container"
        )

        # Assert each internal method is called once
        mock_get_incremental.assert_called_once()
        mock_get_exists.assert_called_once()
        mock_fetch.assert_called_once_with(
            "mock_incremental_data", "mock_existing_data"
        )
        mock_load.assert_called_once_with("mock_final_data")
