"""
Test suite for data transformers.

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

Current transformers tested:
- CoinGecko CoinsList

New transformers can be added by extending `TRANSFORMERS_TEST_CASES` with
the appropriate class and test cases.
"""

import json
from pathlib import Path
from typing import Any, Dict, Generator, List, Tuple

import pandas as pd
import pytest

from include.transformers.coingecko.coins_list import CoinGeckCoinsListBronze

# Define transformers and their test cases
TRANSFORMERS_TEST_CASES: Dict[str, Dict[str, Any]] = {
    "CoinGeckCoinsListBronze": {
        "transformer": CoinGeckCoinsListBronze,
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
) -> Generator[Tuple[CoinGeckCoinsListBronze, Path, str], None, None]:
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
    Tuple[CoinGeckCoinsListBronze, Path, str]
        The instantiated transformer, the temporary path, and the test case name.
    """
    transformer_name: str
    test_case_name: str
    test_case_data: List[Dict[str, Any]]

    transformer_name, test_case_name, test_case_data = request.param

    # Instantiate the transformer
    transformer = TRANSFORMERS_TEST_CASES[transformer_name]["transformer"]()

    # Create a temporary input file
    file_path: Path = (
        tmp_path / f"test_data_{test_case_name}_16-02-25T00_39_21_001.json"
    )
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(test_case_data, f)

    yield transformer, tmp_path, test_case_name


def test_transformer_integration(
    transformer_with_data: Tuple[CoinGeckCoinsListBronze, Path, str],
) -> None:
    """
    Test transformers end-to-end using dynamically assigned test cases.

    This test ensures that the transformer correctly processes data,
    producing expected valid and invalid outputs.

    Parameters
    ----------
    transformer_with_data : Tuple[CoinGeckCoinsListBronze, Path, str]
        The instantiated transformer, the temporary path, and the test case name.

    Raises
    ------
    AssertionError
        If the expected output files are not created or if data validation fails.
    """
    transformer, tmp_path, test_case_name = transformer_with_data

    # Run the transformer pipeline
    transformer.start(tmp_path)

    # Expected output files
    valid_file: Path = tmp_path / (
        f"silver_{transformer.source_name}_{transformer.source_sourname}.parquet"
    )
    invalid_file: Path = tmp_path / (
        f"silver_{transformer.source_name}_"
        f"{transformer.source_sourname}_garbage.parquet"
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
