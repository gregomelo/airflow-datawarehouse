"""
Test cases for the CoinGecko CoinsList transformer and loaders.

This module defines test cases for the CoinGecko CoinsList ETL pipeline. It
associates the `CoinGeckoCoinsListBronze` transformer with its corresponding
silver and gold loaders.

The test cases are designed to validate:
- Correct classification of valid and invalid coin data.
- Schema integrity and expected transformations.
- Handling of missing or malformed data.

These test cases are dynamically loaded in the integration test suite and
executed using pytest.

Attributes
----------
COINSLIST_TEST_CASES : dict of {str : dict}
    Dictionary containing the transformer, silver loader, gold loader,
    and corresponding test cases.

    - `"CoinGeckCoinsList"`:
        - `"transformer"` : CoinGeckoCoinsListBronze
        - `"loader_silver"` : CoinGeckoBaseCoinsListSilverLoader
        - `"loader_gold"` : CoinGeckoBaseCoinsListGoldLoader
        - `"test_cases"` : list of test cases, each containing:
            - A test case name.
            - A list of dictionaries representing raw input data.
"""

from typing import Any, Dict

from include.loaders.coingecko.coins_list import (
    CoinGeckoCoinsListGoldLoader,
    CoinGeckoCoinsListSilverLoader,
)
from include.transformers.coingecko.coins_list import CoinGeckoCoinsListBronze

# Define transformers and their test cases
COINSLIST_TEST_CASES: Dict[str, Dict[str, Any]] = {
    "CoinGeckCoinsList": {
        "transformer": CoinGeckoCoinsListBronze,
        "loader_silver": CoinGeckoCoinsListSilverLoader,
        "loader_gold": CoinGeckoCoinsListGoldLoader,
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
