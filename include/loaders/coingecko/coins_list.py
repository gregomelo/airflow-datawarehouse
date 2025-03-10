"""
CoinGecko Coins List Loaders.

This module defines a loader that processes CoinGecko's coins list
from the bronze layer to the silver layer in the data pipeline.
"""

from include.loaders.coingecko.coingecko_base import CoinGeckoBaseSilverLoader


class CoinGeckoBaseCoinsListSilverLoader(CoinGeckoBaseSilverLoader):
    """
    Loader for transforming CoinGecko Coins List data from Bronze to Silver layer.

    This class extends `CoinGeckoBaseSilverLoader` and is specialized for processing
    the "coins_list" dataset from CoinGecko.

    Attributes
    ----------
    source_name : str
        The source identifier, set to "coingecko".
    source_sourname : str
        The specific dataset identifier, set to "coins_list".
    """

    source_sourname: str = "coins_list"
