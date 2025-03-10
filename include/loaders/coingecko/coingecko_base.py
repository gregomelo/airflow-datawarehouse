"""
CoinGecko Coins Base Loaders.

This module defines a base loader that processes CoinGecko data.
"""

from include.loaders.loader_base import LoaderSilverBase


class CoinGeckoBaseSilverLoader(LoaderSilverBase):
    """
    Loader for transforming CoinGecko data from Bronze to Silver layer.

    This class inherits from `LoaderSilverBase` and is specialized for processing
    CoinGecko data.

    Attributes
    ----------
    source_name : str
        The source identifier, set to "coingecko".
    """

    source_name: str = "coingecko"
