"""
CoinGecko Coins Base Transformer.

This module defines a base transformer that processes CoinGecko data.
"""

from include.transformers.transformer_base import TransformerBase


class CoinGeckoTranformerBase(TransformerBase):
    """
    Transformer Base for CoinGecko from the bronze to the silver layer.

    This class implements a transformation step in the ETL pipeline,
    processing CoinGecko data stored in the bronze layer and
    preparing it for the silver layer.

    Attributes
    ----------
    source_name : str
        The name of the data source, set as "CoinGecko".
    """

    source_name: str = "coingecko"
