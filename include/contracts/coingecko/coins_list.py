"""
CoinGecko Coins List Data Contract.

This module defines a Pandera data schema for validating the structure of
CoinGecko's coins list dataset.
"""

from datetime import datetime

import pandera as pa
from pandera.typing import Series


class CoinGeckoCoinsListSchema(pa.DataFrameModel):
    """
    Data schema for the CoinGecko coins list.

    This schema validates the structure of a dataset containing metadata
    about cryptocurrency coins from CoinGecko. It ensures that required fields
    are non-null, enforces string constraints, and guarantees type consistency.

    Attributes
    ----------
    id : Series[str]
        Unique identifier for the cryptocurrency (non-null, at least one character).
    symbol : Series[str]
        Ticker symbol of the cryptocurrency (non-null, at least one character).
    name : Series[str]
        Name of the cryptocurrency (non-null, at least one character).
    platform_name : Series[str], optional
        Name of the blockchain platform the token is built on (nullable).
    contract_address : Series[str], optional
        Smart contract address of the token (nullable).
    extracted_at : Series[datetime]
        Timestamp indicating when the data was extracted (non-null).
    """

    id: Series[str] = pa.Field(nullable=False, str_length={"min_value": 1}, coerce=True)
    symbol: Series[str] = pa.Field(
        nullable=False, str_length={"min_value": 1}, coerce=True
    )
    name: Series[str] = pa.Field(
        nullable=False, str_length={"min_value": 1}, coerce=True
    )
    platform_name: Series[str] = pa.Field(nullable=True, coerce=True)
    contract_address: Series[str] = pa.Field(nullable=True, coerce=True)
    extracted_at: Series[datetime] = pa.Field(nullable=False, coerce=True)
