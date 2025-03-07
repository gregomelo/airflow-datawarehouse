"""CoinGecko API Extractors.

This module contains classes for extracting data from the CoinGecko API.
The implementation follows a structured approach, inheriting from the
`APIExtractor` base class to ensure consistency across API integrations.

Classes
-------
CoinGeckoBase
    Base class for CoinGecko API extractors.
CoinGeckoCoinsList
    Extractor for retrieving the list of all available coins from the CoinGecko API.

"""

from include.extractors.api_base import APIExtractor


class CoinGeckoBaseExtractor(APIExtractor):
    """Base class for CoinGecko API extractors.

    This class serves as a foundation for specific extractors interacting
    with the CoinGecko API.

    Attributes
    ----------
    _source_name : str
        Name of the data source.
    _endpoint : str
        Base URL of the CoinGecko API.
    """

    source_name: str = "coingecko"
    _endpoint: str = "https://api.coingecko.com/api/v3/"
