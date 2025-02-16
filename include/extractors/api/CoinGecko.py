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

from pathlib import Path
from typing import Any, Dict

from include.extractors.api_base import APIExtractor


class CoinGeckoBase(APIExtractor):
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

    source_name: str = "CoinGecko"
    _endpoint: str = "https://api.coingecko.com/api/v3/"


class CoinGeckoCoinsList(CoinGeckoBase):
    """Extractor for the CoinGecko 'coins/list' endpoint.

    This class retrieves the list of all available coins from the CoinGecko API.

    Attributes
    ----------
    _relative_url : str
        Relative URL of the 'coins/list' endpoint.
    """

    _relative_url: str = "coins/list"

    def __init__(self, params_query: Dict[str, Any], load_to: Path | str):
        """Initialize the extractor for the 'coins/list' endpoint.

        Parameters
        ----------
        params_query : Dict[str, Any]
            Query parameters to be sent in the API request.
        load_to : Path | str
            Destination path or filename where the extracted data will be saved.
        """
        super().__init__(self._relative_url, params_query, load_to)

    def _is_last_page(self, data: Any) -> bool:
        """Determine if the current page is the last one.

        Since the 'coins/list' endpoint does not paginate, this method always
        returns `True`.

        Parameters
        ----------
        data : Any
            The response data from the API.

        Returns
        -------
        bool
            Always `True`, as there is no pagination in this endpoint.
        """
        return True

    def _get_next_pagination(self, data: Any) -> Dict[str, Any]:
        """Retrieve the parameters for the next page of data.

        Since the 'coins/list' endpoint does not support pagination,
        this method always returns an empty dictionary.

        Parameters
        ----------
        data : Any
            The response data from the API.

        Returns
        -------
        Dict[str, Any]
            Always an empty dictionary `{}`.
        """
        return {}
