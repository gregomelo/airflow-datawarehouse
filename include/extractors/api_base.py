"""
Base module for API extraction.

This module defines the `APIExtractor` abstract class, which provides a framework
for paginated API data extraction. Subclasses must implement pagination logic
to customize the extraction process.
"""

import json
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import urljoin

from httpx import Client


class APIExtractor(ABC):
    """
    Abstract base class for API extraction.

    This class provides a generic framework for extracting paginated data from an API.
    It handles session management and data retrieval, while subclasses must define
    pagination logic.

    Attributes
    ----------
    _source_name : str
        Static attribute for the source name (must be defined in subclasses).
    _endpoint : str
        Static attribute for the API endpoint (must be defined in subclasses).
    _relative_url : str
        The specific API path for the request.
    _source_surname : str
        A sanitized version of `_relative_url` used in filenames.
    _params_query : Dict
        Query parameters to be sent in the API request.
    _load_to : Path | str
        The target directory for storing extracted data.
    _session : Optional[Client]
        An HTTPX client session for making API requests.

    Notes
    -----
    To make `_relative_url` static on subclasses, use the following `__init__` method:

    ```python
    super().__init__(self._relative_url, params_query, load_to)
    ```
    """

    _source_name: str
    _endpoint: str

    def __init__(self, relative_url: str, params_query: Dict, load_to: Path | str):
        """
        Initialize the API extractor with request parameters and output settings.

        Parameters
        ----------
        relative_url : str
            The relative path of the API endpoint.
        params_query : Dict
            Dictionary containing default query parameters.
        load_to : Path | str
            Destination path for storing fetched data.
        """
        self._relative_url: str = relative_url
        self._source_surname: str = relative_url.replace("/", "_")
        self._params_query: Dict = params_query
        self._load_to: Path | str = load_to
        self._session: Optional[Client] = None

    def start(self) -> None:
        """
        Start the data extraction process by iterating over paginated results.

        This method initializes an HTTP session, fetches data from the API, and
        stores the results in the specified output location.

        Ensures that the session is properly closed, even if an error occurs.
        """
        self._open_session()
        try:
            self._fetch_data()
        finally:
            self._close_session()

    def _get_headers(self) -> Dict[str, str]:
        """
        Create request headers for API calls.

        Returns
        -------
        Dict[str, str]
            A dictionary containing HTTP headers for the request.
        """
        return {"Content-Type": "application/json"}

    def _open_session(self) -> None:
        """
        Open an HTTP session using `httpx.Client`.

        The session is configured with the API endpoint and default headers.
        """
        headers = self._get_headers()
        self._session = Client(headers=headers)

    def _close_session(self) -> None:
        """
        Close the active HTTP session.

        Ensures that any open connections are properly terminated.
        """
        if self._session:
            self._session.close()

    def _get_data(self, **kwargs) -> Optional[Dict]:
        """
        Fetch data from the API with optional pagination parameters.

        Parameters
        ----------
        **kwargs : dict
            Additional query parameters (e.g., `page`, `cursor`, `offset`).

        Returns
        -------
        Optional[Dict]
            The JSON response from the API if the request is successful;
            otherwise, returns `None`.

        Raises
        ------
        RuntimeError
            If called before initializing the session.
        """
        if self._session is None:
            raise RuntimeError("Session has not been initialized.")

        # Merge static parameters with dynamic pagination parameters
        query_params = {**self._params_query, **kwargs}

        # Ensure proper URL concatenation
        full_url = urljoin(self._endpoint, self._relative_url)

        try:
            response = self._session.get(url=full_url, params=query_params)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"An error occurred: {e}")
            return None

    @abstractmethod
    def _is_last_page(self, data: Dict) -> bool:
        """
        Determine if the API response indicates the last page.

        This method must be implemented by subclasses.

        Parameters
        ----------
        data : Dict
            The API response data.

        Returns
        -------
        bool
            True if the response indicates the last page; False otherwise.
        """
        raise NotImplementedError("Subclasses must implement `_is_last_page()`.")

    @abstractmethod
    def _get_next_pagination(self, data: Dict) -> Dict[str, Any]:
        """
        Extract pagination parameters for the next request.

        This method should be implemented in subclasses to support different
        pagination formats.

        Parameters
        ----------
        data : Dict
            The API response data.

        Returns
        -------
        Dict[str, Any]
            The pagination parameters for the next request.
        """
        raise NotImplementedError("Subclasses must implement `_get_next_pagination()`.")

    def _fetch_data(self) -> None:
        """
        Iterate through paginated API results and process each page.

        This method:
        - Requests data from the API.
        - Saves retrieved data to the specified output path.
        - Determines pagination and fetches subsequent pages if necessary.
        """
        page: int = 1
        pagination_params: Dict[str, Any] = {}  # Empty on first request

        while True:
            data = self._get_data(**pagination_params)
            if not data:
                break

            self._load_data(data, self._load_to, page)

            if self._is_last_page(data):
                break

            # Update pagination parameters for the next request
            pagination_params = self._get_next_pagination(data)
            page += 1

    def _load_data(self, data: Dict, load_to: Path | str, page: int) -> None:
        """
        Save fetched API data to a file.

        The filename is dynamically generated based on the source, timestamp,
        and page number.

        Parameters
        ----------
        data : Dict
            The JSON data retrieved from the API.
        load_to : Path | str
            The destination directory or file path.
        page : int
            The page number (used for file naming).

        Raises
        ------
        Exception
            If an error occurs while writing the file.
        """
        filename = (
            f"{str(load_to)}/"
            f"{self._source_name}_"
            f"{self._source_surname}_"
            f"{datetime.now(timezone.utc).isoformat()}_"
            f"{page:03d}.json"
        )
        try:
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4)
        except Exception as e:
            print(f"Error saving file: {e}")
