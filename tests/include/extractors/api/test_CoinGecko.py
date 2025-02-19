"""
Test suite for CoinGeckoCoinsList extractor.

This module contains unit tests for the CoinGeckoCoinsList class,
ensuring correct API interaction, pagination logic, and data storage.

Tests use pytest and mock dependencies to simulate API responses.
"""

import json
from pathlib import Path
from typing import Any, Dict
from unittest.mock import MagicMock

import pytest

from include.extractors.api.CoinGecko import CoinGeckoCoinsList


class TestCoinGeckoCoinsList:
    """Test suite for the CoinGeckoCoinsList extractor."""

    @pytest.fixture
    def mock_api_response(self) -> Dict[str, Any]:
        """
        Fixture providing a sample API response.

        Returns
        -------
        Dict[str, Any]
            A sample response mimicking the CoinGecko 'coins/list' endpoint.
        """
        return {
            "data": [
                {"id": "bitcoin", "symbol": "btc", "name": "Bitcoin"},
                {"id": "ethereum", "symbol": "eth", "name": "Ethereum"},
            ]
        }

    @pytest.fixture
    def mock_params(self) -> Dict[str, Any]:
        """
        Fixture providing mock query parameters.

        Returns
        -------
        dict
            A dictionary containing sample query parameters.
        """
        return {"some_param": "test_value"}

    @pytest.fixture
    def extractor(self) -> CoinGeckoCoinsList:
        """
        Fixture providing an instance of CoinGeckoCoinsList.

        Returns
        -------
        CoinGeckoCoinsList
            An initialized instance of the extractor.
        """
        return CoinGeckoCoinsList()

    def test_get_data(
        self,
        mocker,
        extractor: CoinGeckoCoinsList,
        mock_api_response: Dict[str, Any],
        mock_params: Dict[str, Any],
        tmp_path: Path,
    ) -> None:
        """
        Test `_get_data()` method to ensure it fetches API data correctly.

        This test mocks API responses and verifies that `_get_data()`
        retrieves and returns expected data.

        Parameters
        ----------
        mocker : pytest_mock.MockerFixture
            Pytest mocker for patching dependencies.
        extractor : CoinGeckoCoinsList
            The extractor instance under test.
        mock_api_response : dict
            The mocked API response.
        """
        mocker.patch.object(extractor, "_open_session")
        mocker.patch.object(extractor, "_close_session")

        mock_session = mocker.MagicMock()
        mock_session.get.return_value = MagicMock(
            status_code=200, json=lambda: mock_api_response
        )

        extractor._session = mock_session

        extractor.start(params_query=mock_params, load_to=tmp_path)

        response = extractor._get_data()

        assert response == mock_api_response, "API response should match mock data."
        assert mock_session.get.call_count >= 1

    def test_is_last_page(
        self, extractor: CoinGeckoCoinsList, mock_api_response: Dict[str, Any]
    ) -> None:
        """
        Test `_is_last_page()` method to ensure correct pagination behavior.

        This method must always return `True`, as the CoinGecko 'coins/list'
        endpoint does not paginate.

        Parameters
        ----------
        extractor : CoinGeckoCoinsList
            The extractor instance under test.
        mock_api_response : dict
            The mocked API response.
        """
        assert (
            extractor._is_last_page(mock_api_response) is True
        ), "Endpoint should not paginate."

    def test_get_next_pagination(
        self, extractor: CoinGeckoCoinsList, mock_api_response: Dict[str, Any]
    ) -> None:
        """
        Test `_get_next_pagination()` method to ensure correct pagination parameters.

        Since the CoinGecko 'coins/list' endpoint does not support pagination,
        this method must always return an empty dictionary.

        Parameters
        ----------
        extractor : CoinGeckoCoinsList
            The extractor instance under test.
        mock_api_response : dict
            The mocked API response.
        """
        assert (
            extractor._get_next_pagination(mock_api_response) == {}
        ), "Pagination should be empty."

    def test_load_data(
        self,
        extractor: CoinGeckoCoinsList,
        mock_api_response: Dict[str, Any],
        tmp_path: Path,
    ) -> None:
        """
        Test `_load_data()` method to verify correct data storage.

        This test ensures that `_load_data()` saves the API response correctly
        in the specified output directory.

        Parameters
        ----------
        extractor : CoinGeckoCoinsList
            The extractor instance under test.
        mock_api_response : dict
            The mocked API response.
        tmp_path : Path
            Temporary directory for storing output files.
        """
        extractor._load_data(mock_api_response, tmp_path, page=1)

        # Verify file creation
        saved_files = list(tmp_path.iterdir())
        assert len(saved_files) == 1, "One file should be created."

        file_path = saved_files[0]

        # Verify file content
        with open(file_path, "r", encoding="utf-8") as f:
            saved_data = json.load(f)

        assert saved_data == mock_api_response, "Saved data should match API response."
