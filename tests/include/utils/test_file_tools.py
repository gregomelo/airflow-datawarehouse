"""
Test suite for file_tools.py

This module contains unit tests for file_tools.py using pytest.

Each function is tested for expected behavior, ensuring correctness and robustness.
"""

import os
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from include.utils.file_tools import (
    create_temp_folder,
    delete_temp_folder,
    list_temp_folder,
    storage_path,
)


class TestFileTools:
    """Test suite for file_tools module."""

    def test_create_temp_folder(self) -> None:
        """
        Test create_temp_folder function.

        Ensures that a temporary directory is created and is accessible.
        """
        temp_folder: TemporaryDirectory = create_temp_folder("test")
        assert os.path.exists(temp_folder.name), "Temporary directory should exist."
        delete_temp_folder(temp_folder.name)

    def test_delete_temp_folder(self) -> None:
        """
        Test delete_temp_folder function.

        Ensures that the temporary directory is properly deleted.
        """
        temp_folder: TemporaryDirectory = create_temp_folder("delete_test")
        temp_path: str = temp_folder.name
        delete_temp_folder(temp_path)
        assert not os.path.exists(temp_path), "Temporary directory should be removed."

    def test_list_temp_folder_empty(self, tmp_path: Path) -> None:
        """
        Test list_temp_folder function on an empty directory.

        Ensures that listing an empty directory returns an empty list.
        """
        assert list_temp_folder(tmp_path) == [], "List should be empty for new folder."

    def test_list_temp_folder_with_files(self, tmp_path: Path) -> None:
        """
        Test list_temp_folder function when files exist.

        Ensures that listing a directory with files returns the correct file names.
        """
        filenames: list[str] = ["test1.txt", "test2.log", "data.csv"]
        for filename in filenames:
            (tmp_path / filename).write_text("test content")

        listed_files: list[str] = list_temp_folder(tmp_path)
        assert sorted(listed_files) == sorted(
            filenames
        ), "List should match created files."

    @pytest.mark.parametrize(
        "layer, source_name, source_surname, expected",
        [
            ("bronze", "sales", "transactions", "bronze/sales/transactions"),
            ("silver", "customers", "data", "silver/customers/data"),
            ("gold", "finance", "reports", "gold/finance/reports"),
            ("raw", "logs", "server", "raw/logs/server"),
            ("processed", "events", "clicks", "processed/events/clicks"),
        ],
    )
    def test_storage_path(
        self, layer: str, source_name: str, source_surname: str, expected: str
    ) -> None:
        """
        Test that the storage_path function correctly formats the storage path.

        Parameters
        ----------
        layer : str
            The storage layer.
        source_name : str
            The name of the data source.
        source_surname : str
            The secondary name of the data source.
        expected : str
            The expected output path.
        """
        result = storage_path(layer, source_name, source_surname)
        assert result == expected, f"Expected '{expected}', but got '{result}'."
