"""
Test suite for file_tools.py

This module contains unit tests for file_tools.py using pytest.

Each function is tested for expected behavior, ensuring correctness and robustness.
"""

import os

from include.utils.file_tools import (
    create_temp_folder,
    delete_temp_folder,
    list_temp_folder,
)


class TestFileTools:
    """Test suite for file_tools module."""

    def test_create_temp_folder(self):
        """
        Test create_temp_folder function.

        Ensures that a temporary directory is created and is accessible.
        """
        temp_folder = create_temp_folder("test")
        assert os.path.exists(temp_folder.name), "Temporary directory should exist."
        delete_temp_folder(temp_folder)

    def test_delete_temp_folder(self):
        """
        Test delete_temp_folder function.

        Ensures that the temporary directory is properly deleted.
        """
        temp_folder = create_temp_folder("delete_test")
        temp_path = temp_folder.name
        delete_temp_folder(temp_folder)
        assert not os.path.exists(temp_path), "Temporary directory should be removed."

    def test_list_temp_folder_empty(self, tmp_path):
        """
        Test list_temp_folder function on an empty directory.

        Ensures that listing an empty directory returns an empty list.
        """
        assert list_temp_folder(tmp_path) == [], "List should be empty for new folder."

    def test_list_temp_folder_with_files(self, tmp_path):
        """
        Test list_temp_folder function when files exist.

        Ensures that listing a directory with files returns the correct file names.
        """
        filenames = ["test1.txt", "test2.log", "data.csv"]
        for filename in filenames:
            (tmp_path / filename).write_text("test content")

        listed_files = list_temp_folder(tmp_path)
        assert sorted(listed_files) == sorted(
            filenames
        ), "List should match created files."
