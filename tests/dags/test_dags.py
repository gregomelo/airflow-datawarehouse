"""
Test suite for DAG validation and execution.

This module contains unit tests for Apache Airflow DAGs using pytest.

Each DAG is tested for expected behavior, ensuring correctness, proper configuration,
and absence of import errors.
"""

import logging
import os
from contextlib import contextmanager

import pytest
from airflow.models import DagBag


@contextmanager
def suppress_logging(namespace: str):
    """Context manager to suppress logging in the given namespace."""
    logger = logging.getLogger(namespace)
    old_value = logger.disabled
    logger.disabled = True
    try:
        yield
    finally:
        logger.disabled = old_value


def get_dags():
    """
    Retrieve all DAGs from the DAGBag.

    Returns
    -------
    list[tuple[str, airflow.DAG, str]]
        A list of tuples containing DAG IDs, DAG objects, and file locations.
    """
    dags_folder = os.getenv("AIRFLOW__CORE__DAGS_FOLDER", "dags")

    with suppress_logging("airflow"):
        dag_bag = DagBag(dag_folder=dags_folder, include_examples=False)

    def strip_path_prefix(path: str) -> str:
        return os.path.relpath(path, os.environ.get("AIRFLOW_HOME"))

    return sorted(
        [
            (dag_id, dag, strip_path_prefix(dag.fileloc))
            for dag_id, dag in dag_bag.dags.items()
        ],
        key=lambda x: x[2],
    )


@pytest.mark.parametrize("dag_fixture", get_dags(), ids=[dag[2] for dag in get_dags()])
class TestDAGs:
    """Parameterized test suite for Apache Airflow DAGs, grouped by DAG."""

    def test_dag_import(self, dag_fixture) -> None:
        """Test if the DAG is imported successfully."""
        dag_id, dag, fileloc = dag_fixture
        assert dag is not None, f"{dag_id} in {fileloc} failed to load."

    def test_dag_execution(self, dag_fixture) -> None:
        """Test if the DAG can be loaded and validated without executing tasks."""
        dag_id, dag, fileloc = dag_fixture
        try:
            dag.validate()
        except Exception as e:
            pytest.fail(f"DAG {dag_id} in {fileloc} failed validation: {e}")

    @pytest.mark.skip(reason="Only tasks to extract or load will have retries setup")
    def test_dag_retries(self, dag_fixture) -> None:
        """Test if the DAG has at least 2 retries configured."""
        dag_id, dag, fileloc = dag_fixture
        assert (
            dag.default_args.get("retries", None) >= 2
        ), f"{dag_id} in {fileloc} must have task retries >= 2."

    @pytest.mark.skip(reason="Feature tags not implemented")
    def test_dag_tags(self, dag_fixture) -> None:
        """Test if the DAG has at least one tag."""
        dag_id, dag, fileloc = dag_fixture
        assert dag.tags, f"{dag_id} in {fileloc} has no tags"
