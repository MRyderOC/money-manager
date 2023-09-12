import pytest

from mymoney.utils.common import column_name_checker
from mymoney.utils.exceptions import DifferentColumnNameException


def test_column_name_checker_equal(
    dataframe_creator, equal_column, superset_column
):
    column_name_checker(dataframe_creator, equal_column, "equal")
    with pytest.raises(DifferentColumnNameException):
        column_name_checker(dataframe_creator, superset_column, "equal")


def test_column_name_checker_subset(
    dataframe_creator, subset_column, superset_column
):
    column_name_checker(dataframe_creator, superset_column, "subset")
    with pytest.raises(DifferentColumnNameException):
        column_name_checker(dataframe_creator, subset_column, "subset")


def test_column_name_checker_superset(
    dataframe_creator, subset_column, superset_column
):
    column_name_checker(dataframe_creator, subset_column, "superset")
    with pytest.raises(DifferentColumnNameException):
        column_name_checker(dataframe_creator, superset_column, "superset")
