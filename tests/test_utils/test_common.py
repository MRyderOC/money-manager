import pytest

from mymoney.utils.common import column_name_checker
from mymoney.utils.common import raise_or_log
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


def test_raise_or_log_with_Exception():
    with pytest.raises(Exception) as exception_info:
        msg = "Exception message!"
        raise_or_log(
            message=msg, logs=False, raises=True, exception_type=Exception
        )
    assert "Exception message!" in str(exception_info.value)


def test_raise_or_log_with_ValueError():
    with pytest.raises(ValueError) as exception_info:
        msg = "ValErr message!"
        raise_or_log(
            message=msg, logs=False, raises=True, exception_type=ValueError
        )
    assert "ValErr message!" in str(exception_info.value)


def test_raise_or_log_with_OSError():
    with pytest.raises(OSError) as exception_info:
        msg = "OSError message!"
        raise_or_log(
            message=msg, logs=False, raises=True, exception_type=OSError
        )
    assert "OSError message!" in str(exception_info.value)
