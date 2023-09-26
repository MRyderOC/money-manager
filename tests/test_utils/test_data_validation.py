import pytest

from mymoney.utils.data_validation import SeriesValidation  # noqa: F401
from mymoney.utils.data_validation import DataFrameValidation  # noqa: F401


def test_has_dtype(range_series):
    assert range_series.validate._check_dtype("int") is True
    with pytest.raises(Exception):
        range_series.validate.has_dtype("datetime", raises=True)


def test_has_no_x(range_series):
    assert range_series.validate._check_no_x([1, 2, "a"]) is True
    assert range_series.validate._check_no_x([3, 4]) == [0, 1]


def test_is_shape(dataframe_creator):
    df_val = DataFrameValidation(dataframe_creator)
    with pytest.raises(Exception):
        df_val.is_shape((3, 4), raises=True)
