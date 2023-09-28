import pytest

from mymoney.utils.data_validation import SeriesValidation  # noqa: F401
from mymoney.utils.data_validation import DataFrameValidation  # noqa: F401


def test_has_dtype(
    series_bool, series_string,
    series_int, series_float, series_complex,
    series_datetime, series_timedelta,
):
    to_test_tuples = [
        (series_bool, "bool"),
        (series_string, "string"),
        (series_int, "int"),
        (series_float, "float"),
        (series_complex, "complex"),
        (series_datetime, "datetime"),
        (series_timedelta, "timedelta64"),
    ]
    for ser, dtype in to_test_tuples:
        assert ser.validate._check_dtype(dtype) is True

    with pytest.raises(Exception):
        series_int.validate.has_dtype("datetime", raises=True)


def test_has_no_x(range_series):
    assert range_series.validate._check_no_x([1, 2, "a"]) is True
    assert range_series.validate._check_no_x([3, 4]) == [0, 1]


def test_is_shape(dataframe_creator):
    df_val = DataFrameValidation(dataframe_creator)
    with pytest.raises(Exception):
        df_val.is_shape((3, 4), raises=True)
