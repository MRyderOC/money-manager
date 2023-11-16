import pytest

from mymoney.utils.data_validation import SeriesValidation  # noqa: F401
from mymoney.utils.data_validation import DataFrameValidation  # noqa: F401


# SeriesValidation related tests

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


def test_has_no_x(series_int):
    assert series_int.validate._check_no_x([1, 2, "a"]) is True
    assert series_int.validate._check_no_x([3, 4]) == [0, 1]


def test_has_vals_range(series_int):
    assert series_int.validate._check_vals((1, 15), "range") is True
    assert series_int.validate._check_vals(
        (5, 10), "range").get("idxs") == [0, 1, 8, 9]
    with pytest.raises(Exception):
        series_int.validate.has_vals((5, 10), "range", raises=True)


def test_has_vals_regex(series_string, series_string2):
    assert series_string.validate._check_vals(r"a*", "regex") is True
    assert series_string.validate._check_vals(
        r"aa+", "regex").get("idxs") == [0, 1]
    assert series_string2.validate._check_vals(r"[a-z]", "regex") is True
    assert series_string2.validate._check_vals(
        r"[a-y]", "regex").get("idxs") == [25]
    with pytest.raises(Exception):
        series_string.validate.has_vals(r"aa+", "regex", raises=True)
        series_string2.validate.has_vals(r"[a-y]", "regex", raises=True)


def test_has_vals_equal(series_bool):
    assert series_bool.validate._check_vals([True, False], "equal") is True
    assert series_bool.validate._check_vals(
        [True], "equal").get("idxs") == [2, 4, 7, 9]
    with pytest.raises(Exception):
        series_bool.validate.has_vals([True], "equal", raises=True)


def test_has_vals_subset(series_bool, series_string, series_string2):
    assert series_bool.validate._check_vals([True], "subset") is True
    assert series_bool.validate._check_vals(
        [True, None], "subset").get("extra_vals") == [None]
    assert series_string.validate._check_vals(
        ["a", "aa", "aaa"], "subset") is True
    assert series_string.validate._check_vals(
        ["a", "aa", "b"], "subset").get("extra_vals") == ["b"]
    assert series_string2.validate._check_vals(
        ["a", "b", "c"], "subset") is True
    assert series_string2.validate._check_vals(
        ["a", "b", "qq"], "subset").get("extra_vals") == ["qq"]
    with pytest.raises(Exception):
        series_bool.validate.has_vals(
            [True, False, None], "subset", raises=True)
        series_string.validate.has_vals(
            ["a", "aa", "b"], "subset", raises=True)
        series_string2.validate.has_vals(
            ["a", "b", "qq"], "subset", raises=True)


def test_has_vals_superset(series_bool, series_string, series_int):
    assert series_bool.validate._check_vals(
        [True, False, None], "superset") is True
    assert series_bool.validate._check_vals(
        [True, None], "superset").get("idxs") == [2, 4, 7, 9]
    assert series_string.validate._check_vals(
        ["a" * i for i in range(20)], "superset") is True
    assert series_string.validate._check_vals(
        ["a" * i for i in range(8)], "superset").get("idxs") == [8, 9]
    assert series_int.validate._check_vals(
        [i for i in range(15)], "superset") is True
    assert series_int.validate._check_vals(
        [i for i in range(5, 10)], "superset").get("idxs") == [0, 1, 7, 8, 9]
    with pytest.raises(Exception):
        series_bool.validate.has_vals(
            [True, False, None], "superset", raises=True)
        series_string.validate.has_vals(
            ["a" * i for i in range(8)], "superset", raises=True)
        series_int.validate.has_vals(
            [i for i in range(5, 10)], "superset", raises=True)


# DataFrameValidation related tests
def test_is_shape(dataframe_creator):
    df_val = DataFrameValidation(dataframe_creator)
    with pytest.raises(Exception):
        df_val.is_shape((3, 4), raises=True)


def test_has_dtypes(dataframe_with_different_types):
    df_val = DataFrameValidation(dataframe_with_different_types)
    dtypes_map_correct = {
        "series_int": "int",
        "series_string": "string",
        "series_bool": "bool",
        "series_float": "float",
        "series_complex": "complex",
        "series_datetime": "datetime",
        "series_timedelta": "timedelta64",
    }
    df_val.has_dtypes(dtypes_map_correct)

    dtypes_map_false = {
        "series_int": "float",
        "series_string": "string",
        "series_bool": "bool",
        "series_float": "int",
        "series_complex": "complex",
        "series_datetime": "datetime",
        "series_timedelta": "timedelta64",
    }
    with pytest.raises(Exception):
        df_val.has_dtypes(dtypes_map_false, raises=True)


def test_has_schema(dataframe_with_different_types):
    df_val = DataFrameValidation(dataframe_with_different_types)
    schema_map_correct = {
        "series_int": "int",
        "series_string": "string",
        "series_bool": "bool",
        "series_float": "float",
        "series_complex": "complex",
        "series_datetime": "datetime",
        "series_timedelta": "timedelta64",
    }
    df_val.has_schema(schema_map_correct)

    schema_map_false = {
        "series_int": "float",
        "series_string": "string",
        "series_bool": "bool",
        "series_float": "int",
        "series_complex": "complex",
        "series_datetime": "datetime",
        "series_timedelta": "timedelta64",
    }
    with pytest.raises(Exception):
        df_val.has_schema(schema_map_false, raises=True)
