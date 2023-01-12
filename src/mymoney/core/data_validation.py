import logging
from typing import (
    Any, List, Tuple, Dict, Union
)

import numpy as np
import pandas as pd


logging.basicConfig(
    level=logging.INFO,
    format="%(name)s\t[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%b/%d/%y %I:%M:%S %p",
    # filename="logs.log",
)


class SeriesValidation:
    """A class for validating specific criteria of a Series."""

    def __init__(self, pandas_obj):
        self._obj = pandas_obj


    def _raise_or_log(
        self,
        message: str,
        logs: bool = True,
        raises: bool = False,
        exception_type = Exception,
    ):
        """Raise an exception or log a message.

        Args:
            message (str):
                The message that should be attached.
            logs (bool):
                Whether to log the results if something went wrong.
            raises (bool):
                Whether to raise an error or not.
            exception_type:
                The exception that should be raised.
        """
        if logs:
            logging.warning(message)
        if raises:
            raise exception_type(message)


    def _find_faulty_indexes(self, ser: pd.Series) -> List:
        """Find the indexes that cause problem.

        Args:
            ser (pd.Series):
                A pandas Series to evaluate.
                If the series contain bool values, it returns the indexes with
                  False values otherwise it returns all of the indexes.

        Returns:
            A list of indexes
        """
        if not pd.api.types.is_bool_dtype(ser):
            return list(ser.index)

        return list(ser[ser == False].index)


    def _check_dtype(self, dtype: str) -> bool:
        """For a given Series specifies if elements have dtypes.
        Args:
            ser (pd.Series): Any Series
            dtype (str): Accepted values: [
                    'object', 'bool', ('string', 'str'),
                # number related
                    'numeric', 'float', 'complex', 'int', 'int64',
                # signed or unsigned int
                    ('signed_int' or 'signed-int' or 'signed int' or 'signedint' or 'sint'),
                    ('unsigned_int' or 'unsigned-int' or 'unsigned int' or 'unsignedint' or 'uint'),
                # time related
                    'datetime', 'datetime64', 'datetime64_ns', 'datetime64_tz',
                    'timedelta64', 'timedelta64_ns'
            ]

        Returns:
            A boolean value
        """
        if dtype == "object":
            result = pd.api.types.is_object_dtype(self._obj)
        elif dtype == "bool":
            result = pd.api.types.is_bool_dtype(self._obj)
        elif dtype in ["string", "str"]:
            result = pd.api.types.is_string_dtype(self._obj)
        elif dtype == "numeric":
            result = pd.api.types.is_numeric_dtype(self._obj)
        elif dtype == "float":
            result = pd.api.types.is_float_dtype(self._obj)
        elif dtype == "complex":
            result = pd.api.types.is_complex_dtype(self._obj)
        elif dtype == "int":
            result = pd.api.types.is_integer_dtype(self._obj)
        elif dtype == "int64":
            result = pd.api.types.is_int64_dtype(self._obj)
        elif dtype in ["signed_int", "signed-int", "signed int", "signedint", "sint"]:
            result = pd.api.types.is_signed_integer_dtype(self._obj)
        elif dtype in ["unsigned_int", "unsigned-int", "unsigned int", "unsignedint", "uint"]:
            result = pd.api.types.is_unsigned_integer_dtype(self._obj)
        elif dtype == "datetime":
            result = pd.api.types.is_datetime64_any_dtype(self._obj)
        elif dtype == "datetime64":
            result = pd.api.types.is_datetime64_dtype(self._obj)
        elif dtype == "datetime64_ns":
            result = pd.api.types.is_datetime64_ns_dtype(self._obj)
        elif dtype == "datetime64tz":
            result = pd.api.types.is_datetime64tz_dtype(self._obj)
        elif dtype == "timedelta64":
            result = pd.api.types.is_timedelta64_dtype(self._obj)
        elif dtype == "timedelta64_ns":
            result = pd.api.types.is_timedelta64_ns_dtype(self._obj)
        else:
            raise ValueError(
                f"This function doesn't support for `{dtype}` checking."
            )

        return result


    def _check_no_x(self, values: List):
        """Check whether the Series doesn't contain `values`.

        Args:
            values (List):
                A list of values to check for in the Series.
        """
        new_ser = self._obj.isin(values)

        if new_ser.any():
            return self._find_faulty_indexes(~new_ser)
        return True


    def _check_vals(
        self, values, mode: str
    ) -> Union[bool, Dict[str, List[int]]]:
        """Check whether the Series contains `values`.

        Args:
            values (List):
                A list of values to check for in the Series.
            mode (str):
                Accepted values: [
                    'range' -> values of the Series are in the range of `values`,
                    'regex' -> values of the Series have the `values`
                        regex pattern,
                    ('n_std' or 'n-std' or 'n std') -> values of the Series
                        are within `values` standard devation of the mean,
                    'equal' -> values of the Series are exactly like `values`,
                    'subset' -> `values` is subset of values in the Series,
                    'superset' -> `values` is superset of values in the Series,
                ]
        """
        if mode == "range":
            # Type error checking
            if not len(values) == 2:
                raise Exception(
                    "For mode=range the `values` should be"
                    " a list or tuple with 2 elements."
                )

            new_ser = self._obj[
                (self._obj < values[0]) | (self._obj > values[1])
            ]
            if not new_ser.empty:
                return {"idxs": self._find_faulty_indexes(new_ser)}
            return True
        elif mode == "regex":
            # Regex compilable type error checking
            if not pd.api.types.is_re_compilable(values):
                raise ValueError("`values` is not a regex compilable string.")

            new_ser = self._obj.astype(str).str.contains(values, na=False, regex=True)
            if not new_ser.all():
                return {"idxs": self._find_faulty_indexes(new_ser)}
            return True
        elif mode in ["n_std", "n-std", "n std"]:
            # Type error checking
            if not pd.api.types.is_numeric_dtype(self._obj):
                raise Exception("This series does not contain numeric values.")
            if not isinstance(values, (int, float)):
                raise ValueError(
                    "`values` for mode 'n-std' should be int or float."
                )

            mean, std = self._obj.mean(), self._obj.std()
            dist_from_mean_ser = np.abs(self._obj - mean)
            outliers = dist_from_mean_ser[dist_from_mean_ser > values * std]
            if not outliers.empty:
                return {"idxs": self._find_faulty_indexes(outliers)}
            return True
        elif mode in ["equal", "subset", "superset"]:
            # Type error checking
            if not isinstance(values, (list, set, np.ndarray)):
                raise ValueError(
                    "`values` should be one of the following types:"
                    " 'list', 'set', 'np.ndarray'."
                )

            ser_unique_vals_set = set(self._obj.unique())
            vals_set = set(values)
            if mode == "equal":
                if vals_set != ser_unique_vals_set:
                    ser_extra = ser_unique_vals_set - vals_set
                    values_extra = vals_set - ser_unique_vals_set
                    return {
                        "idxs": self._find_faulty_indexes(~self._obj.isin(ser_extra)),
                        "extra_vals": list(values_extra)
                    }
                return True
            elif mode == "subset":
                if not vals_set.issubset(ser_unique_vals_set):
                    return {"extra_vals": list(vals_set - ser_unique_vals_set)}
                return True
            elif mode == "superset":
                if not vals_set.issuperset(ser_unique_vals_set):
                    return {"idxs": self._find_faulty_indexes(self._obj.isin(values))}
                return True
        else:
            raise Exception(
                "mode should be one of the following:"
                "\n['range', 'regex', ('n_std' or 'n-std' or 'n std'),"
                " 'equal', 'subset', 'superset']"
            )


    def has_dtype(
        self,
        dtype: str,
        logs: bool = True,
        raises: bool = False,
    ):
        """Specifies if the elements of the Series have `dtypes`.
        Args:
            dtype (str):
                Accepted values: [
                    'object', 'bool', 'string',
                # number related
                    'numeric', 'float', 'complex', 'int', 'int64',
                # signed or unsigned int
                    ('signed_int', 'signed-int', 'signed int', 'signedint', 'sint'),
                    ('unsigned_int', 'unsigned-int', 'unsigned int', 'unsignedint', 'uint'),
                # time related
                    'datetime', 'datetime64', 'datetime64_ns', 'datetime64_tz',
                    'timedelta64', 'timedelta64_ns'
                ]
            logs (bool):
                Whether to log the results if something went wrong.
            raises (bool):
                Whether to raise an error or not.
        """
        if not self._check_dtype(dtype):
            msg = (
                f"This series has the wrong dtype."
                f"\nShould be ({dtype}), but is ({self._obj.dtype})"
            )
            self._raise_or_log(msg, logs, raises, Exception)


    def has_no_x(
        self,
        values: List,
        logs: bool = True,
        raises: bool = False,
    ):
        """Check whether the Series doesn't contain `values`.

        Args:
            values (List):
                A list of values to check for in the Series.
            logs (bool):
                Whether to log the results if something went wrong.
            raises (bool):
                Whether to raise an error or not.
        """
        faulty_idxs = self._check_no_x(values)
        if not(faulty_idxs is True):
            msg = (
                "This series contains at least one of the elements in `values`."
                f"\nThe indexes: {faulty_idxs}"
            )
            self._raise_or_log(msg, logs, raises)


    def has_vals(
        self,
        values,
        mode: str,
        logs: bool = True,
        raises: bool = False,
    ):
        """Check whether the Series contains `values`.

        Args:
            values (List):
                A list of values to check for in the Series.
            mode (str):
                Accepted values: [
                    'range' -> values of the Series are in the range of `values`,
                    'regex' -> values of the Series have the `values`
                        regex pattern,
                    ('n_std' or 'n-std' or 'n std') -> values of the Series
                        are within `values` standard devation of the mean,
                    'equal' -> values of the Series are exactly like `values`,
                    'subset' -> `values` is subset of values in the Series,
                    'superset' -> `values` is superset of values in the Series,
                ]
            logs (bool):
                Whether to log the results if something went wrong.
            raises (bool):
                Whether to raise an error or not.
        """
        error_dict = self._check_vals(values, mode)
        if not(error_dict is True):
            msg = ""
            if mode == "range":
                msg = (
                    "Some of the values of this series are not in the range"
                    f" specified in `values`.\nThe indexes: {error_dict['idxs']}"
                )
            elif mode == "regex":
                msg = (
                    "Some of the values of this series doesn't match with"
                    f" the regex `{values}`.\nThe indexes: {error_dict['idxs']}"
                )
            elif mode in ["n_std", "n-std", "n std"]:
                msg = (
                    "Some of the values of this series are not within"
                    f" `{values}` of standard deviations."
                    f"\nThe indexes: {error_dict['idxs']}"
                )
            elif mode == "equal":
                msg = (
                    "The `values` is not equal to the values in the Series."
                    f"\nExtra values in `values`: {error_dict['extra_vals']}"
                    f"\nIndex of extra values in the series: {error_dict['idxs']}"
                )
            elif mode == "subset":
                msg = (
                    "The `values` is not subset of the values in"
                    f" the Series.\nExtra values: {error_dict['extra_vals']}"
                )
            elif mode == "superset":
                msg = (
                    "The `values` is not superset of the values in"
                    f" the Series.\nThe indexes: {error_dict['idxs']}"
                )

            self._raise_or_log(msg, logs, raises)


class DataFrameValidation:
    """A class for validating specific criteria of a DataFrame."""

    def __init__(self, pandas_obj):
        self._obj = pandas_obj

        @pd.api.extensions.register_series_accessor("validate")
        class MySerVal(SeriesValidation): ...


    def _raise_or_log(
        self,
        message: str,
        logs: bool = True,
        raises: bool = False,
        exception_type = Exception,
    ):
        """Raise an exception or log a message.

        Args:
            message (str):
                The message that should be attached.
            logs (bool):
                Whether to log the results if something went wrong.
            raises (bool):
                Whether to raise an error or not.
            exception_type:
                The exception that should be raised.
        """
        if logs:
            logging.warning(message)
        if raises:
            raise exception_type(message)


    def is_shape(
        self,
        shape: Tuple[int, int],
        logs: bool = True,
        raises: bool = False,
    ):
        """Speciefies the DataFrame has `shape`.

        Args:
            shape (Tuple[int, int]):
                Shape of the DataFrame as (n_rows, n_columns).
                Use -1 if you don't care about a specific dimension.
            logs (bool):
                Whether to log the results if something went wrong.
            raises (bool):
                Whether to raise an error or not.
        """
        # Error checking
        if not len(shape) == 2:
            raise Exception(
                "`shape` should be a list or tuple with 2 elements."
            )

        ax0_shape, ax1_shape = len(self._obj), len(self._obj.columns)

        msg = (
            f"\nExpected shape: {shape}"
            f"\nActual shape:{self._obj.shape}"
        )
        if shape[0] == -1:
            if ax1_shape != shape[1]:
                self._raise_or_log(msg, logs, raises, Exception)
        elif shape[1] == -1:
            if ax0_shape != shape[0]:
                self._raise_or_log(msg, logs, raises, Exception)
        else:
            if ax0_shape != shape[0] or ax1_shape != shape[1]:
                self._raise_or_log(msg, logs, raises, Exception)


    def has_schema(
        self,
        schema: Dict[str, str],
        logs: bool = True,
        raises: bool = False,
    ):
        """Check whether the DataFrame has `schema`.

        Args:
            schema (Dict[str, str])
                Mapping of columns to dtypes as string.
            logs (bool):
                Whether to log the results if something went wrong.
            raises (bool):
                Whether to raise an error or not.
        """
        for col, dtype in schema.items():
            if col not in self._obj.columns:
                continue
            if not self._obj[col].validate._check_dtype(dtype):
                msg = (
                    f"\n{col} has the wrong dtype."
                    f"\nShould be ({dtype}), is ({self._obj[col].dtype})"
                )
                self._raise_or_log(msg, logs, raises, Exception)


    def has_dtypes(
        self,
        dtypes_dict: Dict[str, str],
        logs: bool = True,
        raises: bool = False,
    ):
        """Same as `has_schema`.

        Args:
            schema (Dict[str, str])
                Mapping of columns to dtypes as string.
            logs (bool):
                Whether to log the results if something went wrong.
            raises (bool):
                Whether to raise an error or not.
        """
        self.has_schema(dtypes_dict, logs, raises)


    def has_vals(
        self,
        col_vals_dict: Dict[str, Dict[str, Any]],
        return_validation_col: bool = True,
        logs: bool = True,
        raises: bool = False,
    ):
        """Check whether the DataFrame columns contains specific.

        Args:
            col_vals_dict (Dict[str, Dict[str, Any]]):
                A dictionary with keys as column names and values should be
                a dictionary like
                  {"vlaues": `desired value`, "mode": `desired mode`}
                Refer to `has_vals` method in `SeriesValidation` for more info.
            validation_column (bool):
                Create `is_valid` column in the DataFrame with bool values to
                find the not valid rows.
            logs (bool):
                Whether to log the results if something went wrong.
            raises (bool):
                Whether to raise an error or not.
        """
        faulty_idxs = []
        for col, val_args in col_vals_dict.items():
            if col not in self._obj.columns:
                continue
            vals_error = self._obj[col].validate._check_vals(**val_args)
            if not(vals_error is True):
                faulty_idxs.extend(vals_error["idxs"])
                msg = (
                    f"{col} has the wrong value."
                    f"\nShould be ({val_args['values']})"
                    f"\nErrors: {vals_error}\n"
                )
                self._raise_or_log(msg, logs, raises, Exception)

        if return_validation_col:
            faulty_idxs_unique = list(set(faulty_idxs))
            # Create a Series based on `faulty_idxs_unique`
            return pd.Series({
                idx: False if idx in faulty_idxs_unique else True
                for idx in self._obj.index
            })
