import logging
from typing import List

import pandas as pd

from mymoney.utils.exceptions import DifferentColumnNameException


logging.basicConfig(
    level=logging.INFO,
    format="%(name)s\t[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%b/%d/%y %I:%M:%S %p",
)


def raise_or_log(
    message: str,
    logs: bool = True,
    raises: bool = False,
    exception_type=Exception,
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


def column_name_checker(
    input_df: pd.DataFrame, columns: List[str], mode: str = "equal"
):
    """Check if the columns of `input_df`
    is subset, superset, or equal to `columns`.

    Args:
        input_df (pd.DataFrame):
            The input DataFrame.
        columns (List[str]):
            Columns that `input_df` columns will be compared to.
        mode (str):
            How to compare the columns. Accepted values:
                equal: `columns` and `input_df` columns have identical values.
                subset: `input_df` columns are subset of `columns`.
                superset: `input_df` columns are superset of `columns`.

    Raises:
        DifferentColumnNameException: if the columns don't match.
        ValueError: if mode is not one of the following:
            ['equal', 'subset', 'superset']
    """
    if mode == "equal":
        if set(input_df.columns) != set(columns):
            raise DifferentColumnNameException(
                "DataFrame columns are not matched with `columns`."
            )
    elif mode == "subset":
        if not set(input_df.columns).issubset(set(columns)):
            raise DifferentColumnNameException(
                "DataFrame columns are not subset of `columns`."
            )
    elif mode == "superset":
        if not set(input_df.columns).issuperset(set(columns)):
            raise DifferentColumnNameException(
                "DataFrame columns are not superset of `columns`."
            )
    else:
        raise ValueError(
            "`mode` should be one of the following:"
            " ['equal', 'subset', 'superset']"
        )
