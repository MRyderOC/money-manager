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


def column_name_checker(input_df: pd.DataFrame, columns: List[str]):
    """Check if the columns of `input_df` is identical to `columns`.

    Args:
        input_df (pd.DataFrame):
            The input DataFrame.
        columns (List[str]):
            Columns that `input_df` columns will be compared to.

    Raises:
        DifferentColumnNameException: if the columns don't match.
    """
    if set(input_df.columns) != set(columns):
        raise DifferentColumnNameException(
            "DataFrame columns are not matched with `columns`."
        )
