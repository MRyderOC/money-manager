import logging

import numpy as np
import pandas as pd

from mymoney.institutions import institution_base


logging.basicConfig(
    level=logging.INFO,
    format="%(name)s\t[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%b/%d/%y %I:%M:%S %p",
)


class TDAmeriTrade(institution_base.Institution):
    """A class for TD AmeriTrade institution's data cleaning functions."""

    _this_institution_name = "tdameritrtade"

    def __init__(self) -> None:
        super().__init__()

    class ExchangeService(institution_base.Institution.ExchangeService):
        """A class for Exchange Service."""

        def _csv_cleaning(
            self, input_df: pd.DataFrame, account_name: str
        ) -> pd.DataFrame:
            """A method for cleaning process of CSV files for this service.

            Args:
                input_df (pd.DataFrame):
                    The input DataFrame.
                account_name (str):
                    The name of the account associated with this service.

            Returns:
                The same DataFrame with new columns for cleaned data.
            """
            ...
