import re
import logging

import numpy as np  # noqa: F401
import pandas as pd

from mymoney.institutions import institution_base


logging.basicConfig(
    level=logging.INFO,
    format="%(name)s\t[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%b/%d/%y %I:%M:%S %p",
)


class SamsClub(institution_base.Institution):
    """A class for SamsClub institution's data cleaning functions."""

    _this_institution_name = "samsclub"

    def __init__(self) -> None:
        super().__init__()

    class CreditService(institution_base.Institution.CreditService):
        """A class for Credit Service."""

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

            def is_transfer_finder(val):
                try:
                    regex_flag = re.search(
                        r"AUTOMATIC PAYMENT - THANK YOU", val
                    )
                except Exception:
                    return "consider"

                if regex_flag:
                    return "transfer"
                else:
                    return "expense"

            input_df["_new_Description"] = input_df["Description"].map(lambda val: str(val).strip())  # noqa: E501
            input_df["_new_Amount"] = input_df["Amount"].copy(deep=True)
            input_df["_new_Date"] = input_df["Transaction Date"].copy(deep=True)  # noqa: E501
            input_df["_new_InstitutionCategory"] = np.nan
            input_df["_new_MyCategory"] = np.nan
            input_df["_new_Institution"] = "SamsClub"
            input_df["_new_AccountName"] = account_name
            input_df["_new_Service"] = self._service_type.value
            input_df["_new_IsTransfer"] = input_df["Description"].map(is_transfer_finder)  # noqa: E501

            return input_df
