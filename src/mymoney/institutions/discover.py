import re
import logging

import numpy as np
import pandas as pd

from mymoney.institutions import institution_base


logging.basicConfig(
    level=logging.INFO,
    format="%(name)s\t[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%b/%d/%y %I:%M:%S %p",
)


class Discover(institution_base.Institution):
    """A class for Discover institution's data cleaning functions."""

    _this_institution_name = "discover"

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

            def is_transfer_finder(row):
                try:
                    regex_flag = re.search(r"PAYPAL", str(row["_new_Description"]))
                except Exception:
                    return "consider"

                if (row["_new_Description"] == "INTERNET PAYMENT - THANK YOU" or regex_flag):
                    return "transfer"
                elif (row["_new_Description"] != "INTERNET PAYMENT - THANK YOU" and not regex_flag):
                    return "expense"
                else:
                    return "consider"

            input_df["_new_Description"] = input_df["Description"].map(lambda val: str(val).strip())
            input_df["_new_Amount"] = -input_df["Amount"]
            input_df["_new_Date"] = input_df["Trans. Date"].copy(deep=True)
            input_df["_new_InstitutionCategory"] = input_df["Category"].copy(deep=True)
            input_df["_new_MyCategory"] = input_df["Category"].copy(deep=True)
            input_df["_new_Institution"] = pd.Series([f"Discover {account_name}"] * len(input_df))
            input_df["_new_IsTransfer"] = input_df.apply(is_transfer_finder, axis=1)

            return input_df
