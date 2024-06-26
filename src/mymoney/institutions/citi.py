import logging
import re

import numpy as np
import pandas as pd

from mymoney.institutions import institution_base


logging.basicConfig(
    level=logging.INFO,
    format="%(name)s\t[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%b/%d/%y %I:%M:%S %p",
)


class Citi(institution_base.Institution):
    """A class for Citi institution's data cleaning functions."""

    _this_institution_name = "citi"

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
            # # row["_new_Description"].startswith("Thankyou Points Redeemed"): for point redeem # noqa: E501

            def amount_finder(row):
                try:
                    regex_flag = re.search(
                        r"AUTOPAY|ONLINE PAYMENT, THANK YOU|"
                        r"Thankyou Points Redeemed",
                        str(row["_new_Description"])
                    )
                except Exception:
                    return "consider"

                if regex_flag or np.isnan(row["Debit"]):
                    return -row["Credit"]
                else:
                    return -row["Debit"]

            def is_transfer_finder(row):
                try:
                    regex_flag = re.search(
                        r"AUTOPAY|ONLINE PAYMENT, THANK YOU",
                        str(row["_new_Description"])
                    )
                except Exception:
                    return "consider"

                if regex_flag:
                    return (
                        "consider" if np.isnan(row["Credit"]) else "transfer"
                    )
                else:
                    return "expense"

            input_df["_new_Description"] = input_df["Description"].map(lambda val: str(val).strip())  # noqa: E501
            input_df["_new_Amount"] = input_df.apply(amount_finder, axis=1)
            input_df["_new_Date"] = input_df["Date"].copy(deep=True)
            input_df["_new_InstitutionCategory"] = np.nan
            input_df["_new_MyCategory"] = np.nan
            input_df["_new_Institution"] = "Citi"
            input_df["_new_AccountName"] = account_name
            input_df["_new_Service"] = self._service_type.value
            input_df["_new_IsTransfer"] = input_df.apply(is_transfer_finder, axis=1)  # noqa: E501

            return input_df
