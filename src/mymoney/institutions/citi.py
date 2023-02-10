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
        """docs here!"""


        def _cleaning(
            self, input_df: pd.DataFrame, account_name: str
        ) -> pd.DataFrame:
            """docs here!"""
            # # row["_new_Description"].startswith("Thankyou Points Redeemed"): for point redeem

            def amount_finder(row):
                try:
                    regex_flag = re.search("AUTOPAY|ONLINE PAYMENT, THANK YOU", str(row["_new_Description"]))
                except Exception:
                    return "consider"

                if regex_flag:
                    return row["Credit"]
                elif np.isnan(row["Debit"]):
                    return -row["Credit"] 
                else:
                    return -row["Debit"]

            def is_transfer_finder(row):
                try:
                    regex_flag = re.search("AUTOPAY|ONLINE PAYMENT, THANK YOU", str(row["_new_Description"]))
                except Exception:
                    return "consider"

                if regex_flag:
                    return "consider" if np.isnan(row["Credit"]) else "transfer"
                else:
                    return "expense"


            input_df["_new_Description"] = input_df["Description"].map(lambda val: str(val).strip())
            input_df["_new_Amount"] = input_df.apply(amount_finder, axis=1)
            input_df["_new_Date"] = input_df["Date"].copy(deep=True)
            input_df["_new_InstitutionCategory"] = pd.Series([np.nan] * len(input_df))
            input_df["_new_MyCategory"] = pd.Series([np.nan] * len(input_df))
            input_df["_new_Institution"] = pd.Series([f"Citi {account_name}"] * len(input_df))
            input_df["_new_IsTransfer"] = input_df.apply(is_transfer_finder, axis=1)

            return input_df
