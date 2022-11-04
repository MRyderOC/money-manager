import re
import logging

import numpy as np
import pandas as pd

from mymoney.institutions import institution_base


logging.basicConfig(
    level=logging.INFO,
    format="%(name)s\t[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%b/%d/%y %I:%M:%S %p",
    # filename="logs.log",
)


class PayPal(institution_base.Institution):
    """docs here!"""

    _this_institution_name = "paypal"

    def __init__(self) -> None:
        super().__init__()


    def _third_party_cleaning(
        self, input_df: pd.DataFrame, account_name: str
    ) -> pd.DataFrame:
        """docs here!"""

        def is_transfer_finder(row):
            name_is_nan = pd.isna(row["Name"])
            try:
                regex_flag_redundant = re.search(
                    "Authorization|Order", str(row["Type"])
                )
            except Exception:
                return "consider"

            if regex_flag_redundant:
                return "redundant"
            elif name_is_nan:
                return "transfer"
            elif not(name_is_nan or regex_flag_redundant):
                return "expense"
            else:
                return "consider"

        def description_finder(row):
            if pd.isna(row["Name"]):
                return str(row["Type"])
            else:
                return f"{str(row['Name'])}: {row['Type']}"

        def amount_finder(val):
            return float(str(val).replace(",", ""))


        input_df["_new_Description"] = input_df.apply(description_finder, axis=1)
        input_df["_new_Amount"] = input_df["Amount"].map(amount_finder)
        input_df["_new_Date"] = input_df["Date"].copy(deep=True)
        input_df["_new_InstitutionCategory"] = pd.Series([np.nan] * len(input_df))
        input_df["_new_MyCategory"] = pd.Series([np.nan] * len(input_df))
        input_df["_new_Institution"] = pd.Series([f"PayPal {account_name}"] * len(input_df))
        input_df["_new_IsTransfer"] = input_df.apply(is_transfer_finder, axis=1)

        return input_df
