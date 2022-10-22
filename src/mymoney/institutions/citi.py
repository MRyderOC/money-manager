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


class Citi(institution_base.Institution):
    """docs here!"""

    _this_institution_name = "citi"

    def __init__(self) -> None:
        super().__init__()


    def _credit_cleaning(
        self, input_df: pd.DataFrame, account_name: str
    ) -> pd.DataFrame:
        """docs here!"""

        def amount_finder(row):
            return row["Credit"] if np.isnan(row["Debit"]) else -row["Debit"]

        def is_transfer_finder(row):
            if not np.isnan(row["Credit"]):
                return "transfer"
            elif not np.isnan(row["Debit"]):
                return "expense"
            else:
                return "consider"

        input_df["_new_Description"] = input_df["Description"].copy(deep=True)
        input_df["_new_Amount"] = input_df.apply(amount_finder, axis=1)
        input_df["_new_Date"] = input_df["Date"].copy(deep=True)
        input_df["_new_InstitutionCategory"] = pd.Series([np.nan] * len(input_df))
        input_df["_new_MyCategory"] = pd.Series([np.nan] * len(input_df))
        input_df["_new_Institution"] = pd.Series([f"Citi {account_name}"] * len(input_df))
        input_df["_new_IsTransfer"] = input_df.apply(is_transfer_finder, axis=1)
        # TODO: input_df["_new_IsCompatible"] =

        return input_df
