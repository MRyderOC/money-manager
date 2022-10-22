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


class Chase(institution_base.Institution):
    """docs here!"""

    _this_institution_name = "chase"

    def __init__(self) -> None:
        super().__init__()


    def _credit_cleaning(
        self, input_df: pd.DataFrame, account_name: str
    ) -> pd.DataFrame:
        """docs here!"""

        def is_transfer_finder(val):
            if val == "Sale":
                return "expense"
            elif val == "Payment":
                return "transfer"
            else:
                return "consider"


        input_df["_new_Description"] = input_df["Description"].copy(deep=True)
        input_df["_new_Amount"] = input_df["Amount"].copy(deep=True)
        input_df["_new_Date"] = input_df["Transaction Date"].copy(deep=True)
        input_df["_new_InstitutionCategory"] = input_df["Category"].copy(deep=True)
        input_df["_new_MyCategory"] = input_df["Category"].copy(deep=True)
        input_df["_new_Institution"] = pd.Series([f"Chase {account_name}"] * len(input_df))
        input_df["_new_IsTransfer"] = input_df["Type"].map(is_transfer_finder)
        # TODO: input_df["_new_IsCompatible"] =

        return input_df
