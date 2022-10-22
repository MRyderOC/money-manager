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


class AmEx(institution_base.Institution):
    """docs here!"""

    _this_institution_name = "amex"

    def __init__(self) -> None:
        super().__init__()


    def _credit_cleaning(
        self, input_df: pd.DataFrame, account_name: str
    ) -> pd.DataFrame:
        """docs here!"""
        # row["_new_Description"] == "YOUR CASH REWARD/REFUND IS": for cash back payments
        # row["Extended Details"].startswith("Amex Offer Credit"): for offer reedems

        def is_transfer_finder(row):
            try:
                regex_flag_paypal = re.search(r"PAYPAL", str(row["_new_Description"]))
                regex_flag_payment = re.search(r"\w* PAYMENT - THANK YOU", str(row["_new_Description"]))
            except Exception:
                return "consider"

            if (regex_flag_payment or regex_flag_paypal):
                return "transfer"
            elif (not regex_flag_payment and not regex_flag_paypal):
                return "expense"
            else:
                return "consider"

        input_df["_new_Description"] = input_df["Description"].copy(deep=True)
        input_df["_new_Amount"] = -input_df["Amount"]
        input_df["_new_Date"] = input_df["Date"].copy(deep=True)
        input_df["_new_InstitutionCategory"] = input_df["Category"].copy(deep=True)
        input_df["_new_MyCategory"] = input_df["Category"].copy(deep=True)
        input_df["_new_Institution"] = pd.Series([f"AmEx {account_name}"] * len(input_df))
        input_df["_new_IsTransfer"] = input_df.apply(is_transfer_finder, axis=1)
        # TODO: input_df["_new_IsCompatible"] =

        return input_df
