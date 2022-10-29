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


class CapitalOne(institution_base.Institution):
    """docs here!"""

    _this_institution_name = "capitalone"

    def __init__(self) -> None:
        super().__init__()


    def _credit_cleaning(
        self, input_df: pd.DataFrame, account_name: str
    ) -> pd.DataFrame:
        """docs here!"""
        # row["_new_Description"] == "CREDIT-CASH BACK REWARD": for cash back payments
        # row["_new_Description"] == "AU BONUS": for bonuses

        def institution_finder(val):
            return "Capital One " + str(val)

        def amount_finder(row):
            return row["Credit"] if np.isnan(row["Debit"]) else -row["Debit"]

        def is_transfer_finder(row):
            try:
                regex_flag_paypal = re.search(r"PAYPAL", str(row["_new_Description"]))
                regex_flag_payment = re.search(r"CAPITAL ONE \w* PYMT", str(row["_new_Description"]))
            except Exception:
                return "consider"

            if (regex_flag_payment or regex_flag_paypal):
                return "transfer"
            elif (not regex_flag_payment and not regex_flag_paypal):
                return "expense"
            else:
                return "consider"


        input_df["_new_Description"] = input_df["Description"].copy(deep=True)
        input_df["_new_Amount"] = input_df.apply(amount_finder, axis=1)
        input_df["_new_Date"] = input_df["Transaction Date"].copy(deep=True)
        input_df["_new_InstitutionCategory"] = input_df["Category"].copy(deep=True)
        input_df["_new_MyCategory"] = input_df["Category"].copy(deep=True)
        input_df["_new_Institution"] = input_df["Card No."].map(institution_finder)
        input_df["_new_IsTransfer"] = input_df.apply(is_transfer_finder, axis=1)
        # TODO: input_df["_new_IsCompatible"] =

        return input_df


    def _debit_cleaning(
        self, input_df: pd.DataFrame, account_name: str
    ) -> pd.DataFrame:
        """docs here!"""

        def institution_finder(val):
            return "Capital One " + str(val)

        def is_transfer_finder(row):
            try:
                regex_flag = re.search(
                    r"VENMO|DISCOVER|AMEX|WELLS FARGO|PAYPAL|CITI CARD|"
                    r"CAPITAL ONE [\w\s]*PMT|SOFI [\w\s\.]* CARD PAYMT|"
                    r"360 Checking",
                    str(row["_new_Description"])
                )
            except Exception:
                return "consider"

            if regex_flag:
                return "transfer"
            else:
                return "expense"

        input_df["_new_Description"] = input_df["Transaction Description"].copy(deep=True)
        input_df["_new_Amount"] = input_df["Transaction Amount"].copy(deep=True)
        input_df["_new_Date"] = input_df["Transaction Date"].copy(deep=True)
        input_df["_new_InstitutionCategory"] = pd.Series([np.nan] * len(input_df))
        input_df["_new_MyCategory"] = pd.Series([np.nan] * len(input_df))
        input_df["_new_Institution"] = input_df["Account Number"].map(institution_finder)
        input_df["_new_IsTransfer"] = input_df.apply(is_transfer_finder, axis=1)
        # TODO: input_df["_new_IsCompatible"] =

        return input_df
