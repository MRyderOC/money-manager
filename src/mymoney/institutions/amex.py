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


class AmEx(institution_base.Institution):
    """A class for AmEx institution's data cleaning functions."""

    _this_institution_name = "amex"

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
            # row["_new_Description"] == "YOUR CASH REWARD/REFUND IS":
            #     for cash back payments
            # row["Extended Details"].startswith("Amex Offer Credit"):
            #     for offer redeems

            def is_transfer_finder(row):
                try:
                    regex_flag_paypal = re.search(
                        r"PAYPAL",
                        str(row["_new_Description"])
                    )
                    regex_flag_payment = re.search(
                        r"\w* PAYMENT - THANK YOU",
                        str(row["_new_Description"])
                    )
                except Exception:
                    return "consider"

                if (regex_flag_payment or regex_flag_paypal):
                    return "transfer"
                elif (not regex_flag_payment and not regex_flag_paypal):
                    return "expense"
                else:
                    return "consider"

            df_len = len(input_df)
            input_df["_new_Description"] = input_df["Description"].map(
                lambda val: str(val).strip()
            )
            input_df["_new_Amount"] = -input_df["Amount"]
            input_df["_new_Date"] = input_df["Date"].copy(deep=True)
            input_df["_new_InstitutionCategory"] = input_df["Category"].copy(
                deep=True
            )
            input_df["_new_MyCategory"] = input_df["Category"].copy(deep=True)
            input_df["_new_Institution"] = pd.Series(["AmEx"] * df_len)
            input_df["_new_AccountName"] = pd.Series([account_name] * df_len)
            input_df["_new_IsTransfer"] = input_df.apply(
                is_transfer_finder, axis=1
            )

            return input_df
