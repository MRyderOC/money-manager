import logging

import numpy as np
import pandas as pd

from mymoney.institutions import institution_base


logging.basicConfig(
    level=logging.INFO,
    format="%(name)s\t[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%b/%d/%y %I:%M:%S %p",
)


class Venmo(institution_base.Institution):
    """A class for Venmo institution's data cleaning functions."""

    _this_institution_name = "venmo"

    def __init__(self) -> None:
        super().__init__()


    class ThirdPartyService(institution_base.Institution.ThirdPartyService):
        """docs here!"""


        def _cleaning(
            self, input_df: pd.DataFrame, account_name: str
        ) -> pd.DataFrame:
            """A class for Venmo institution's data cleaning functions."""

            def is_transfer_finder(val):
                if val == "Standard Transfer":
                    return "transfer"
                elif val in ["Payment", "Charge", "Merchant Transaction"]:
                    return "expense"
                else:
                    return "consider"

            def amount_finder(val):
                return float(
                    str(val).replace(" $", "").replace(",", "")
                )

            def description_finder(row):
                row_type = row["Type"]
                if row_type == "Standard Transfer":
                    out = f"transfer to {row['Destination']}"
                elif row_type == "Merchant Transaction":
                    out = row["To"]
                elif row_type == "Payment":
                    out = f"{row['From']} -> {row['To']}: {row['Note']}"
                elif row_type == "Charge":
                    out = f"{row['To']} -> {row['From']}: {row['Note']}"
                else:
                    out = f"Consider: {row['Note']}: {row['From']} -> {row['To']}. (Type: {row_type})"

                return out.strip()

            input_df["_new_Description"] = input_df.apply(description_finder, axis=1)
            input_df["_new_Amount"] = input_df["Amount (total)"].map(amount_finder)
            input_df["_new_Date"] = input_df["Datetime"].copy(deep=True)
            input_df["_new_InstitutionCategory"] = pd.Series([np.nan] * len(input_df))
            input_df["_new_MyCategory"] = pd.Series([np.nan] * len(input_df))
            input_df["_new_Institution"] = pd.Series([f"Venmo {account_name}"] * len(input_df))
            input_df["_new_IsTransfer"] = input_df["Type"].map(is_transfer_finder)

            return input_df.dropna(subset=["_new_Date"])
