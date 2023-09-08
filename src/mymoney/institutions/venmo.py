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
        """A class for ThirdParty Service."""

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

            def is_transfer_finder(val):
                transfer_list = ["Standard Transfer", "Credit Card Payment"]
                expense_list = [
                    "Payment", "Charge",
                    "Merchant Transaction", "Credit Card Reward"
                ]
                if val in transfer_list:
                    return "transfer"
                elif val in expense_list:
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
                elif row_type in ["Credit Card Payment", "Credit Card Reward"]:
                    out = row_type
                else:
                    out = (
                        f"Consider: {row['Note']}:"
                        f" {row['From']} -> {row['To']}. (Type: {row_type})"
                    )

                return out.strip()

            input_df["_new_Description"] = input_df.apply(description_finder, axis=1)  # noqa: E501
            input_df["_new_Amount"] = input_df["Amount (total)"].map(amount_finder)  # noqa: E501
            input_df["_new_Date"] = input_df["Datetime"].copy(deep=True)
            input_df["_new_InstitutionCategory"] = np.nan
            input_df["_new_MyCategory"] = np.nan
            input_df["_new_Institution"] = "Venmo"
            input_df["_new_AccountName"] = account_name
            input_df["_new_Service"] = self._service_type.value
            input_df["_new_IsTransfer"] = input_df["Type"].map(is_transfer_finder)  # noqa: E501

            return input_df.dropna(subset=["_new_Date"])
