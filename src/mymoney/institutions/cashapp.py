import logging

import numpy as np
import pandas as pd

from mymoney.institutions import institution_base


logging.basicConfig(
    level=logging.INFO,
    format="%(name)s\t[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%b/%d/%y %I:%M:%S %p",
)


class CashApp(institution_base.Institution):
    """A class for CashApp institution's data cleaning functions."""

    _this_institution_name = "cashapp"

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
            def is_transfer_finder(row):
                row_status = row["Status"]
                if row_status == "PAYMENT REVERSED":
                    return "redundant"
                elif row_status == "TRANSFER SENT":
                    return "transfer"
                elif row_status in ["PAYMENT SENT", "PAYMENT DEPOSITED"]:
                    return "expense"
                else:
                    return "consider"

            def amount_finder(val):
                return float(
                    str(val).replace(",", "").replace("$", "")
                )

            def description_finder(row):
                row_type = row["Transaction Type"]
                row_sender_receiver = row["Name of sender/receiver"]
                row_notes = row["Notes"]
                notes = "" if pd.isna(row_notes) else f" ({row_notes})"

                if row_type == "Sent P2P":
                    out = f"Me -> {row_sender_receiver}{notes}"
                elif row_type == "Received P2P":
                    out = f"From {row_sender_receiver} -> Me{notes}"
                elif row_type == "Cash out":
                    out = "Cash out"
                else:
                    out = "Consider"

                return out.strip()

            input_df["_new_Description"] = input_df.apply(description_finder, axis=1)  # noqa: E501
            input_df["_new_Amount"] = input_df["Amount"].map(amount_finder)
            input_df["_new_Date"] = input_df["Date"].copy(deep=True)
            input_df["_new_InstitutionCategory"] = np.nan
            input_df["_new_MyCategory"] = np.nan
            input_df["_new_Institution"] = "CashApp"
            input_df["_new_AccountName"] = account_name
            input_df["_new_Service"] = self._service_type.value
            input_df["_new_IsTransfer"] = input_df.apply(is_transfer_finder, axis=1)  # noqa: E501

            return input_df
