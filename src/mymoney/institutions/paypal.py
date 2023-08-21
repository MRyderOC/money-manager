import re
import logging

import numpy as np
import pandas as pd

from mymoney.institutions import institution_base


logging.basicConfig(
    level=logging.INFO,
    format="%(name)s\t[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%b/%d/%y %I:%M:%S %p",
)


class PayPal(institution_base.Institution):
    """A class for PayPal institution's data cleaning functions."""

    _this_institution_name = "paypal"

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
                elif not (name_is_nan or regex_flag_redundant):
                    return "expense"
                else:
                    return "consider"

            def description_finder(row):
                if pd.isna(row["Name"]):
                    out = str(row["Type"])
                else:
                    out = f"{str(row['Name'])}: {row['Type']}"

                return out.strip()

            def amount_finder(val):
                return float(str(val).replace(",", ""))

            input_df["_new_Description"] = input_df.apply(description_finder, axis=1)  # noqa: E501
            input_df["_new_Amount"] = input_df["Amount"].map(amount_finder)
            input_df["_new_Date"] = input_df["Date"].copy(deep=True)
            input_df["_new_InstitutionCategory"] = np.nan
            input_df["_new_MyCategory"] = np.nan
            input_df["_new_Institution"] = "PayPal"
            input_df["_new_AccountName"] = account_name
            input_df["_new_Service"] = self._service_type.value
            input_df["_new_IsTransfer"] = input_df.apply(is_transfer_finder, axis=1)  # noqa: E501

            return input_df
