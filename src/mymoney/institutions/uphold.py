import logging

import numpy as np
import pandas as pd

from mymoney.institutions import institution_base


logging.basicConfig(
    level=logging.INFO,
    format="%(name)s\t[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%b/%d/%y %I:%M:%S %p",
)


class Uphold(institution_base.Institution):
    """A class for Uphold institution's data cleaning functions."""

    _this_institution_name = "uphold"

    def __init__(self) -> None:
        super().__init__()

    class ExchangeService(institution_base.Institution.ExchangeService):
        """A class for Exchange Service."""

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
            def from_account_finder(row):
                origin, row_type = row["Origin"], row["Type"]

                if origin == "uphold":
                    if row_type == "in":
                        return "Brave"
                    elif row_type in ["out", "transfer"]:
                        return "Uphold"
                    else:
                        return np.nan
                elif origin == "bank":
                    return "Bank"
                else:
                    return np.nan

            def to_account_finder(val):
                if val == "uphold":
                    return "Uphold"
                elif val == "ethereum":
                    # Alternative: f'{row["ToAsset"]} Network'
                    return "Wallet"
                elif val == "bank":
                    return "Bank"
                else:
                    return np.nan

            def from_asset_finder(row):
                if row["_new_FromAccount"] == "Brave":
                    return "R3W"
                else:
                    return row["Origin Currency"]

            def in_amount_finder(row):
                if row["_new_FromAsset"] == "R3W":
                    return .0
                return float(row["Origin Amount"])

            def fee_value_finder(val):
                if pd.isna(val):
                    return .0
                else:
                    return np.nan

            def trx_type_finder(row):
                row_type = row["Type"]
                if row_type == "transfer":
                    return "Trade"
                elif row_type == "out":
                    return "Transfer"
                elif row_type == "in":
                    from_acc = row["_new_FromAccount"]
                    if from_acc == "Brave":
                        return "Reward"
                    elif from_acc == "Bank":
                        return "Transfer"
                    else:
                        return np.nan
                else:
                    return np.nan

            def trx_sub_type_finder(row):
                trx_type = row["_new_TrxType"]
                from_asset = row["_new_FromAsset"]
                to_asset = row["_new_ToAsset"]
                from_acc = row["_new_FromAccount"]
                to_acc = row["_new_ToAccount"]

                if trx_type == "Trade":
                    if from_asset in self._USDs:
                        return "Buy"
                    elif to_asset in self._USDs:
                        return "Sell"
                    else:
                        return "Pairwise"
                elif trx_type == "Transfer":
                    if from_acc == "Bank":
                        if to_asset in self._USDs:
                            return "Deposit"
                        else:
                            return "Buy"
                    elif to_acc == "Bank":
                        if from_asset in self._USDs:
                            return "Withdrawal"
                        else:
                            return "Sell"
                    elif to_acc == "Wallet":
                        return "Crypto"
                    else:
                        return np.nan
                elif trx_type == "Reward":
                    return "Brave"
                elif trx_type == "HODL":
                    return "HODL"
                else:
                    return np.nan

            def usd_amount_finder(row):
                from_asset = row["_new_FromAsset"]
                to_asset = row["_new_ToAsset"]
                in_amount = row["_new_InAmount"]
                out_amount = row["_new_OutAmount"]

                if from_asset in self._USDs and to_asset in self._USDs:
                    return max(in_amount, out_amount)
                elif from_asset in self._USDs:
                    return in_amount
                elif to_asset in self._USDs:
                    return out_amount
                else:
                    return np.nan

            input_df["_new_Datetime"] = pd.to_datetime(input_df["Date"], utc=True)  # noqa: E501
            input_df["_new_FromAccount"] = input_df.apply(from_account_finder, axis=1)  # noqa: E501
            input_df["_new_ToAccount"] = input_df["Destination"].map(to_account_finder)  # noqa: E501
            input_df["_new_FromAsset"] = input_df.apply(from_asset_finder, axis=1)  # noqa: E501
            input_df["_new_ToAsset"] = input_df["Destination Currency"]
            input_df["_new_InAmount"] = input_df.apply(in_amount_finder, axis=1)  # noqa: E501
            input_df["_new_OutAmount"] = input_df["Destination Amount"]
            input_df["_new_FeeAsset"] = input_df["Fee Currency"].fillna("USD")
            input_df["_new_FeeAmount"] = input_df["Fee Amount"].fillna(.0)
            input_df["_new_FeeValue"] = input_df["Fee Amount"].map(fee_value_finder)  # noqa: E501
            input_df["_new_TrxType"] = input_df.apply(trx_type_finder, axis=1)
            input_df["_new_TrxSubType"] = input_df.apply(trx_sub_type_finder, axis=1)  # noqa: E501
            input_df["_new_AssetType"] = "Crypto"
            # input_df["_new_USDAmount"] = input_df.apply(usd_amount_finder, axis=1)  # noqa: E501

            return input_df
