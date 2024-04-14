import logging
import re

import numpy as np
import pandas as pd

from mymoney.institutions import institution_base


logging.basicConfig(
    level=logging.INFO,
    format="%(name)s\t[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%b/%d/%y %I:%M:%S %p",
)


class Coinbase(institution_base.Institution):
    """A class for Coinbase institution's data cleaning functions."""

    _this_institution_name = "coinbase"

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
                trx_type = row["Transaction Type"]
                if trx_type in [
                    "Inflation Reward", "Staking Income",
                    "Convert", "Send", "Sell", "Withdrawal",
                ]:
                    return "coinbase"
                elif trx_type == "Buy":
                    # Can be coinbase itself
                    # TODO: Need revision and compare to `expense` df
                    return "Bank"
                elif trx_type == "Learning Reward":
                    return "Coinbase Reward"
                elif trx_type == "Receive":
                    if (
                        row["NotesHelper"][-2] == "Coinbase"
                        and row["NotesHelper"][-1] in ["Earn", "Rewards"]
                    ):
                        return "Coinbase Reward"
                    elif row["NotesHelper"][-1] == "from":
                        return "coinbase"

                    # Alternative: f'{row["ToAsset"]} Network'
                    return "Wallet"
                else:
                    return np.nan

            def to_account_finder(row):
                trx_type = row["Transaction Type"]
                if trx_type == "Send":
                    try:
                        re_pattern = r"to\s(.*)$"
                        groups = re.search(re_pattern, row["Notes"]).groups()
                        if not groups[0]:
                            return f"{row['Asset']} Network"
                        return groups[0]
                    except Exception:
                        return np.nan
                elif trx_type == "Withdrawal":
                    return "Bank"

                return "coinbase"

            def from_asset_finder(row):
                trx_type = row["Transaction Type"]
                if trx_type == "Learning Reward":
                    return "R3W"
                elif trx_type == "Buy":
                    return "USD"
                elif trx_type in [
                    "Inflation Reward", "Staking Income",
                    "Convert", "Send", "Sell", "Withdrawal",
                ]:
                    return row["Asset"]
                elif trx_type == "Receive":
                    if (
                        row["NotesHelper"][-2] == "Coinbase"
                        and row["NotesHelper"][-1] in ["Earn", "Rewards"]
                    ):
                        return "R3W"
                    elif row["NotesHelper"][-1] == "from":
                        return row["Asset"]
                else:
                    return np.nan

            def to_asset_finder(row):
                trx_type = row["Transaction Type"]
                if trx_type in [
                    "Learning Reward", "Buy", "Send", "Withdrawal",
                    "Inflation Reward", "Staking Income", "Receive",
                ]:
                    return row["Asset"]
                elif trx_type in ["Convert", "Sell"]:
                    return row["NotesHelper"][-1]
                else:
                    return np.nan

            def in_amount_finder(row):
                trx_type = row["Transaction Type"]
                if trx_type in [
                    "Learning Reward", "Inflation Reward",
                    "Staking Income", "Receive",
                ]:
                    return .0
                elif trx_type == "Buy":
                    return float(row["Subtotal"])
                elif trx_type in [
                    "Convert", "Send", "Sell", "Withdrawal",
                ]:
                    return row["Quantity Transacted"]
                else:
                    return np.nan

            def out_amount_finder(row):
                trx_type = row["Transaction Type"]
                if trx_type in [
                    "Learning Reward", "Buy", "Send", "Withdrawal",
                    "Inflation Reward", "Staking Income", "Receive"
                ]:
                    return float(row["Quantity Transacted"])
                elif trx_type in ["Convert", "Sell"]:
                    price = str(row["NotesHelper"][-2]).replace("$", "")
                    return float(price)
                else:
                    return np.nan

            def trx_type_finder(row):
                trx_type = row["Transaction Type"]
                if trx_type == "Learning Reward":
                    return "Reward"
                elif trx_type in ["Buy", "Send", "Withdrawal"]:
                    # TODO: `Buy` could be `Trade` depend on
                    # where the source is
                    return "Transfer"
                elif trx_type in ["Convert", "Sell"]:
                    return "Trade"
                elif trx_type in ["Inflation Reward", "Staking Income"]:
                    return "HODL"
                elif trx_type == "Receive":
                    if (
                        row["NotesHelper"][-2] == "Coinbase"
                        and row["NotesHelper"][-1] in ["Earn", "Rewards"]
                    ):
                        return "Reward"
                    elif row["NotesHelper"][-1] == "from":
                        return "HODL"
                else:
                    return np.nan

            def trx_sub_type_finder(row):
                trx_type = row["Transaction Type"]
                if trx_type == "Learning Reward":
                    return "Coinbase Reward"
                elif trx_type in ["Inflation Reward", "Staking Income"]:
                    return "Stake"
                elif trx_type == "Receive":
                    if (
                        row["NotesHelper"][-2] == "Coinbase"
                        and row["NotesHelper"][-1] in ["Earn", "Rewards"]
                    ):
                        return "Coinbase Reward"
                    elif row["NotesHelper"][-1] == "from":
                        return "Stake"
                elif trx_type in ["Send", "Withdrawal"]:
                    return "Withdraw"
                elif trx_type == "Buy":
                    return "Buy"
                elif trx_type in ["Sell", "Convert"]:
                    if row["_new_FromAsset"] in self._USDs:
                        return "Buy"
                    elif row["_new_ToAsset"] in self._USDs:
                        return "Sell"
                    else:
                        return "Pairwise"
                else:
                    return np.nan

            input_df["NotesHelper"] = input_df["Notes"].str.split()

            input_df["_new_Datetime"] = pd.to_datetime(input_df["Timestamp"], utc=True)  # noqa: E501
            input_df["_new_FromAccount"] = input_df.apply(from_account_finder, axis=1)  # noqa: E501
            input_df["_new_ToAccount"] = input_df.apply(to_account_finder, axis=1)  # noqa: E501
            input_df["_new_FromAsset"] = input_df.apply(from_asset_finder, axis=1)  # noqa: E501
            input_df["_new_ToAsset"] = input_df.apply(to_asset_finder, axis=1)  # noqa: E501
            input_df["_new_InAmount"] = input_df.apply(in_amount_finder, axis=1)  # noqa: E501
            input_df["_new_OutAmount"] = input_df.apply(out_amount_finder, axis=1)  # noqa: E501
            input_df["_new_FeeAsset"] = "USD"
            input_df["_new_FeeAmount"] = input_df["Fees and/or Spread"].copy(deep=True)  # noqa: E501
            input_df["_new_FeeValue"] = input_df["Fees and/or Spread"].copy(deep=True)  # noqa: E501
            input_df["_new_TrxType"] = input_df.apply(trx_type_finder, axis=1)
            input_df["_new_TrxSubType"] = input_df.apply(trx_sub_type_finder, axis=1)  # noqa: E501
            input_df["_new_AssetType"] = "Crypto"
            # input_df["_new_USDAmount"] = input_df["Subtotal"]

            return input_df
