import logging

import numpy as np
import pandas as pd

from mymoney.institutions import institution_base


logging.basicConfig(
    level=logging.INFO,
    format="%(name)s\t[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%b/%d/%y %I:%M:%S %p",
)


class CryptoDotCom(institution_base.Institution):
    """A class for Crypto.Com institution's data cleaning functions."""

    _this_institution_name = "cryptodotcom"

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
            def from_account_finder(val):
                if val in [
                    "crypto_exchange",
                    "crypto_withdrawal",
                    "crypto_earn_interest_paid",
                    "crypto_earn_program_withdrawn",
                    "crypto_earn_program_created",
                    "rewards_platform_deposit_credited",
                ]:
                    return "cryptodotcom"
                elif val == "crypto_purchase":
                    return "Bank"
                elif val == "crypto_deposit":
                    # Alternative: f'{row["ToAsset"]} Network'
                    return "Wallet"
                else:
                    return np.nan

            def to_account_finder(val):
                if val in [
                    "crypto_earn_interest_paid",
                    "crypto_purchase",
                    "crypto_deposit",
                    "crypto_exchange",
                    "rewards_platform_deposit_credited",
                    "crypto_earn_program_withdrawn",
                    "crypto_earn_program_created",
                ]:
                    return "cryptodotcom"
                elif val == "crypto_withdrawal":
                    # Alternative: f'{row["ToAsset"]} Network'
                    return "Wallet"
                else:
                    return np.nan

            def from_asset_finder(row):
                trx_kind = row["Transaction Kind"]
                if trx_kind == "crypto_purchase":
                    return "USD"
                elif trx_kind == "rewards_platform_deposit_credited":
                    return "R3W"
                elif trx_kind in [
                    "crypto_exchange",
                    "crypto_withdrawal",
                    "crypto_earn_interest_paid",
                    "crypto_earn_program_withdrawn",
                    "crypto_earn_program_created",
                    "crypto_deposit",
                ]:
                    return row["Currency"]
                else:
                    return np.nan

            def to_asset_finder(row):
                if pd.isna(row["To Currency"]):
                    return row["Currency"]
                else:
                    return row["To Currency"]

            def in_amount_finder(row):
                trx_kind = row["Transaction Kind"]
                if trx_kind == "crypto_purchase":
                    return abs(row["Native Amount"])
                elif trx_kind in [
                    "rewards_platform_deposit_credited",
                    "crypto_earn_interest_paid",
                ]:
                    return .0
                elif trx_kind in [
                    "crypto_exchange",
                    "crypto_withdrawal",
                    "crypto_deposit",
                    "crypto_earn_program_withdrawn",
                    "crypto_earn_program_created",
                ]:
                    return row["Amount"]
                else:
                    return np.nan

            def out_amount_finder(row):
                if pd.isna(row["To Currency"]):
                    return row["Amount"]
                else:
                    return row["To Amount"]

            def trx_sub_type_finder(row):
                trx_kind = row["Transaction Kind"]
                if trx_kind == "crypto_earn_interest_paid":
                    return "Stake"
                elif trx_kind in [
                    "crypto_earn_program_withdrawn",
                    "crypto_earn_program_created",
                ]:
                    return "Redundant"
                elif trx_kind in ["crypto_withdrawal", "crypto_deposit"]:
                    return "Crypto"
                elif trx_kind == "crypto_purchase":
                    return "Buy"
                elif trx_kind == "rewards_platform_deposit_credited":
                    return "CryptoDotComRewards"
                elif trx_kind == "crypto_exchange":
                    if row["Currency"] in self._USDs:
                        return "Buy"
                    elif row["To Currency"] in self._USDs:
                        return "Sell"
                    else:
                        return "Pairwise"
                else:
                    return np.nan

            trx_type_mapping = {
                "crypto_earn_interest_paid": "HODL",
                "crypto_purchase": "Transfer",
                "crypto_withdrawal": "Transfer",
                "crypto_deposit": "Transfer",
                "crypto_exchange": "Trade",
                "rewards_platform_deposit_credited": "Reward",
                "crypto_earn_program_withdrawn": "HODL",
                "crypto_earn_program_created": "HODL",
            }

            input_df["Amount"] = input_df["Amount"].abs()

            input_df["_new_Datetime"] = pd.to_datetime(input_df["Timestamp (UTC)"], utc=True)  # noqa: E501
            input_df["_new_FromAccount"] = input_df["Transaction Kind"].map(from_account_finder)  # noqa: E501
            input_df["_new_ToAccount"] = input_df["Transaction Kind"].map(to_account_finder)  # noqa: E501
            input_df["_new_FromAsset"] = input_df.apply(from_asset_finder, axis=1)  # noqa: E501
            input_df["_new_ToAsset"] = input_df.apply(to_asset_finder, axis=1)  # noqa: E501
            input_df["_new_InAmount"] = input_df.apply(in_amount_finder, axis=1)  # noqa: E501
            input_df["_new_OutAmount"] = input_df.apply(out_amount_finder, axis=1)  # noqa: E501
            input_df["_new_FeeAmount"] = .0
            input_df["_new_FeeAsset"] = "USD"
            input_df["_new_FeeValue"] = .0
            input_df["_new_TrxType"] = input_df["Transaction Kind"].map(trx_type_mapping)  # noqa: E501
            input_df["_new_TrxSubType"] = input_df.apply(trx_sub_type_finder, axis=1)  # noqa: E501
            input_df["_new_AssetType"] = "Crypto"
            # input_df["_new_USDAmount"] = input_df["Native Amount"].abs()

            return input_df
