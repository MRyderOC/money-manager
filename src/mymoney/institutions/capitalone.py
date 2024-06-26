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


class CapitalOne(institution_base.Institution):
    """A class for CapitalOne institution's data cleaning functions."""

    _this_institution_name = "capitalone"

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
            # row["_new_Description"] == "CREDIT-CASH BACK REWARD":
            #     for cash back payments
            # row["_new_Description"] == "AU BONUS": for bonuses

            def amount_finder(row):
                return (
                    row["Credit"] if np.isnan(row["Debit"]) else -row["Debit"]
                )

            def is_transfer_finder(row):
                try:
                    regex_flag_paypal = re.search(
                        r"PAYPAL",
                        str(row["_new_Description"])
                    )
                    regex_flag_payment = re.search(
                        r"CAPITAL ONE \w* PYMT",
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

            input_df["_new_Description"] = input_df["Description"].map(lambda val: str(val).strip())  # noqa: E501
            input_df["_new_Amount"] = input_df.apply(amount_finder, axis=1)
            input_df["_new_Date"] = input_df["Transaction Date"].copy(deep=True)  # noqa: E501
            input_df["_new_InstitutionCategory"] = input_df["Category"].copy(deep=True)  # noqa: E501
            input_df["_new_MyCategory"] = input_df["Category"].copy(deep=True)
            input_df["_new_Institution"] = "Capital One"
            input_df["_new_AccountName"] = input_df["Card No."].copy(deep=True)  # noqa: E501
            input_df["_new_Service"] = self._service_type.value
            input_df["_new_IsTransfer"] = input_df.apply(is_transfer_finder, axis=1)  # noqa: E501

            return input_df

    class DebitService(institution_base.Institution.DebitService):
        """A class for Debit Service."""

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
                try:
                    regex_flag = re.search(
                        r"VENMO|DISCOVER|AMEX|PAYPAL|SAMS CLUB PAYMENT|"
                        r"Wealthfront EDI PYMNTS|JPMorgan Chase Ext Trnsfr|"
                        r"CHASE CREDIT CRD AUTOPAY|CHASE CREDIT CRD EPAY|"
                        r"WELLS FARGO|WF Credit Card AUTO PAY|"
                        r"CITI CARD|CITI AUTOPAY PAYMENT|Cash App|"
                        r"SOFI [\w\s\.]* CARD PAYMT|SoFi Bank TRANSFER|"
                        r"CAPITAL ONE [\w\s]*PMT|"
                        r"360 Checking|360 Performance Savings",
                        str(row["_new_Description"])
                    )
                except Exception:
                    return "consider"

                if regex_flag:
                    return "transfer"
                else:
                    return "expense"

            def amount_finder(row):
                trx_type = row["Transaction Type"]
                amount = row["Transaction Amount"]
                if trx_type == "Debit":
                    return -abs(amount)
                elif trx_type == "Credit":
                    return abs(amount)

            input_df["_new_Description"] = input_df["Transaction Description"].map(lambda val: str(val).strip())  # noqa: E501
            input_df["_new_Amount"] = input_df.apply(amount_finder, axis=1)
            input_df["_new_Date"] = input_df["Transaction Date"].copy(deep=True)  # noqa: E501
            input_df["_new_InstitutionCategory"] = np.nan
            input_df["_new_MyCategory"] = np.nan
            input_df["_new_Institution"] = "Capital One"
            input_df["_new_AccountName"] = input_df["Account Number"].copy(deep=True)  # noqa: E501
            input_df["_new_Service"] = self._service_type.value
            input_df["_new_IsTransfer"] = input_df.apply(is_transfer_finder, axis=1)  # noqa: E501

            return input_df
