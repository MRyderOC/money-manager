import logging
from typing import Dict

import numpy as np  # noqa: F401
import pandas as pd

from mymoney.utils.common import column_name_checker


logging.basicConfig(
    level=logging.INFO,
    format="%(name)s\t[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%b/%d/%y %I:%M:%S %p",
)


class ExpenseAnalysis:
    """Main class for Expense analysis."""

    _expense_columns = [
        "Description", "Amount", "Date",
        "Institution", "AccountName",
        "InstitutionCategory", "MyCategory",
        "IsTransfer", "IsValid", "Service", "Notes"
    ]
    _timeline_map = {
        "Y": "Y", "y": "Y", "yearly": "Y",
        "M": "M", "m": "M", "monthly": "M",
        "W": "W", "w": "W", "weekly": "W",
    }

    def __init__(self, df: pd.DataFrame) -> None:
        if df.empty:
            raise ValueError(
                "The `df` should not be empty. Please load a valid DataFrame.")
        column_name_checker(df, self._expense_columns)

        self._whole_df = df.copy(deep=True)
        self._expense_df = df[df["IsTransfer"] == "expense"]
        self._transfer_df = df[df["IsTransfer"] == "transfer"]
        self._redundant_df = df[df["IsTransfer"] == "redundant"]

    def _timeline_error_check(self, timeline: str):
        """Check if the timeline is in `_timeline_map`.

        Args:
            timeline (str):
                The input timeline as a string.

        Raises:
            ValueError: If the `timeline` not in `_timeline_map`.
        """
        if timeline not in self._timeline_map:
            raise ValueError(
                "`timeline` should be one of the following:"
                f"\n{list(self._timeline_map.keys())}"
            )

    def _column_sum_grouper(
        self, column: str, freq: str = "M"
    ) -> Dict[str, pd.Series]:
        """Create a DataFrame that the columns are the unique values present
        in the `column` and the rows are the aggregated data based on the
        `freq`.

        Args:
            column (str):
                The name of the column to the aggregation on.
            freq (str):
                The freq that the aggregation will be perform on.
                Options:
                    "Y", "y", "yearly"
                    "M", "m", "monthly"
                    "W", "w", "weekly"

        Returns:
            A pandas DataFrame with the aggregated data.
        """
        self._timeline_error_check(freq)

        out_dict = {}
        for col_value in self._expense_df[column].unique():
            tmp_df = self._expense_df[self._expense_df[column] == col_value]
            tmp_df = tmp_df[["Amount", "Date"]]
            grouper = pd.Grouper(freq=self._timeline_map[freq], key="Date")
            out_dict[col_value] = tmp_df.groupby(grouper).sum()

        df = pd.DataFrame({
            cat: ser["Amount"]
            for cat, ser in out_dict.items()
        })
        return df.fillna(.0)

    # DataFrame creator methods
    def get_unique_categories_df(self) -> pd.DataFrame:
        return pd.DataFrame({
            "MyCategory": self._whole_df["MyCategory"].unique()
        })

    def get_last_date_df(self, sort_by: str = "institution") -> pd.DataFrame:
        """Get the last transactions for each account.

        Args:
            sort_by (str):
                The column to sort by.
                Options:
                    "date",
                    "institution", "inst"
                    "service",
                    "accountname", "account", "acc"

        Returns:
            A DataFrame with the last transactions for each account.
        """
        sort_options_map = {
            "date": "LastDate",
            "service": "Service",
            "institution": "Institution",
            "inst": "Institution",
            "accountname": "AccountName",
            "account": "AccountName",
            "acc": "AccountName",
        }
        last_date_cols = ["Institution", "AccountName", "Service", "LastDate"]
        last_date_df = pd.DataFrame(columns=last_date_cols)

        grouped_by_list = ["Institution", "AccountName", "Service"]
        grouped = self._whole_df.groupby(by=grouped_by_list)
        for name, grp in grouped:
            last_date = grp["Date"].max().date()
            tmp_df = pd.DataFrame({
                "Institution": [name[0]],
                "AccountName": [name[1]],
                "Service": [name[2]],
                "LastDate": [last_date],
            })
            last_date_df = pd.concat([last_date_df, tmp_df], ignore_index=True)

        return last_date_df.sort_values(
            by=sort_options_map[sort_by], ignore_index=True)

    def get_accounts_df(self, multi_index: bool = False) -> pd.DataFrame:
        """Get a DataFrame with the AccountName for each Institution
        along with their Service.

        Args:
            multi_index (bool):
                Whether to retutn a pandas multi index or not.

        Returns:
            A DataFrame with the accounts for each institution.
        """
        if multi_index:
            grouped_by = ["Institution", "AccountName", "Service"]
            return self._expense_df[grouped_by].groupby(grouped_by).count()
        else:
            return self.get_last_date_df().drop(columns=["LastDate"])

    def get_last_n_transactions_df(self, n: int = 5) -> pd.DataFrame:
        """Get the last n transactions for each account.

        Args:
            n (int):
                The number of transactions to get.

        Returns:
            A DataFrame with the last n transactions for each account.
        """
        last_n_cols = [
            "Institution", "AccountName", "Date", "Amount", "Description"]
        last_n_df = pd.DataFrame(columns=last_n_cols)

        grouped_by_list = ["Institution", "AccountName", "Service"]
        grouped = self._whole_df.groupby(by=grouped_by_list)
        for _, grp in grouped:
            tmp_df = grp.sort_values(by="Date", ignore_index=True)
            tmp_df = tmp_df[last_n_cols].tail(n)
            last_n_df = pd.concat([last_n_df, tmp_df], ignore_index=True)

        return last_n_df

    # Spend related methods
    def category_spend(self, freq: str = "M") -> Dict[str, pd.Series]:
        """Create an aggregated data for expense categories.

        Args:
            freq (str):
                The freq that the aggregation will be perform on.
                Options:
                    "Y", "y", "yearly"
                    "M", "m", "monthly"
                    "W", "w", "weekly"

        Returns:
            A pandas DataFrame with the aggregated data.
        """
        return self._column_sum_grouper(column="MyCategory", freq=freq)

    def institution_spend(self, freq: str = "M") -> Dict[str, pd.Series]:
        """Create an aggregated data for institutions.

        Args:
            freq (str):
                The freq that the aggregation will be perform on.
                Options:
                    "Y", "y", "yearly"
                    "M", "m", "monthly"
                    "W", "w", "weekly"

        Returns:
            A pandas DataFrame with the aggregated data.
        """
        return self._column_sum_grouper(column="Institution", freq=freq)

    def account_spend(self, freq: str = "M") -> Dict[str, pd.Series]:
        """Create an aggregated data for account.

        Args:
            freq (str):
                The freq that the aggregation will be perform on.
                Options:
                    "Y", "y", "yearly"
                    "M", "m", "monthly"
                    "W", "w", "weekly"

        Returns:
            A pandas DataFrame with the aggregated data.
        """
        return self._column_sum_grouper(column="AccountName", freq=freq)
