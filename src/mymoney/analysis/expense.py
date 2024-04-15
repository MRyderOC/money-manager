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
        """Create an aggregated data which returns a dictionary with
        column's unique values as keys and the aggregated data as values.

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
            A dictionary with the column's unique values as keys
            and the aggregated data as values.
        """
        self._timeline_error_check(freq)

        out_dict = {}
        for col_value in self._expense_df[column].unique():
            tmp_df = self._expense_df[self._expense_df[column] == col_value]
            tmp_df = tmp_df[["Amount", "Date"]]
            grouper = pd.Grouper(freq=self._timeline_map[freq], key="Date")
            out_dict[col_value] = tmp_df.groupby(grouper).sum()

        return out_dict

    def get_unique_categories(self):
        return pd.DataFrame({
            "MyCategory": self._whole_df["MyCategory"].unique()
        })

    def get_last_date_df(self):
        last_date_cols = ["Institution", "AccountName", "Service", "LastDate"]
        last_date_df = pd.DataFrame(columns=last_date_cols)

        grouped_by_list = ["Institution", "AccountName", "Service"]
        grouped = self._expense_df.groupby(by=grouped_by_list)
        for name, grp in grouped:
            last_date = grp["Date"].max().date()
            tmp_df = pd.DataFrame({
                "Institution": [name[0]],
                "AccountName": [name[1]],
                "Service": [name[2]],
                "LastDate": [last_date],
            })
            last_date_df = pd.concat([last_date_df, tmp_df], ignore_index=True)

        return last_date_df

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
            A dictionary with the categories as keys
            and the aggregated data as values.
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
            A dictionary with the institutions as keys
            and the aggregated data as values.
        """
        return self._column_sum_grouper(column="Institution", freq=freq)
