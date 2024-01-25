import logging
from typing import Dict

import numpy as np  # noqa: F401
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe

from mymoney.utils.common import raise_or_log


logging.basicConfig(
    level=logging.INFO,
    format="%(name)s\t[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%b/%d/%y %I:%M:%S %p",
)


class SheetsOperations:
    """A class for using google sheets as a storage option.

    Structure:
    sheet_name (Default: MyMoney)
    |
    | - Expenses
    | - Trades
    """

    _expense_columns = [
        "Description", "Amount", "Date",
        "Institution", "AccountName",
        "InstitutionCategory", "MyCategory",
        "IsTransfer", "IsValid", "Service", "Notes"
    ]
    _trade_columns = [
        "Datetime",
        "FromAccount", "ToAccount",
        "FromAsset", "ToAsset",
        "InAmount", "OutAmount",
        "FeeAsset", "FeeAmount", "FeeValue",
        "TrxType", "TrxSubType",
        "AssetType",
        "USDAmount"
    ]

    def __init__(
        self,
        creds: str | Dict[str, str],
        sheet_name: str = "MyMoney",
    ):
        """Creates gspread instance and get the sheet with
        the title `sheet_name`.

        Args:
            creds (str | Dict[str, str]):
                The credentials for authentication. Either path to
                service account key or a dictionary containing the key.
            sheet_name (str):
                The title of the spreadsheet.
        """
        # Authentication
        if isinstance(creds, str):
            self._gc = gspread.service_account(filename=creds)
        elif isinstance(creds, dict):
            self._gc = gspread.service_account_from_dict(creds)
        else:
            raise ValueError(
                "`creds` should be either a path-like string"
                " to a JSON or a dict.")

        self.sheet_name = sheet_name
        if not self._is_sheets_structure_exists():
            logging.warning(
                f"The sheet `{sheet_name}` doesn't have the correct structure."
                " Use `sheets_init` to continue.")
            return

        # Get the sheet and related worksheets
        self._the_sheet = self._gc.open(self.sheet_name)
        self._expense_wsheet = self._the_sheet.worksheet("Expenses")
        self._trade_wsheet = self._the_sheet.worksheet("Trades")

    def _is_sheets_structure_exists(
        self,
        sheet_name: str = None,
        logs: bool = True,
        raises: bool = False,
    ) -> bool:
        """Check if the correct sheet structure exists in `sheet_name`.

        Args:
            sheet_name (str):
                The title of the spreadsheet.
            logs (bool):
                Whether to log the results if something went wrong.
            raises (bool):
                Whether to raise an error or not.

        Returns:
            True if the correct sheet structure exists in `sheet_name`
            False otherwise.
        """
        if not sheet_name:
            sheet_name = self.sheet_name

        # Open the sheets
        try:
            the_sheet = self._gc.open(sheet_name)
        except Exception as err:
            msg = (
                f"Couldn't open the sheet `{sheet_name}`."
                " Use `sheets_init` to create appropriate sheet."
                f"\nError: {err}.")
            raise_or_log(msg, logs, raises)
            return False

        # Check the worksheets
        targeted_worksheets = ["Expenses", "Trades"]
        available_worksheets = [ws.title for ws in the_sheet.worksheets()]
        not_present_worksheets = [
            ws
            for ws in targeted_worksheets
            if ws not in available_worksheets
        ]
        if not_present_worksheets:
            msg = (
                f"The worksheet(s) {not_present_worksheets}"
                f" is/are not present in the sheet `{the_sheet.title}`.")
            raise_or_log(msg, logs, raises)
            return False

        return True

    def sheets_init(self, sheet_name: str = None, share_address: str = None):
        """Create the sheet with `sheet_name` title and related worksheets.

        Args:
            sheet_name (str):
                The title of the spreadsheet.
            share_address (str):
                The email address the spreadsheet should be shared with.
        """
        if not sheet_name:
            sheet_name = self.sheet_name

        if not self._is_sheets_structure_exists(
            sheet_name=sheet_name, logs=False
        ):
            try:
                self._the_sheet = self._gc.open(sheet_name)
            except gspread.exceptions.SpreadsheetNotFound:
                self._the_sheet = self._gc.create(sheet_name)

            self._expense_wsheet = self._the_sheet.add_worksheet(
                title="Expenses", rows=100, cols=20)
            self._trade_wsheet = self._the_sheet.add_worksheet(
                title="Trades", rows=100, cols=20)
            self._expense_wsheet.update([self._expense_columns])
            self._trade_wsheet.update([self._trade_columns])

            # Delete the "Sheet1"
            self._the_sheet.del_worksheet(self._the_sheet.worksheet("Sheet1"))

            if share_address:
                self._the_sheet.share(
                    share_address, perm_type="user", role="writer")

    def expenses_to_df(self) -> pd.DataFrame:
        """Read 'Expenses' worksheet and returns it as a DataFrame."""
        self._is_sheets_structure_exists(raises=True)

        df = pd.DataFrame(self._expense_wsheet.get_all_records())
        df["Date"] = pd.to_datetime(df["Date"])
        df["Amount"] = (
            df["Amount"]
            .replace("", ".0")
            .str.replace("$", "", regex=False)
            .str.replace(",", "", regex=False)
            .astype(float)
        )

        return df

    def trades_to_df(self) -> pd.DataFrame:
        """Read 'Trades' worksheet and returns it as a DataFrame."""
        self._is_sheets_structure_exists(raises=True)

        return pd.DataFrame(self._trade_wsheet.get_all_records())

    def write_df_to_sheets(
        self, df: pd.DataFrame, wsheet_name: str, create: bool = False
    ):
        """Write a pandas DataFrame to the worksheet named `wsheet_name`.
        Args:
            df (pd.DataFrame):
                The input DataFrame.
            wsheet_name (str):
                The name of the worksheet to write the DataFrame to.
            create (bool):
                Whether to create a new worksheet or use an existing one.
        """
        if create:
            tmp_wsheet = self._the_sheet.add_worksheet(
                title=wsheet_name, rows=100, cols=20)
        else:
            tmp_wsheet = self._the_sheet.worksheet(wsheet_name)

        set_with_dataframe(tmp_wsheet, df)
        logging.info("Done!")
