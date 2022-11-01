import os
import json
import logging
import dataclasses
from typing import Dict, List, Union

import numpy as np
import pandas as pd
from pandas.errors import ParserError
from importlib_resources import files

from mymoney.institutions import institution_base
from mymoney.institutions import amex
from mymoney.institutions import capitalone
from mymoney.institutions import chase
from mymoney.institutions import citi
from mymoney.institutions import discover
from mymoney.institutions import paypal
from mymoney.institutions import venmo

logging.basicConfig(
    level=logging.INFO,
    format="%(name)s\t[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%b/%d/%y %I:%M:%S %p",
    # filename="logs.log",
)


@dataclasses.dataclass
class RawData:
    """docs here!"""
    path: str
    institution_name: str
    service_name: str
    account_name: str
    input_df: pd.DataFrame


    def institution_executer(self) -> institution_base.TransformedData:
        """docs here!"""
        if self.institution_name == "amex": the_institution_obj = amex.AmEx()
        elif self.institution_name == "capitalone": the_institution_obj = capitalone.CapitalOne()
        elif self.institution_name == "chase": the_institution_obj = chase.Chase()
        elif self.institution_name == "citi": the_institution_obj = citi.Citi()
        elif self.institution_name == "discover": the_institution_obj = discover.Discover()
        elif self.institution_name == "paypal": the_institution_obj = paypal.PayPal()
        elif self.institution_name == "venmo": the_institution_obj = venmo.Venmo()
        else:
            raise ValueError(
                f"Institution {self.institution_name} is not supported yet."
                "\nYou can file an issue and provide more information"
                " to add the institution."
            )
        
        return the_institution_obj.service_executer(
            self.input_df, self.service_name, self.account_name
        )


class RawDataReader():
    """Main class for reading data."""
    # TODO: WellsFargo data reader
    # TODO: Data reader for base: expense & trade (meta_data.json)


    def __init__(self) -> None:
        self._meta_data = json.loads(
            files("mymoney").joinpath("meta_data.json").read_text()
        )


    def _column_name_checker(
        self, input_df: pd.DataFrame, columns: List[str]
    ):
        """Check if the columns of input_df is identical to the stored columns."""
        if set(input_df.columns) != set(columns):
            raise Exception(
                "DataFrame columns are not matched with our database."
            )


    def data_reader(self, path: str) -> RawData:
        """Read the data in the path and returns
        a DataFrame with the Institution name and it's service."""

        for institution in self._meta_data:
            for service in self._meta_data[institution]:
                name = f"{institution}/{service}"

                cols = self._meta_data[institution][service]["columns"]
                read_args = self._meta_data[institution][service]["read_args"]

                # Read the data
                try:
                    input_df = pd.read_csv(filepath_or_buffer=path, **read_args)
                    self._column_name_checker(input_df, cols)
                    account_name = os.path.basename(path).split(".")[0]
                    logging.info(f"Completed: {name} - {account_name}")
                    return RawData(
                        path=path,
                        institution_name=institution,
                        service_name=service,
                        account_name=account_name,
                        input_df=input_df,
                    )
                # except ParserError:
                #     logging.warning(f"Parse Error: {name}")
                except Exception as err:
                    # logging.warning(f"An error occurred for {name}: {err}")
                    continue

        # Log a warning if data can not be read
        logging.warning(f"Couldn't read the data for {path}")
