import logging
import dataclasses
from typing import Dict, Union

import numpy as np
import pandas as pd


logging.basicConfig(
    level=logging.INFO,
    format="%(name)s\t[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%b/%d/%y %I:%M:%S %p",
    # filename="logs.log",
)


@dataclasses.dataclass
class MyData:
    """docs here!"""
    expense: pd.DataFrame
    trade: pd.DataFrame
    balance: pd.DataFrame


@dataclasses.dataclass
class WholeData:
    """docs here!"""
    path: str
    institution_name: str
    service_name: str
    account_name: str
    input_df: pd.DataFrame
    sanity_df: pd.DataFrame
    output_df: pd.DataFrame
    out_type: str


    def generate_file_name(self) -> str:
        """docs here!"""
        last_date = str(self.output_df["Date"].max().date())
        return (
            f"{self.institution_name} - {self.service_name} - {self.account_name} ({last_date})"
        )
