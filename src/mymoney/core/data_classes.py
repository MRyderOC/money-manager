import logging
import dataclasses
from typing import Dict, Union

import numpy as np
import pandas as pd

from mymoney.core import raw_data_reader
from mymoney.institutions import institution_base


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
class WholeData(raw_data_reader.RawData, institution_base.TransformedData):
    """docs here!"""
    path: str
    institution_name: str
    service_name: str
    account_name: str
    input_df: pd.DataFrame
    sanity_df: pd.DataFrame
    output_df: pd.DataFrame
    out_type: str
