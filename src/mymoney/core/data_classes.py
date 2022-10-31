import logging
import dataclasses
from typing import Dict, Union

import numpy as np
import pandas as pd

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
class MyData:
    """docs here!"""
    expense: pd.DataFrame
    trade: pd.DataFrame
    balance: pd.DataFrame


@dataclasses.dataclass
class TransformedData:
    """docs here!"""
    sanity_df: pd.DataFrame
    output_df: pd.DataFrame
    out_type: str


@dataclasses.dataclass
class RawData:
    """docs here!"""
    path: str
    institution_name: str
    service_name: str
    account_name: str
    input_df: pd.DataFrame


    def institution_executer(self) -> TransformedData:
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


@dataclasses.dataclass
class WholeData(RawData, TransformedData):
    """docs here!"""
    path: str
    institution_name: str
    service_name: str
    account_name: str
    input_df: pd.DataFrame
    sanity_df: pd.DataFrame
    output_df: pd.DataFrame
    out_type: str
