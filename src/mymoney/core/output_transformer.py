import logging
from typing import Dict

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


class OutputTransformer():
    """docs here!"""


    def __init__(self) -> None:
        pass


    def institution_executer(
        self,
        input_df: pd.DataFrame,
        institution_name: str,
        service_name: str,
        account_name: str
    ) -> Dict[str, pd.DataFrame]:
        """docs here!"""
        if institution_name == "amex": the_institution_obj = amex.AmEx()
        elif institution_name == "capitalone": the_institution_obj = capitalone.CapitalOne()
        elif institution_name == "chase": the_institution_obj = chase.Chase()
        elif institution_name == "citi": the_institution_obj = citi.Citi()
        elif institution_name == "discover": the_institution_obj = discover.Discover()
        elif institution_name == "paypal": the_institution_obj = paypal.PayPal()
        elif institution_name == "venmo": the_institution_obj = venmo.Venmo()
        else:
            raise ValueError(
                f"Institution {institution_name} is not supported yet."
                "\nYou can file an issue and provide more information"
                " to add the institution."
            )
        
        return the_institution_obj.service_executer(
            input_df, service_name, account_name
        )


    def output_df_creator(self, df: pd.DataFrame) -> pd.DataFrame:
        """docs here!"""
        new_columns_name_map = {
            col: col[5:]
            for col in df.columns
            if col.startswith("_new_")
        }
        old_columns = [
            col
            for col in df.columns
            if not col.startswith("_new_")
        ]

        return df.drop(columns=old_columns).rename(columns=new_columns_name_map)
