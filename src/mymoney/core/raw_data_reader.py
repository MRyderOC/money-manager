import os
import json
import logging
from typing import Dict, List, Union

import numpy as np
import pandas as pd
from pandas.errors import ParserError
from importlib_resources import files


logging.basicConfig(
    level=logging.INFO,
    format="%(name)s\t[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%b/%d/%y %I:%M:%S %p",
    # filename="logs.log",
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


    def data_reader(self, path: str) -> Dict[str, Union[pd.DataFrame, str]]:
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
                    return {
                        "institution_name": institution,
                        "service_name": service,
                        "account_name": account_name,
                        "input_df": input_df,
                    }
                # except ParserError:
                #     logging.warning(f"Parse Error: {name}")
                except Exception as err:
                    # logging.warning(f"An error occurred for {name}: {err}")
                    continue

        # Log a warning if data can not be read
        logging.warning(f"Couldn't read the data for {path}")
