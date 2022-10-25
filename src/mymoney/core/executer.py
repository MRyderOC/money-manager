import os
import logging
from typing import Dict

import numpy as np
import pandas as pd

from mymoney.core import raw_data_reader
from mymoney.core import output_transformer


logging.basicConfig(
    level=logging.INFO,
    format="%(name)s\t[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%b/%d/%y %I:%M:%S %p",
    # filename="logs.log",
)


class ExecClass():
    """docs here!"""
    # TODO: ETL Pipeline

    def __init__(self) -> None:
        self._data_reader = raw_data_reader.RawDataReader()
        self._output_trnsfrmr = output_transformer.OutputTransformer()
        pass


    def path_to_whole_data_dict(self, path: str, account_name: str = None) -> Dict[str, pd.DataFrame]:
        """docs here!"""
        input_data = self._data_reader.data_reader(path)
        if account_name:
            input_data["account_name"] = account_name
        out_dict = self._output_trnsfrmr.institution_executer(**input_data)
        try:
            sanity_df = out_dict["expense"]
        except KeyError:
            sanity_df = out_dict["trade"]
        except Exception as err:
            raise Exception(
                "Either out_dict doesn't contain anything or"
                " contains wrong keys that are not compatible."
                f" The error is: \n {err}"
            )
        out_df = self._output_trnsfrmr.output_df_creator(sanity_df)
        input_data.update({
            "sanity_df": sanity_df,
            "output_df": out_df,
        })
        return input_data


    def traversal(self, folder_path: str = "./csv_files") -> Dict[str, pd.DataFrame]:
        """Traverse csv_files folder by default and
        return two DataFrames each for trades and expenses.
        Note: This function does not traverse inner folders.
        """
        all_dfs_dict = {"expenses_list": [], "trades_list": []}

        for dirname, _, filenames in os.walk(folder_path):
            if dirname == folder_path:
                for filename in filenames:
                    try:
                        name, ext = filename.split(".")
                    except ValueError:
                        raise OSError(
                            f"{filename} doesn't have any extension."
                        )
                    if ext.casefold() != "csv":
                        continue

                    path = os.path.join(dirname, filename)
                    out_dict = self.path_to_whole_data_dict(path, account_name=name)
                    if out_dict["service_name"] in ["credit", "debit", "3rdparty"]:
                        all_dfs_dict["expenses_list"].append(out_dict["output_df"])
                    elif out_dict["service_name"] == "exchange":
                        all_dfs_dict["trades_list"].append(out_dict["output_df"])
                    else:
                        raise Exception(
                            "Either out_dict doesn't contain anything or"
                            " contains wrong keys that are not compatible."
                        )

        expenses_df, trades_df = None, None
        if all_dfs_dict["expenses_list"]:
            expenses_df = pd.concat(all_dfs_dict["expenses_list"], ignore_index=True)
        if all_dfs_dict["trades_list"]:
            trades_df = pd.concat(all_dfs_dict["trades_list"], ignore_index=True)
        return {
            "expenses": expenses_df, "trades": trades_df
        }
