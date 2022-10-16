import os
import json
import logging
from typing import Dict, List, Union

import numpy as np
import pandas as pd
from pandas.errors import ParserError

from mymoney.institutions import amex
from mymoney.institutions import capitalone
from mymoney.institutions import citi
from mymoney.institutions import discover
from mymoney.institutions import paypal


logging.basicConfig(
    level=logging.INFO,
    format="%(name)s\t[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%b/%d/%y %I:%M:%S %p",
    # filename="logs.log",
)


# TODO Add retention (and or restoration) option
class ExecClass():
    """Main class for execution functions."""

    _meta_data_path = "meta_data.json"

    def __init__(self) -> None:
        with open(self._meta_data_path) as f:
            self._meta_data = json.load(f)


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
                    logging.info(f"Completed: {name}")
                    return {
                        "input_df": input_df,
                        "institution_name": institution,
                        "service_name": service
                    }
                # except ParserError:
                #     logging.warning(f"Parse Error: {name}")
                except Exception as err:
                    # logging.warning(f"An error occurred for {name}: {err}")
                    continue


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
        elif institution_name == "citi": the_institution_obj = citi.Citi()
        elif institution_name == "discover": the_institution_obj = discover.Discover()
        elif institution_name == "paypal": the_institution_obj = paypal.PayPal()
        else:
            raise ValueError(
                f"Institution {institution_name} is not supported yet."
                "\nYou can file an issue and provide more information"
                " to add the institution."
            )
        
        return the_institution_obj.service_executer(
            input_df, service_name, account_name
        )




    def _new_column_df_creator(self, df: pd.DataFrame) -> pd.DataFrame:
        """docs here!"""
        new_columns_name_map = {col: col[5:] for col in df.columns if col.startswith("_new_")}
        old_columns = [col for col in df.columns if not col.startswith("_new_")]
        return df.drop(columns=old_columns).rename(columns=new_columns_name_map)


    def path_to_whole_data_dict(self, path: str, account_name: str) -> Dict[str, pd.DataFrame]:
        """docs here!"""
        input_data = self.data_reader(path)
        out_dict = self.institution_executer(account_name=account_name, **input_data)
        try:
            sanity_df = out_dict["expense"]
            out_df = self._new_column_df_creator(sanity_df)
        except KeyError:
            sanity_df = out_dict["trade"]
            out_df = self._new_column_df_creator(sanity_df)
        except Exception as err:
            raise Exception(
                "Either out_dict doesn't contain anything or"
                " contains wrong keys that are not compatible."
                f" The error is: \n {err}"
            )
        input_data.update({
            "sanity_df": sanity_df,
            "output_df": out_df,
        })
        return input_data





    def path_to_dataframe_dict(self, path: str, account_name: str) -> Dict[str, pd.DataFrame]:
        """docs here!"""
        input_data = self.data_reader(path)
        return self.institution_executer(account_name=account_name, **input_data)


    def traversal(self, folder_path: str = "csv_files") -> Dict[str, List[pd.DataFrame]]:
        """Traverse csv_files folder and
        return two dataframes each for trades and expenses.
        """
        all_dataframes = {"expenses_list": [], "trades_list": []}

        for dirname, _, filenames in os.walk(folder_path):
            if dirname == "./csv_files":
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
                    out_dict = self.path_to_dataframe_dict(path, account_name=name)
                    try:
                        all_dataframes["expenses_list"].append(out_dict["expense"])
                    except KeyError:
                        all_dataframes["trades_list"].append(out_dict["trade"])
                    except Exception as err:
                        raise Exception(
                            "Either out_dict doesn't contain anything or"
                            " contains wrong keys that are not compatible."
                            f" The error is: \n {err}"
                        )

        return all_dataframes


    def sanity_check_executor(
        self,
        dfs_dict: Dict[str, List[pd.DataFrame]],
        sanity_check: bool = False
    ):
        """docs here!"""
        if not sanity_check:
            expenses_list = [
                self._new_column_df_creator(e_df)
                for e_df in dfs_dict["expenses_list"]
            ]
            trades_list = [
                self._new_column_df_creator(t_df)
                for t_df in dfs_dict["trades_list"]
            ]

            return {
                "expenses_df": pd.concat(expenses_list, ignore_index=True),
                "trades_df": pd.concat(trades_list, ignore_index=True)
            }
        else:
            # Sanity check process
            pass


    def main(self, sanity_check: bool = False):
        """docs here!"""
        all_dataframes = self.traversal()
        self.sanity_check_executor(
            dfs_dict=all_dataframes, sanity_check=sanity_check
        )








if __name__ == "__main__":
    # path_builder = lambda inst: "/content/drive/MyDrive/csv_files/{}.csv".format(inst)
    path = "/Users/milad/Desktop/Dev/github_repos/finances/csv_files/outs/expenses/2022-03-10/raw_data/credits/capitalone 2091.csv"
    obj_main = ExecClass()
    # x = obj_main.data_reader(path)
    capone = obj_main.path_to_dataframe_dict(path, "capone")
    print(capone)
    # print(x)
