import os
import logging
import dataclasses
from typing import List, Union

import numpy as np
import pandas as pd

from mymoney.core import data_classes
from mymoney.core import raw_data_reader
from mymoney.core import data_operations


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
        pass


    def path_to_whole_data(self, path: str, account_name: str = None) -> data_classes.WholeData:
        """docs here!"""
        data_reader_instance = raw_data_reader.RawDataReader()
        input_data = data_reader_instance.data_reader(path)
        if account_name:
            input_data.account_name = account_name
        transformed_data = input_data.institution_executer()

        return data_classes.WholeData(
            **{
                **dataclasses.asdict(input_data),
                **dataclasses.asdict(transformed_data)
            }
        )


    def traversal(
        self,
        folder_path: str,
        append_to_db: bool = True,
        store_sanity_data: bool = True,
        store_raw_data: bool = True,
        remove_source: bool = True,
        return_whole_data_list: bool = True
    ) -> Union[List[data_classes.WholeData], None]:
        """Traverse `folder_path` and
        return a list contains WholeData for each file in the folder_path.
        Note: This function does not traverse inner folders.
        """
        all_outs_as_list = []

        for dirname, _, filenames in os.walk(folder_path):
            if dirname == folder_path:
                for filename in filenames:
                    name, ext = os.path.splitext(filename)
                    if ext.casefold() != ".csv":
                        continue

                    path = os.path.join(dirname, filename)
                    whole_data = self.path_to_whole_data(path, account_name=name)
                    all_outs_as_list.append(whole_data)

        data_op_instance = data_operations.DataOperations()

        for wd in all_outs_as_list:
            if append_to_db:
                data_op_instance.append_to_db(wd)
            if store_sanity_data:
                data_op_instance.store_sanity_data(wd)
            if store_raw_data:
                data_op_instance.store_raw_data(wd, remove_source)

        if return_whole_data_list:
            return all_outs_as_list
        return
