import pytest
import numpy as np
import pandas as pd


@pytest.fixture
def dataframe_creator():
    return pd.DataFrame({
        "col1": [1, 2, 3],
        "col2": ["a", "b", "c"],
        "col3": [True, True, False],
    })


@pytest.fixture
def superset_column():
    return ["col1", "col2", "col3", "col4", ]


@pytest.fixture
def subset_column():
    return ["col1", "col2", ]


@pytest.fixture
def equal_column():
    return ["col1", "col2", "col3", ]


@pytest.fixture
def range_series():
    return pd.Series(np.arange(3, 13))
