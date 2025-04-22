data_loader

import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """
    url = 'https://storage.googleapis.com/taxibuckett/taxidata.csv'
    response = requests.get(url)

    # Option 1: Set low_memory=False to avoid the DtypeWarning
    return pd.read_csv(io.StringIO(response.text), sep=',', low_memory=False)

# Option 2: Specify dtype for the columns with mixed types
    # dtype = {
    #     'column_name_3': 'str',  # Change to the actual column name and type
    #     'column_name_5': 'str',
    #     'column_name_17': 'str',
    #     'column_name_18': 'str'
    # }
    # return pd.read_csv(io.StringIO(response.text), sep=',', dtype=dtype)


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'