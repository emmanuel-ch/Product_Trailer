import pytest
import pathlib
import pandas as pd

from product_trailer.core import extract_items

@pytest.fixture
def prep_test_dataset_1():
    input_filepath = pathlib.Path("tests/extract_items_test_datasets/1_input.pkl")
    output_filepath = pathlib.Path("tests/extract_items_test_datasets/1_output.pkl")
    input_df = pd.read_pickle(input_filepath)
    output_df = pd.read_pickle(output_filepath)

    return (input_df, output_df)

def test_extract_items_1(prep_test_dataset_1):
    new_out = extract_items(prep_test_dataset_1[0])
    pd.testing.assert_frame_equal(
        new_out.sort_index().sort_index(axis=1).drop(columns=['Waypoints']), 
        prep_test_dataset_1[1].sort_index().sort_index(axis=1).drop(columns=['Waypoints'])
    )

