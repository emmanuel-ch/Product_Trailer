""" test_defaultprofile_processing.py
Tests on default profile processing functions.
"""

import pytest
from product_trailer.default_profile.config import processing as pr


@pytest.fixture(scope='module')
def dummy_mvts():
    return pr.import_movements('tests/test_data/raw_mvts1.csv')

def test_import_dtype(dummy_mvts):
    assert (
        (
            list(dummy_mvts.select_dtypes('number').columns)
            == ['QTY', 'Unit_Value']
        )
        and (
            list(dummy_mvts.select_dtypes('object').columns)
            == ['Document', 'Batch']
        )
        and (
            list(dummy_mvts.select_dtypes('category').columns) ==
            [
                'Company', 'Country', 'PO', 'Special Stock Ind Code',
                'Mvt Code', 'SLOC', 'Sold to', 'Brand', 'Category', 'SKU'
            ]
        )
    )

def test_import_csv_vs_xlsx():
    csv = pr.import_movements('tests/test_data/raw_mvts1.csv')
    xlsx = pr.import_movements('tests/test_data/raw_mvts1.xlsx')
    assert csv.equals(xlsx)
