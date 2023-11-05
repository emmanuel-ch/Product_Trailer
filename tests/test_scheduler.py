""" test_scheduler.py
Tests on Scheduler class.
"""

from pathlib import Path
import shutil

import pytest

from product_trailer.profile import Profile
from product_trailer.scheduler import Scheduler


@pytest.fixture(scope='module')
def dummy_extract():
    profile_name = 'test_profile_scheduler1'
    profile_path = Path('profiles') / profile_name
    testprofile = Profile(profile_name)
    scheduler = Scheduler(testprofile)
    imported = testprofile.import_movements('tests/test_data/raw_mvts2.xlsx')
    extracted = scheduler._extract_items(imported)
    yield extracted
    shutil.rmtree(profile_path)


class Test_extract_items:
    def test__extract_items_number(self, dummy_extract):
        assert len(dummy_extract) == 8

    def test__extract_items_totalqty(self, dummy_extract):
        assert dummy_extract['QTY'].sum() == 12

    def test__extract_items_noduplicates(self, dummy_extract):
        assert not any(dummy_extract.index.duplicated())

    def test__extract_items_positiveqty(self, dummy_extract):
        assert all(dummy_extract['QTY'] > 0)

    def test__extract_items_dtypes(self, dummy_extract):
        assert (
            (
                list(dummy_extract.select_dtypes('category').columns)
                == ['First_Country', 'SKU', 'Brand', 'Category']
            )
            and (list(dummy_extract.select_dtypes('number').columns) == ['QTY', 'Unit_Value'])
            and (list(dummy_extract.select_dtypes(bool).columns) == ['Open'])
            and (list(dummy_extract.select_dtypes('object').columns) == ['Waypoints'])
        )


@pytest.fixture(scope='module')
def dummy_mvts():
    profile_name = 'test_profile_scheduler2'
    profile_path = Path('profiles') / profile_name
    testprofile = Profile(profile_name)
    scheduler = Scheduler(testprofile)
    imported = testprofile.import_movements('tests/test_data/raw_mvts2.xlsx')
    scheduler.tasklist = list(imported['SKU'].unique())
    mvts = scheduler._prep_mvt(imported)
    yield mvts
    shutil.rmtree(profile_path)

class Test_prep_mvt:
    def test_prep_mvt_len(self, dummy_mvts):
        assert len(dummy_mvts) == 17

    def test_prep_mvt_dtypes(self, dummy_mvts):
        assert (
            (
                list(dummy_mvts.select_dtypes('category').columns)
                == ['Company', 'PO', 'Mvt Code', 'SLOC', 'Sold to', 'SKU']
            )
            and (
                list(dummy_mvts.select_dtypes('datetime').columns)
                == ['Posting Date']
            )
            and (
                list(dummy_mvts.select_dtypes('object').columns)
                == ['Document', 'Batch', 'Items_Allocated', 'Company_SLOC_Batch']
            )
            and (
                list(dummy_mvts.select_dtypes('number').columns)
                == ['QTY', 'QTY_Unallocated']
            )
        )
