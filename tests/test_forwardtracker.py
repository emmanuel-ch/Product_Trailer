""" test_forwardtracker.py
Tests on ForwardTracker class.
"""

from pathlib import Path
import shutil

import numpy as np
import pandas as pd
import pytest

from product_trailer.profile import Profile
from product_trailer.scheduler import Scheduler
from product_trailer.forwardtracker import ForwardTracker


WPT_DEF = ['Posting Date', 'Company', 'SLOC', 'Sold to', 'Mvt Code', 'Batch']

def make_ForwardTracker(mvtfp):
    mvts = (
        pd.read_csv(mvtfp,
            dtype={
                'Company': 'category',
                'Document': str,
                'PO': 'category',
                'Mvt Code': 'category',
                'SLOC': 'category',
                'Sold to': 'category',
                'SKU': 'category',
                'Batch': str,
                'QTY': int
            },
            parse_dates=['Posting Date'],
            na_filter=False
        )
        .assign(
            QTY_Unallocated = lambda df: abs(df['QTY']),
            Items_Allocated = lambda df: [[] for _ in range(len(df))],
            Company_SLOC_Batch = lambda df: df[['Company', 'SLOC', 'Batch']].apply(
                    lambda row: "-".join(row), axis=1
                ),
        )
    )
    return ForwardTracker(WPT_DEF, mvts)

class Test_find_decr:
    def test__find_decr_case1_std(self):
        ft = make_ForwardTracker('tests/test_data/mvts_1.csv')
        decrements = ft._find_decr(
            False,
            [
                pd.to_datetime('01/01/2023'),
                '1100',
                'SLOC_1',
                'NA',
                '',
                'b0101'
            ],
            '_some_ID'
        )
        assert list(decrements.index) == [3, 5, 9]

    def test__find_decr_case2_itemallocated(self):
        ft = make_ForwardTracker('tests/test_data/mvts_1.csv')
        ft.mvts['Items_Allocated'].iloc[3] = ['_some_ID']
        decrements = ft._find_decr(
            False,
            [
                pd.to_datetime('01/01/2023'),
                '1100',
                'SLOC_1',
                'NA',
                '',
                'b0101'
            ],
            '_some_ID'
        )
        assert list(decrements.index) == [5, 9]

    def test__find_decr_case3_date(self):
        ft = make_ForwardTracker('tests/test_data/mvts_1.csv')
        decrements = ft._find_decr(
            False,
            [
                pd.to_datetime('02/01/2023'),
                '1100',
                'SLOC_1',
                'NA',
                '',
                'b0101'
            ],
            '_some_ID'
        )
        assert list(decrements.index) == []

    def test__find_decr_case4_qtyunallocated(self):
        ft = make_ForwardTracker('tests/test_data/mvts_1.csv')
        ft.mvts['QTY_Unallocated'].iloc[5] = 0
        decrements = ft._find_decr(
            False,
            [
                pd.to_datetime('01/01/2023'),
                '1100',
                'SLOC_1',
                'NA',
                '',
                'b0101'
            ],
            '_some_ID'
        )
        assert list(decrements.index) == [3, 9]

    def test__find_decr_case5_soldto(self):
        ft = make_ForwardTracker('tests/test_data/mvts_1.csv')
        decrements = ft._find_decr(
            False,
            [
                pd.to_datetime('01/01/2023'),
                '1100',
                'NA',
                '0000222222',
                '',
                'b0101'
            ],
            '_some_ID'
        )
        assert list(decrements.index) == [8]

    def test__find_decr_case6_firstmvt(self):
        ft = make_ForwardTracker('tests/test_data/mvts_1.csv')
        decrements = ft._find_decr(
            True,
            [
                pd.to_datetime('01/01/2023'),
                '1100',
                'SLOC_2',
                '',
                'C02',
                'b0101'
            ],
            '_some_ID'
        )
        assert list(decrements.index) == [1]

    def test__find_decr_case7_firstmvtinconsignment(self):
        ft = make_ForwardTracker('tests/test_data/mvts_1.csv')
        decrements = ft._find_decr(
            True,
            [
                pd.to_datetime('01/01/2023'),
                '1100',
                'NA',
                '0000222222',
                'C01',
                'b0101'
            ],
            '_some_ID'
        )
        assert list(decrements.index) == [8]

class Test_find_incr:
    def test__find_incr_case1_std(self):
        ft = make_ForwardTracker('tests/test_data/mvts_1.csv')
        increments = ft._find_incr(
            decr=pd.Series(
                {
                    'Posting Date': pd.to_datetime('01/01/2023'),
                    'Company': '1100',
                    'Sold to': '',
                    'Batch': 'b0101',
                    'Mvt Code': 'C01',
                    'Document': 'DOC001',
                    'PO': '-2'
                }
            )
        )
        assert list(increments.index) == [0, 4]
    
    def test__find_incr_case2_changesoldto(self):
        ft = make_ForwardTracker('tests/test_data/mvts_1.csv')
        increments = ft._find_incr(
            decr=pd.Series(
                {
                    'Posting Date': pd.to_datetime('01/01/2023'),
                    'Company': '1100',
                    'Batch': 'b0101',
                    'Mvt Code': '956',
                    'PO': '-2'
                }
            )
        )
        assert list(increments.index) == [10]

    def test__find_incr_case3_changebatch(self):
        ft = make_ForwardTracker('tests/test_data/mvts_1.csv')
        increments = ft._find_incr(
            decr=pd.Series(
                {
                    'Posting Date': pd.to_datetime('01/01/2023'),
                    'Company': '1100',
                    'SLOC': 'SLOC_1',
                    'Sold to': '',
                    'Mvt Code': '702',
                    'PO': '-2'
                }
            )
        )
        assert list(increments.index) == [11]
    
    def test__find_incr_case4_po(self):
        ft = make_ForwardTracker('tests/test_data/mvts_1.csv')
        increments = ft._find_incr(
            decr=pd.Series(
                {
                    'Posting Date': pd.to_datetime('01/01/2023'),
                    'Batch': 'b0101',
                    'PO': 'PO001',
                    'Mvt Code': 'something'
                }
            )
        )
        assert list(increments.index) == [4, 15]
    
    def test__find_incr_case5_nobatch(self):
        ft = make_ForwardTracker('tests/test_data/mvts_1.csv')
        increments = ft._find_incr(
            decr=pd.Series(
                {
                    'Posting Date': pd.to_datetime('01/01/2023'),
                    'Company': '1100',
                    'Sold to': '',
                    'Mvt Code': 'C01',
                    'Document': 'DOC001',
                    'PO': '-2'
                }
            ),
            nobatch=True
        )
        #FIXME: shouldn't take a +mvt coming from a PO! (row 4)
        assert list(increments.index) == [0, 4]

@pytest.fixture()
def dummy_mvts(request):
    profile_name = 'test_profile_scheduler3'
    profile_path = Path('profiles') / profile_name
    testprofile = Profile(profile_name)
    scheduler = Scheduler(testprofile)
    imported = testprofile.import_movements(request.param)
    scheduler.tasklist = list(imported['SKU'].unique())
    mvts = scheduler._prep_mvt(imported)
    yield mvts
    shutil.rmtree(profile_path)

class Test_make_route:
    @pytest.mark.parametrize(
        'dummy_mvts', ['tests/test_data/fwt_case1.xlsx'], indirect=True
    )
    def test_case1_simpletransfer(self, dummy_mvts):  # DC = DecrementIncrement
        tracker = ForwardTracker(WPT_DEF, dummy_mvts)
        ini_item = pd.Series(
            {
                'First_Country': 'SomeCountry',
                'SKU': 'SomeSKU',
                'QTY': 1,
                'Open': True,
                'Waypoints': [
                    [pd.NaT, '3500', 'NA', '0000111111', '', '2002FON6440'],
                    [pd.Timestamp('2023-01-17'), '3500', '00299', 'NA', '632', '2002FON6440']
                ],
                'Unit_Value': 10,
                'Brand': 'SomeBrand',
                'Category': 'SomeCategory'
            }
        )
        expected_item = ini_item.copy()
        expected_item.Waypoints.append(
            [pd.Timestamp('2023-01-18'), '3500', '00213', np.nan, '311', '2002FON6440']
        )
        assert tracker._make_route(ini_item)[0].equals(expected_item)

    @pytest.mark.parametrize(
        'dummy_mvts', ['tests/test_data/fwt_case2.xlsx'], indirect=True
    )
    def test_case2_2transfers(self, dummy_mvts):
        tracker = ForwardTracker(WPT_DEF, dummy_mvts)
        ini_item = pd.Series(
            {
                'First_Country': 'SomeCountry',
                'SKU': 'SomeSKU',
                'QTY': 1,
                'Open': True,
                'Waypoints': [
                    [pd.Timestamp('2023-01-19'), '3100', 'NA', '0000449493', '632', '2210OZS1496']
                ],
                'Unit_Value': 10,
                'Brand': 'SomeBrand',
                'Category': 'SomeCategory'
            }
        )
        expected_item = ini_item.copy()
        expected_item.Waypoints = [
            [pd.NaT, '3100', 'NA', '0000449493', '', '2210OZS1496'],
            [pd.Timestamp('2023-01-19'), '3100', '00209', np.nan, '632', '2210OZS1496'],
            [pd.Timestamp('2023-01-19'), '3100', '00204', np.nan, '311', '2210OZS1496']
        ]
        assert tracker._make_route(ini_item)[0].equals(expected_item)

    @pytest.mark.parametrize(
        'dummy_mvts', ['tests/test_data/fwt_case3.xlsx'], indirect=True
    )
    def test_case3_incrementotherbatch(self, dummy_mvts):
        tracker = ForwardTracker(WPT_DEF, dummy_mvts)
        ini_item = pd.Series(
            {
                'First_Country': 'SomeCountry',
                'SKU': 'SomeSKU',
                'QTY': 1,
                'Open': True,
                'Waypoints': [
                    [pd.Timestamp('2023-01-19'), '3100', 'NA', '0000449493', '632', '2210OZS1496']
                ],
                'Unit_Value': 10,
                'Brand': 'SomeBrand',
                'Category': 'SomeCategory'
            }
        )
        expected_item = ini_item.copy()
        expected_item.Waypoints = [
            [pd.NaT, '3100', 'NA', '0000449493', '', '2210OZS1496'],
            [pd.Timestamp('2023-01-19'), '3100', '00209', np.nan, '632', '2210OZS1496'],
            [pd.Timestamp('2023-01-19'), '3100', '00204', np.nan, '311', '2210O000000']
        ]
        assert tracker._make_route(ini_item)[0].equals(expected_item)

    @pytest.mark.parametrize(
        'dummy_mvts', ['tests/test_data/fwt_case4.xlsx'], indirect=True
    )
    def test_case4_burnt(self, dummy_mvts):
        tracker = ForwardTracker(WPT_DEF, dummy_mvts)
        ini_item = pd.Series(
            {
                'First_Country': 'SomeCountry',
                'SKU': 'SomeSKU',
                'QTY': 1,
                'Open': True,
                'Waypoints': [
                    [pd.Timestamp('2023-01-19'), '3100', 'NA', '0000449493', '632', '2210OZS1496']
                ],
                'Unit_Value': 10,
                'Brand': 'SomeBrand',
                'Category': 'SomeCategory'
            }
        )
        expected_item = ini_item.copy()
        expected_item.Open = False
        expected_item.Waypoints = [
            [pd.NaT, '3100', 'NA', '0000449493', '', '2210OZS1496'],
            [pd.Timestamp('2023-01-19'), '3100', '00209', np.nan, '632', '2210OZS1496'],
            [pd.Timestamp('2023-01-19'), '3100', 'BURNT 00209', '0000000000', '311', '2210OZS1496']
        ]
        assert tracker._make_route(ini_item)[0].equals(expected_item)

    @pytest.mark.parametrize(
        'dummy_mvts', ['tests/test_data/fwt_case5.xlsx'], indirect=True
    )
    def test_case5_po(self, dummy_mvts):
        tracker = ForwardTracker(WPT_DEF, dummy_mvts)
        ini_item = pd.Series(
            {
                'First_Country': 'SomeCountry',
                'SKU': 'SomeSKU',
                'QTY': 1,
                'Open': True,
                'Waypoints': [
                    [pd.Timestamp('2023-01-24'), '2200', 'NA', '0000385977', '632', '2305PXT6252']
                ],
                'Unit_Value': 10,
                'Brand': 'SomeBrand',
                'Category': 'SomeCategory'
            }
        )
        expected_item = ini_item.copy()
        expected_item.Waypoints = [
            [pd.NaT, '2200', 'NA', '0000385977', '', '2305PXT6252'],
            [pd.Timestamp('2023-01-24'), '2200', '00296', np.nan, '632', '2305PXT6252'],
            [pd.Timestamp('2023-01-25'), '1100', '05507', np.nan, '161/673', '2305PXT6252']
        ]
        assert tracker._make_route(ini_item)[0].equals(expected_item)

    @pytest.mark.parametrize(
        'dummy_mvts', ['tests/test_data/fwt_case6.xlsx'], indirect=True
    )
    def test_case6_changebatch(self, dummy_mvts):
        tracker = ForwardTracker(WPT_DEF, dummy_mvts)
        ini_item = pd.Series(
            {
                'First_Country': 'SomeCountry',
                'SKU': 'SomeSKU',
                'QTY': 1,
                'Open': True,
                'Waypoints': [
                    [pd.Timestamp('2022-12-28'), '2100', '00001', np.nan, 'SomeCode', '2001CZD4079']
                ],
                'Unit_Value': 10,
                'Brand': 'SomeBrand',
                'Category': 'SomeCategory'
            }
        )
        expected_item = ini_item.copy()
        expected_item.Waypoints.append(
            [pd.Timestamp('2023-01-05'), '2100', '00001', np.nan, '702/701', '2001CZ00NEW']
        )
        assert tracker._make_route(ini_item)[0].equals(expected_item)
