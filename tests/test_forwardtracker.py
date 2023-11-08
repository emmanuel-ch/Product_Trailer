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
        'dummy_mvts', ['tests/test_data/fwt_case01.xlsx'], indirect=True
    )
    def test_case01_simpletransfer(self, dummy_mvts):  # DC = DecrementIncrement
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
        'dummy_mvts', ['tests/test_data/fwt_case02.xlsx'], indirect=True
    )
    def test_case02_2transfers(self, dummy_mvts):
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
        'dummy_mvts', ['tests/test_data/fwt_case03.xlsx'], indirect=True
    )
    def test_case03_incrementotherbatch(self, dummy_mvts):
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
        'dummy_mvts', ['tests/test_data/fwt_case04.xlsx'], indirect=True
    )
    def test_case04_burnt(self, dummy_mvts):
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
        'dummy_mvts', ['tests/test_data/fwt_case05.xlsx'], indirect=True
    )
    def test_case05_po(self, dummy_mvts):
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
        'dummy_mvts', ['tests/test_data/fwt_case06.xlsx'], indirect=True
    )
    def test_case06_ponoreceipt(self, dummy_mvts):
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
        expected_item.Open = np.nan
        expected_item.Waypoints = [
            [pd.NaT, '2200', 'NA', '0000385977', '', '2305PXT6252'],
            [pd.Timestamp('2023-01-24'), '2200', '00296', np.nan, '632', '2305PXT6252'],
            [pd.Timestamp('2023-01-24'), '2200', 'PO FROM 00296, mvt 161', '0000007905', '9000667710', '2305PXT6252']
        ]
        assert tracker._make_route(ini_item)[0].equals(expected_item)

    @pytest.mark.parametrize(
        'dummy_mvts', ['tests/test_data/fwt_case07.xlsx'], indirect=True
    )
    def test_case07_changebatch(self, dummy_mvts):
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

    @pytest.mark.parametrize(
        'dummy_mvts', ['tests/test_data/fwt_case08.xlsx'], indirect=True
    )
    def test_case08_longroute(self, dummy_mvts):
        tracker = ForwardTracker(WPT_DEF, dummy_mvts)
        ini_item = pd.Series(
            {
                'First_Country': 'SomeCountry',
                'SKU': 'SomeSKU',
                'QTY': 1,
                'Open': True,
                'Waypoints': [
                    [pd.Timestamp('2023-01-04'), '3400', 'NA', '0000397038', '932', '2204DKM3293']
                ],
                'Unit_Value': 10,
                'Brand': 'SomeBrand',
                'Category': 'SomeCategory'
            }
        )
        expected_item = ini_item.copy()
        expected_item.Waypoints = [
            [pd.NaT, '3400', 'NA', '0000397038', '', '2204DKM3293'],
            [pd.Timestamp('2023-01-04'), '3400', '00217', np.nan, '932', '2204DKM3293'],
            [pd.Timestamp('2023-01-04'), '3400', '00209', np.nan, '321', '2204DKM3293'],
            [pd.Timestamp('2023-01-18'), '3500', '00299', np.nan, '161/101', '2204DKM3293'],
            [pd.Timestamp('2023-01-18'), '3500', '00204', np.nan, '321', '2204DKM3293'],
            [pd.Timestamp('2023-01-20'), '3500', '002Q3', np.nan, '311', '2204DKM3293'],
            [pd.Timestamp('2023-01-23'), '3500', '002Q2', np.nan, '321', '2204DKM3293'],
        ]
        assert tracker._make_route(ini_item)[0].equals(expected_item)

    @pytest.mark.parametrize(
        'dummy_mvts', ['tests/test_data/fwt_case09.xlsx'], indirect=True
    )
    def test_case09_repassentrypoint(self, dummy_mvts):
        tracker = ForwardTracker(WPT_DEF, dummy_mvts)
        ini_item = pd.Series(
            {
                'First_Country': 'SomeCountry',
                'SKU': 'SomeSKU',
                'QTY': 1,
                'Open': True,
                'Waypoints': [
                    [pd.Timestamp('2023-01-16'), '1000', 'NA', '0000329283', '632', '2102ZXH2048']
                ],
                'Unit_Value': 10,
                'Brand': 'SomeBrand',
                'Category': 'SomeCategory'
            }
        )
        expected_item = ini_item.copy()
        expected_item.Waypoints = [
            [pd.NaT, '1000', 'NA', '0000329283', '', '2102ZXH2048'],
            [pd.Timestamp('2023-01-16'), '1000', '00207', np.nan, '632', '2102ZXH2048'],
            [pd.Timestamp('2023-01-17'), '1000', '00213', np.nan, '311', '2102ZXH2048'],
            [pd.Timestamp('2023-01-19'), '1000', 'NA', '0000323040', '631', '2102ZXH2048'],
            [pd.Timestamp('2023-01-23'), '1000', '00209', np.nan, '632', '2102ZXH2048'],
            [pd.Timestamp('2023-01-26'), '1000', '00202', np.nan, '311', '2102ZXH2048'],
        ]
        assert tracker._make_route(ini_item)[0].equals(expected_item)

    @pytest.mark.parametrize(
        'dummy_mvts', ['tests/test_data/fwt_case10.xlsx'], indirect=True
    )
    def test_case10_split_1decr2incr(self, dummy_mvts):
        tracker = ForwardTracker(WPT_DEF, dummy_mvts)
        ini_item = pd.Series(
            {
                'First_Country': 'SomeCountry',
                'SKU': 'SomeSKU',
                'QTY': 4,
                'Open': True,
                'Waypoints': [
                    [pd.NaT, '3500', 'NA', '0000111111', '', '2002FON6440'],
                    [pd.Timestamp('2023-01-03'), '1100', '1000', 'NA', '632', '2204NOM8139']
                ],
                'Unit_Value': 10,
                'Brand': 'SomeBrand',
                'Category': 'SomeCategory'
            },
            name='_some_ID'
        )
        out_items = tracker._make_route(ini_item)
        exp_item = [ini_item.copy(), ini_item.copy()]
        exp_item[0].name = ini_item.name+'.0'
        exp_item[0].QTY = 3
        exp_item[0].Waypoints = [
            [pd.NaT, '3500', 'NA', '0000111111', '', '2002FON6440'],
            [pd.Timestamp('2023-01-03'), '1100', '1000', 'NA', '632', '2204NOM8139'],
            [pd.Timestamp('2023-01-03'), '1100', '00204', np.nan, '311', '2204NOM8139']
        ]
        exp_item[1].name = ini_item.name+'.1'
        exp_item[1].QTY = 1
        exp_item[1].Waypoints = [
            [pd.NaT, '3500', 'NA', '0000111111', '', '2002FON6440'],
            [pd.Timestamp('2023-01-03'), '1100', '1000', 'NA', '632', '2204NOM8139'],
            [pd.Timestamp('2023-01-03'), '1100', 'Y05', np.nan, '311', '2204NOM8139']
        ]
        assert (
            out_items[0].equals(exp_item[0])
            and out_items[1].equals(exp_item[1])
        )
