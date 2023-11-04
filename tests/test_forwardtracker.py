""" test_forwardtracker.py
Tests on ForwardTracker class.
"""

import pytest
import pandas as pd

from product_trailer.forwardtracker import ForwardTracker


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
    tracker = ForwardTracker(
        ['Posting Date', 'Company', 'SLOC', 'Sold to', 'Mvt Code', 'Batch'],
        mvts
    )
    return tracker

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
