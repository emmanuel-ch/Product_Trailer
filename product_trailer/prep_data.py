import pandas as pd
from .standards import *

def prep_raw_mvt(filepath):
    
    raw_mvt_dtypes = {
        'Company': 'category',
        'Country ISO Code': 'category',
        'Material Document Number': 'category',
        'Purchase Order Document Number': 'category',
        'Special Stock Ind Code': 'category',
        'Movement Type Code': 'category',
        'Storage Location Code': 'category',
        'Sold to Customer': 'category',
        'Material Type Code': 'category',
        'Brand': 'category',
        'Category': 'category',
        'Material': 'category',
        'Batch No': 'category',
        'QTY': 'float64',
        'Standard Price': 'float64',
    }

    raw_mvt = pd.read_excel(filepath, \
                            dtype=raw_mvt_dtypes,
                            parse_dates=[0])

    raw_mvt.rename(columns={
        'Country ISO Code': 'Country',
        'Material Document Number': 'Document',
        'Purchase Order Document Number': 'PO',
        'Movement Type Code': 'Mvt Code',
        'Storage Location Code': 'SLOC',
        'Sold to Customer': 'Sold to',
        'Material': 'SKU',
        'Batch No': 'Batch',
        'Standard Price': 'Unit_Value'
    }, inplace=True)

    for col_name in ['Special Stock Ind Code', 'SLOC', 'Batch']:
        raw_mvt[col_name] = raw_mvt[col_name].cat.add_categories('NA')
        raw_mvt[col_name].fillna('NA', inplace=True)

    return raw_mvt.sort_values(by=['Posting Date', 'QTY'], ascending=[True, False])