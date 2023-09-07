""" processing.py
User-customised functions to enable algorithm to work.

Functions:
    import_movements
    is_entry_point
"""


import pandas as pd

def import_movements(filepath: str) -> pd.DataFrame:
    """import_movements - Steps 1, 2, and 3 should be customised to align to user's data:
    1. Reads a movement file, with dtypes specified in inputcols_dtypes
    2. Renames columns according to algorithm expectation
    3. Performs filtering
    4. Performs sorting - user may change at their own risk
    """
    
    inputcols_dtypes = {
        'Posting Date': 'datetime64[ns]',
        'Company': 'category',
        'Country ISO Code': 'category',
        'Material Document Number': str,
        'Purchase Order Document Number': 'category',
        'Special Stock Ind Code': 'category',
        'Movement Type Code': 'category',
        'Storage Location Code': 'category',
        'Sold to Customer': 'category',
        'Material Type Code': 'category',
        'Brand': 'category',
        'Category': 'category',
        'Material': 'category',
        'Batch No': str,
        'QTY': 'int16',
        'Standard Price': 'float32',
    }
    renaming_dict = {
        'Country ISO Code': 'Country',
        'Material Document Number': 'Document',
        'Purchase Order Document Number': 'PO',
        'Movement Type Code': 'Mvt Code',
        'Storage Location Code': 'SLOC',
        'Sold to Customer': 'Sold to',
        'Material': 'SKU',
        'Batch No': 'Batch',
        'Standard Price': 'Unit_Value'
    }
    output_cols = ['Posting Date', 'Company', 'Country', 'Document', 'PO',
                   'Special Stock Ind Code', 'Mvt Code', 'SLOC', 'Sold to',
                   'Brand', 'Category', 'SKU', 'Batch', 'QTY', 'Unit_Value']

    raw_mvt = (
        pd.read_excel(filepath, dtype=inputcols_dtypes)
        .rename(columns=renaming_dict)
        .pipe(lambda df: df.loc[df['Material Type Code'] == 'FERT'])
        .sort_values(by=['Posting Date', 'QTY'], ascending=[True, False])
    )

    for col_name in ['Special Stock Ind Code', 'SLOC']:
        raw_mvt[col_name] = raw_mvt[col_name].cat.add_categories('NA')
        raw_mvt[col_name].fillna('NA', inplace=True)

    return raw_mvt[output_cols]


def is_entry_point(item: pd.DataFrame | pd.Series) -> bool | pd.Series:
    """is_entry_point: Defines when to start tracking a product
    Should be customised regarding needs of user."""
    if isinstance(item, pd.Series):
        return (item['Mvt Code'] in ['632', '932', '956'])  & (item['Special Stock Ind Code'] == 'K')
    return (item['Mvt Code'].isin(['632', '932', '956']))  & (item['Special Stock Ind Code'] == 'K')

