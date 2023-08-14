_ID_SPECS_ = ['Country', 'Company', 'SLOC', 'Sold to', 'SKU', 'Brand', 'Category', 
              'Batch', 'Posting Date', 'Mvt Code']

_WAYPOINTS_DEF_ = ['Posting Date', 'Company', 'SLOC', 'Sold to', 'Mvt Code', 'Batch']  # Order matters!

_MVT_DB_COLUMNS_ = ['Posting Date', 'Company', 'Country', 'Document', 'PO',
                    'Special Stock Ind Code', 'Mvt Code', 'SLOC', 'Sold to', 'Material Type Code',
                    'Brand', 'Category', 'SKU', 'Batch', 'QTY']

_RETURN_CODES_ = ['632', '932', '956']

_RAW_DIR_ = 'raw_data/'
_DB_DIR_ = 'databases/'
_REPORTS_DIR_ = 'reports_out/'
_POSTPROCESSING_DIR_ = 'post_processing/'

_FILENAME_MVT_FILES_PROCESSED_ = 'input_files_processed.pkl'
_MVT_DB_PREFIX_ = 'All movements _'
_TRACKED_ITEMS_DB_PREFIX_ = 'Tracked _'


__all__ = ['_ID_SPECS_',
           '_WAYPOINTS_DEF_',
           '_MVT_DB_COLUMNS_',
           '_RETURN_CODES_',
           '_RAW_DIR_',
           '_DB_DIR_',
           '_REPORTS_DIR_',
           '_POSTPROCESSING_DIR_',
           '_FILENAME_MVT_FILES_PROCESSED_',
           '_MVT_DB_PREFIX_',
           '_TRACKED_ITEMS_DB_PREFIX_']