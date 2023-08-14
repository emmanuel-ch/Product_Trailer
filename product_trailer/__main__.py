import argparse
import os

from .standards import *

def main():
    from . import utils
    from .standards import _RAW_DIR_

    # Arg parser
    parser = argparse.ArgumentParser(
                    prog = 'Product-Trailer',
                    description = 'Tracking products through supply-chain network by using product movement logs.')
    parser.add_argument('db_name')
    parser.add_argument('-r', '--raw-dir', default=_RAW_DIR_)
    parser.add_argument('-p', '--raw-prefix', default='Extract log')
    parser.add_argument('-s', '--save-mvt', default=False, action='store_true')
    parser.add_argument('-k', '--keep-old-db', default=False, action='store_true')
    parser.add_argument('-ne', '--no-excel-report', default=False, action='store_true')
    args = parser.parse_args()

    print('\n\n', ' PRODUCT-TRAILER '.center(80, '#'), sep='')

    # Process files
    tracked = utils.raw_scan_process(args.raw_dir, 
                                     args.db_name, 
                                     prefix_input_files = args.raw_prefix, 
                                     save_mvts = args.save_mvt, 
                                     one_db = not args.keep_old_db)
    
    # Save to excel
    if not args.no_excel_report:
        print('Post-processing...')

        if tracked is None:
            db_path = os.path.join(_DB_DIR_, args.db_name)
            possible_db = [filename for filename in ['nope'] + os.listdir(db_path) if filename.startswith(_TRACKED_ITEMS_DB_PREFIX_)]
            if len(possible_db) == 0:
                print(f'No DB exists')
            else:
                tracked = sorted(possible_db)[-1]
                tracked = os.path.join(db_path, tracked)

    
    from . import analysis_tk
    tracked_file = utils.open_db(tracked)
    post_processed_analysis = analysis_tk.post_process(tracked_file)
    report_filename = analysis_tk.save_report(post_processed_analysis, tab_with_waypoints=False)
    print(f'Post-processed report saved: {report_filename}\n')
    
    # End of program
    print(' Program finished '.center(80, '#'), end='\n\n\n')


if __name__ == '__main__':
    main()