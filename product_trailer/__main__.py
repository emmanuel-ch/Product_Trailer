import argparse
import os

from .standards import *

def main():
    from . import utils
    from .standards import _RAW_DIR_
    from .config import Config

    # Arg parser
    parser = argparse.ArgumentParser(
                    prog = 'Product-Trailer',
                    description = 'Tracking products through supply-chain network by using product movement logs.')
    parser.add_argument('profile_name')
    parser.add_argument('-r', '--raw-dir', default=_RAW_DIR_)
    parser.add_argument('-p', '--raw-prefix', default='Extract log')
    parser.add_argument('-ne', '--no-excel-report', default=False, action='store_true')
    args = parser.parse_args()

    print('\n\n', ' PRODUCT-TRAILER '.center(80, '#'), sep='')

    # Configuration
    config = Config(args.profile_name)

    # Process files
    utils.scan_new_input(args.raw_dir, config, prefix_input_files = args.raw_prefix)
    
    # Post-processing
    if not args.no_excel_report:
        from . import analysis_tk
        print('\nPost-processing...')

        tracked_file = config.fetch_saved_items()
        post_processed_analysis = analysis_tk.post_process(tracked_file)
        report_filename = analysis_tk.save_report(post_processed_analysis, config.reports_path, tab_with_waypoints=False)
        print(f'Post-processed report saved: {report_filename}')
    
    # End of program
    print('\n' + ' Program finished '.center(80, '#'), end='\n\n\n')


if __name__ == '__main__':
    main()