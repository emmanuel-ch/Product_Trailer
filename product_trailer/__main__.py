import argparse


def main():
    from . import utils
    from .config import Config

    # Arg parser
    parser = argparse.ArgumentParser(
                    prog = 'Product-Trailer',
                    description = 'Tracking products through supply-chain network by using product movement logs.')
    parser.add_argument('profile_name')
    parser.add_argument('-r', '--raw-dir', default='raw_data/')
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
        print('\nPost-processing... ', end='')
        tracked_items = config.fetch_saved_items()
        config.postprocess(tracked_items)
        print('Finished.')
    
    # End of program
    print('\n' + ' Program finished '.center(80, '#'), end='\n\n\n')


if __name__ == '__main__':
    main()