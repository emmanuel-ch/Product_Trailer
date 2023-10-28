#!/usr/bin/env python
""" __main__.py
Called upon starting program.
"""

import argparse


def main() -> None:
    from product_trailer.scheduler import Scheduler
    from product_trailer.config import validate_configname, Config

    # Arg parser
    parser = argparse.ArgumentParser(
        description = (
            'Tracking products through supply-chain network',
            'by using product movement logs.'
        ))
    parser.add_argument('profile_name')
    parser.add_argument('-r', '--raw-dir', default='raw_data/')
    parser.add_argument('-p', '--raw-prefix', default='Extract log')
    parser.add_argument('-ne', '--no-excel-report',
                        default=False, action='store_true')
    args = parser.parse_args()


    print('\n\n', ' PRODUCT-TRAILER '.center(80, '#'), sep='')
    if not validate_configname(args.profile_name):
        print('Profile name not valid.',
              'Characters allowed (max 30): a-z, A-Z, 0-9, -_.,()')
    else:
        config = Config(args.profile_name)
        unprocessed_raw_files = config.find_unprocessed_files(
            args.raw_dir,
            args.raw_prefix
        )
        print(f'Detected {len(unprocessed_raw_files)} file(s) not processed.')
        for fpath in unprocessed_raw_files:
            config.incr_run_count()
            print(f'File: {fpath}', end='')
            new_raw_mvt = config.import_movements(fpath)
            scheduler = Scheduler(config)
            scheduler.prepare(new_raw_mvt)
            all_items, mvts_done = scheduler.run()
            print('Saved %s items' % all_items.shape[0])
            config.save_items(all_items)
            config.save_movements(mvts_done)
            config.record_inputfile_processed(fpath)
        
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