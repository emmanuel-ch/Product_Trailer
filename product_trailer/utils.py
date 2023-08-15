""" Functions to support main workflow
"""

import pandas as pd
import numpy as np
import tqdm
from datetime import datetime
import os

from .standards import *
from .prep_data import prep_raw_mvt
from .config import Config

# LINE_PROFILER
# import line_profiler
# profiler = line_profiler.LineProfiler()


def open_db(db_filename=False):
    import pandas as pd
    return pd.read_pickle(db_filename)

def save_db(df_mvt, db_filename):
    df_mvt.to_pickle(db_filename)


def scan_new_input(foldername, db_name, prefix_input_files='', save_mvts=False, one_db=True):
    import pickle

    config = Config(db_name, _DB_DIR_)
    unprocessed_raw_files = config.find_unprocessed_files(foldername, prefix_input_files)
    print(f'Detected {len(unprocessed_raw_files)} file(s) not processed.\n')

    tracked = None
    for filename in unprocessed_raw_files:
        tracked = process_mvt_file(os.path.join(foldername, filename), db_name, save_mvts, one_db)
        config.record_inputfile_processed(filename)

    return tracked


def prep_mvt_tracking_db(new_raw_mvt=None):
    import pandas as pd
    new_mvts = new_raw_mvt[_MVT_DB_COLUMNS_].copy()
    new_mvts['QTY_Unallocated'] = new_mvts['QTY'].apply(lambda qty: max(qty, -qty))
    new_mvts['Items_Allocated'] = new_mvts.apply(lambda r: [], result_type='reduce', axis=1)
    return new_mvts


def prep_trackedItems_db(df_tracked_items=None, new_tracked_items=None):
    import pandas as pd
    list_df = []
    if isinstance(df_tracked_items, pd.DataFrame):
        list_df.append(df_tracked_items)
    if isinstance(new_tracked_items, pd.DataFrame):
        list_df.append(new_tracked_items)
    return pd.concat(list_df)


def extract_items(raw_mvt):
    
    # 1: Select the returns and prep data
    trailed_products = raw_mvt.loc[(raw_mvt['Mvt Code'].isin(_RETURN_CODES_)) \
                             & (raw_mvt['Special Stock Ind Code'] == 'K') \
                             & (raw_mvt['Material Type Code'] == 'FERT')].copy()
    trailed_products['ID_temp'] = '_' + trailed_products['Company'].astype(str) + '/' \
                               + trailed_products['Sold to'].astype(str).str[4:11] + '/' + trailed_products['Posting Date'].astype(str) + '_' \
                               + trailed_products['SKU'].astype('str') + ':' + trailed_products['Batch'].astype('str')
    trailed_products['NbLines'] = (-trailed_products['QTY']).apply(np.floor).astype(int)

    # 2: Make it granular - 1 line = 1 EA
    tp_granular = trailed_products \
        .pivot_table(index=['ID_temp', 'Mvt Code'], values='QTY', aggfunc=np.sum) \
        .reset_index() \
        .merge(
            trailed_products[['ID_temp', *_ID_SPECS_, 'Standard Price']] \
                .drop_duplicates(subset=['ID_temp', 'Mvt Code']), \
            on=['ID_temp', 'Mvt Code'],
            how='left') \
        .assign(NbLines = lambda row: np.floor(-row['QTY']).astype(int)) \
        .sort_values(by='ID_temp')
    
    # 3: Multiply the lines & give them a name
    tp_final = tp_granular.loc[tp_granular.index.repeat(tp_granular['NbLines'])].copy().sort_values(by=['ID_temp']).reset_index()
    
    global prev_
    prev_ = ['none', 0] # Previous ID, ID line number
    def generate_ID_LineQTY(row):
        global prev_
        this_line_ordinal = 1
        if row['ID_temp'] == prev_[0]:
            this_line_ordinal += prev_[1]
        prev_ = [row['ID_temp'], this_line_ordinal]
        return [row['ID_temp'] + ':' + str(this_line_ordinal), 1, row['Mvt Code']]
    tp_final[['ID', 'QTY_Returned', 'Mvt Code']] = tp_final.apply(lambda r: generate_ID_LineQTY(r), axis=1, result_type ='expand')

    # 4: Cleanup and add 1st waypoint
    tp_final.set_index('ID', inplace=True)
    tp_final['Open'] = True
    tp_final['Waypoints'] = tp_final.apply(lambda row: [row[_WAYPOINTS_DEF_].to_list()], axis=1)
    tp_final.drop(columns=['QTY', 'NbLines', 'ID_temp', 'index', 'Mvt Code'], inplace=True)
    
    tp_final.rename(columns={
            'Posting Date': 'Return_Date',
            'Country': 'First_Country',
            'Company': 'First_Company',
            'SLOC': 'First_SLOC',
            'Sold to': 'First_Soldto',
            'Batch': 'First_Batch'
        }, inplace=True)

    return tp_final

# @profiler  # LINE_PROFILER
def next_hop(ID, MVT_DB, tracked_items):
    """ Carry out next hop for specified ID.
    Returns:
        True if could find a hop (incl half-hop) and successfully updated databases.
        False if moving forward is not needed (part is burnt, or not able to find a -1 mvt)"""
    
    waypoints_list = tracked_items.loc[ID, 'Waypoints']
    this_is_first_step = len(waypoints_list) == 1
    latest_wpt = waypoints_list[-1]
    
    #
    # 1: Find where the product currently is, and which Mvt code takes it away.
    #
    if np.isnan(tracked_items.loc[ID, 'Open']):  # It's not Open nor Closed: it's a PO for which we are looking for the receipt mvt
        minus1_line = pd.Series({
            'Posting Date': latest_wpt[0],
            'Batch': latest_wpt[5],
            'PO': latest_wpt[4],
            'Mvt Code': 'PO'
        })
    else:
        if not this_is_first_step:
            if latest_wpt[2] == 'NA': # Add filter on SoldTo if SKU is in consignment
                minus1_line = MVT_DB.loc[
                    (MVT_DB['Posting Date'].values >= latest_wpt[0]) & \
                    (MVT_DB['Company'].values == latest_wpt[1]) & \
                    (MVT_DB['SLOC'].values == latest_wpt[2]) & \
                    (MVT_DB['Sold to'].values == latest_wpt[3]) & \
                    (MVT_DB['Batch'].values == latest_wpt[5]) & \
                    (MVT_DB['Items_Allocated'].apply(lambda Item_Allocated: ID not in Item_Allocated)) & \
                    (MVT_DB['QTY'].values <= -1) & \
                    (MVT_DB['QTY_Unallocated'].values >= 1)
                ]
            else:
                minus1_line = MVT_DB.loc[
                    (MVT_DB['Posting Date'].values >= latest_wpt[0]) & \
                    (MVT_DB['Company'].values == latest_wpt[1]) & \
                    (MVT_DB['SLOC'].values == latest_wpt[2]) & \
                    (MVT_DB['Batch'].values == latest_wpt[5]) & \
                    (MVT_DB['Items_Allocated'].apply(lambda Item_Allocated: ID not in Item_Allocated)) & \
                    (MVT_DB['QTY'].values <= -1) & \
                    (MVT_DB['QTY_Unallocated'].values >= 1)
                ]
        else: # We're looking for the 1st movement of the tracked product
            minus1_line = MVT_DB.loc[
                (MVT_DB['Posting Date'].values == latest_wpt[0]) & \
                (MVT_DB['Company'].values == latest_wpt[1]) & \
                (MVT_DB['SLOC'].values == latest_wpt[2]) & \
                (MVT_DB['Sold to'].values == latest_wpt[3]) & \
                (MVT_DB['Batch'].values == latest_wpt[5]) & \
                (MVT_DB['Mvt Code'].values == latest_wpt[4]) & \
                (MVT_DB['QTY'].values <= -1) & \
                (MVT_DB['QTY_Unallocated'].values >= 1)
            ]
        
        # Nothing found: The product didn't move
        if len(minus1_line) == 0:
            return (False, MVT_DB, tracked_items)
        
        minus1_line = minus1_line.iloc[0]  # In case multiple lines are found, we take the 1st one
        minus1_line_idx = minus1_line.name
        
        # Check if it's a re-return
        if (not this_is_first_step) and (minus1_line['Mvt Code'] in _RETURN_CODES_):
            new_wpt = [minus1_line['Posting Date'], minus1_line['Company'], f"RE-RETURNED", latest_wpt[3], minus1_line['Mvt Code'], latest_wpt[5]]
            tracked_items.loc[ID, 'Waypoints'].append(new_wpt)
            tracked_items.loc[ID, 'Open'] = False
            return (False, MVT_DB, tracked_items)
    
    #
    # 2: We have the -1, let's find the +1
    # We start with the exceptions, and the general case is down
    #
    if minus1_line['Mvt Code'] == '956': # Change of SoldTo
        plus1_line = MVT_DB.loc[
            (MVT_DB['Posting Date'].values == minus1_line['Posting Date']) & \
            (MVT_DB['Company'].values == minus1_line['Company']) & \
            (MVT_DB['Mvt Code'].values == '955') & \
            (MVT_DB['Batch'].values == minus1_line['Batch']) & \
            (MVT_DB['QTY'].values >= 1) & \
            (MVT_DB['QTY_Unallocated'].values >= 1)
        ]
    elif minus1_line['Mvt Code'] == '702': # Change of batch number. Product remains in same SLOC & SoldTo
        plus1_line = MVT_DB.loc[
            (MVT_DB['Posting Date'].values == minus1_line['Posting Date']) & \
            (MVT_DB['Company'].values == minus1_line['Company']) & \
            (MVT_DB['SLOC'].values == minus1_line['SLOC']) & \
            (MVT_DB['Sold to'].values == minus1_line['Sold to']) & \
            (MVT_DB['Mvt Code'].values == '701') & \
            (MVT_DB['QTY'].values >= 1) & \
            (MVT_DB['QTY_Unallocated'].values >= 1)
        ]
    elif minus1_line['PO'] != '-2': # If it's a PO, we don't check with the mvt codes
        plus1_line = MVT_DB.loc[
            (MVT_DB['Posting Date'].values >= minus1_line['Posting Date']) & \
            (MVT_DB['Batch'].values == minus1_line['Batch']) & \
            (MVT_DB['PO'].values == minus1_line['PO']) & \
            (MVT_DB['QTY'].values >= 1) & \
            (MVT_DB['QTY_Unallocated'].values >= 1)
        ]
    else:  # Standard mvt
        plus1_line = MVT_DB.loc[
            (MVT_DB['Posting Date'].values == minus1_line['Posting Date']) & \
            (MVT_DB['Company'].values == minus1_line['Company']) & \
            (MVT_DB['Sold to'].values == minus1_line['Sold to']) & \
            (MVT_DB['Batch'].values == minus1_line['Batch']) & \
            (MVT_DB['Mvt Code'].values == minus1_line['Mvt Code']) & \
            (MVT_DB['Document'].values == minus1_line['Document']) & \
            (MVT_DB['QTY'].values >= 1) & \
            (MVT_DB['QTY_Unallocated'].values >= 1)
        ]
    
    # No 1st-pass result for a +1: we widen the search
    if (len(plus1_line) == 0) and (np.isnan(tracked_items.loc[ID, 'Open'])):  # Except if we were looking for 2nd half of PO but didn't find it
        return (False, MVT_DB, tracked_items)
    elif (len(plus1_line) == 0) and (not np.isnan(tracked_items.loc[ID, 'Open'])):
        # Last chance to find a +1: let's remove the filter on batch#
        plus1_line = MVT_DB.loc[
            (MVT_DB['Posting Date'].values == minus1_line['Posting Date']) & \
            (MVT_DB['Company'].values == minus1_line['Company']) & \
            (MVT_DB['Sold to'].values == minus1_line['Sold to']) & \
            (MVT_DB['Mvt Code'].values == minus1_line['Mvt Code']) & \
            (MVT_DB['Document'].values == minus1_line['Document']) & \
            (MVT_DB['QTY'].values >= 1) & \
            (MVT_DB['QTY_Unallocated'].values >= 1)
        ]

        # Part is consumed, scrapped or has moved away from country
        if len(plus1_line) == 0:
            if minus1_line['PO'] != '-2':  # It's on a PO, so it's potentially somewhere in the network
                new_wpt = [minus1_line['Posting Date'], minus1_line['Company'], f"PO FROM {minus1_line['SLOC']}, mvt {minus1_line['Mvt Code']}", minus1_line['Sold to'], minus1_line['PO'], minus1_line['Batch']]
                tracked_items.loc[ID, 'Waypoints'].append(new_wpt)
                tracked_items.loc[ID, 'Open'] = np.nan
                MVT_DB.loc[minus1_line_idx, 'QTY_Unallocated'] -= 1
                MVT_DB.loc[minus1_line_idx, 'Items_Allocated'].append(ID)
                return (False, MVT_DB, tracked_items)
            else:  # It's not on a PO, so it's been burnt
                new_wpt = [minus1_line['Posting Date'], minus1_line['Company'], f"BURNT {minus1_line['SLOC']}", minus1_line['Sold to'], minus1_line['Mvt Code'], minus1_line['Batch']]
                tracked_items.loc[ID, 'Waypoints'].append(new_wpt)
                tracked_items.loc[ID, 'Open'] = False
                MVT_DB.loc[minus1_line_idx, 'QTY_Unallocated'] -= 1
                MVT_DB.loc[minus1_line_idx, 'Items_Allocated'].append(ID)
                return (False, MVT_DB, tracked_items)

    plus1_line = plus1_line.iloc[0]
    plus1_line_idx = plus1_line.name

    if not np.isnan(tracked_items.loc[ID, 'Open']):  # If it's not 2nd part of a PO
        MVT_DB.loc[minus1_line_idx, 'QTY_Unallocated'] -= 1
        MVT_DB.loc[minus1_line_idx, 'Items_Allocated'].append(ID)
    else:  # If it's the 2nd half of a PO, then we set it back to Open
        tracked_items.loc[ID, 'Open'] = True
    
    MVT_DB.loc[plus1_line_idx, 'QTY_Unallocated'] -= 1
    MVT_DB.loc[plus1_line_idx, 'Items_Allocated'].append(ID)
    
    new_wpt = list(MVT_DB.loc[plus1_line_idx, _WAYPOINTS_DEF_])
    if new_wpt[2] != 'NA': # Remove SoldTo if the SLOC isn't a Consignment
        new_wpt[3] = np.nan
    
    if minus1_line['Mvt Code'] != plus1_line['Mvt Code']: # Combination of codes
        new_wpt[4] = minus1_line['Mvt Code'] + '/' + plus1_line['Mvt Code']
    
    if this_is_first_step: # Remove 1st mvt code
        tracked_items.loc[ID, 'Waypoints'][0][4] = '-'
    
    tracked_items.loc[ID, 'Waypoints'].append(new_wpt)
    return (True, MVT_DB, tracked_items)


#@profiler
def process_task(task, Items_open, MVT_DB):
    task_items = Items_open.loc[(Items_open['SKU'] == task)].copy()
    task_MVTs = MVT_DB.loc[(MVT_DB['SKU'] == task)].copy()

    if len(task_MVTs) == 0:  # No mvt => Skip this
        return task_items, task_MVTs

    for ID in task_items.index:  # Do the work on task_item and task_MVTs ...
        hop_again = True
        while hop_again:
            hop_again, task_MVTs, task_items = next_hop(ID, task_MVTs, task_items)
    
    return task_items, task_MVTs


def process_mvt_file(filepath, db_name, save_mvts=False, one_db=True):
    
    print(f"""########## Process new movements ##########
 - Movement file: {filepath}
 - DB name: {db_name} (Will be created if it doesn't exist)""")
    
    # ############################## PART 0 ##############################
    db_path = os.path.join(_DB_DIR_, db_name)
    
    
    # ############################## PART 1 ##############################
    print('Reading new movement data... ', end='')
    new_raw_mvt = prep_raw_mvt(filepath)
    print('Preparing it... ', end='')
    MVT_DB = prep_mvt_tracking_db(new_raw_mvt)
    max_MVT_date = MVT_DB['Posting Date'].max().strftime("%Y-%m-%d")
    print(f'Done {MVT_DB.shape}')

    # Prepare the database of products that will be trailed
    # BUG TO CORRECT: Do NOT open a DB which already contains the Items we want to track
    print('Preparing product tracking database... ', end='')
    new_tracked_items = extract_items(new_raw_mvt)
    
    
    if not os.path.isdir(db_path):
        os.makedirs(db_path)
    
    if os.path.isdir(db_path):
        possible_db = [filename for filename in ['nope'] + os.listdir(db_path) if filename.startswith(_TRACKED_ITEMS_DB_PREFIX_)]
        if len(possible_db) == 0:
            print('New database... ', end='')
            tracked_items = prep_trackedItems_db(None, new_tracked_items)
            print(f'Done {tracked_items.shape}')
            prev_dbfilename_items = ''
        else:
            db_trackedItems_filename = sorted(possible_db)[-1]
            db_name_items = db_trackedItems_filename[max(0,db_trackedItems_filename.rfind('/')):]
            print(f'Recovering DB: {db_name_items} ... ', end='')
            prev_dbfilename_items = os.path.join(db_path, db_trackedItems_filename)
            tracked_items = prep_trackedItems_db(open_db(prev_dbfilename_items), new_tracked_items)
            print(f'Adding {new_tracked_items.shape[0]} records, total {tracked_items.shape[0]}. Done.')
    else:
        return False
    
    # ############################## PART 2 ##############################
    print('Preparing for computation... ', end='')
    Items_open = tracked_items.loc[tracked_items['Open'].fillna(True)].copy()
    list_computed_items = [tracked_items.loc[~tracked_items['Open'].fillna(True)].copy()] # List of df for Items which are closed. Will be toped up with the ones we will work on.
    
    tasks_queue = (
        Items_open
        .value_counts(['SKU'])
        .loc[Items_open.value_counts(['SKU']).gt(0)]
        .reset_index()['SKU']
        .to_list()
    )  # 1 task = 1 SKU
    
    mvt_rm_columns = ['Country', 'Special Stock Ind Code', 'Material Type Code', 'Brand', 'Category']
    MVT_DB = MVT_DB.drop(columns=mvt_rm_columns)
    MVT_DB = MVT_DB.loc[MVT_DB['SKU'].isin(tasks_queue)].sort_values(by='Posting Date', ascending=True) # Select the combinations which are interesting
    list_computed_MVTS = [] # Empty list for now
    print('Done.')

    # Looping through the tasks
    for task in (pbar := tqdm.tqdm(tasks_queue, desc='Crunching data... ')):
        pbar.set_postfix({'SKU': task}, refresh=False)

        out_items, out_MVTs = process_task(task, Items_open, MVT_DB)
        list_computed_items.append(out_items)
        if save_mvts:
            list_computed_MVTS.append(out_MVTs)
    
    tracked_items = pd.concat(list_computed_items, axis=0)
    

    # ############################## PART 3 ##############################
    print('Saving...', end='')
    
    date_range_db = tracked_items['Return_Date'].min().strftime("%Y-%m-%d") + "..." + max_MVT_date
    datetime_db_creation = datetime.today().strftime("%Y-%m-%d %Hh%M")
    
    if save_mvts: # Save MVT database if it's wanted
        MVT_DB = pd.concat(list_computed_MVTS, axis=0)
        possible_mvt_db = [filename for filename in ['nope'] + os.listdir(db_path) if filename.startswith(_MVT_DB_PREFIX_)]
        if len(possible_mvt_db) == 0:
            new_MVT_DB = MVT_DB
            prev_dbfilename_MVTS = ''
        else:
            db_mvts_filename = sorted(possible_mvt_db)[-1]
            prev_dbfilename_MVTS = os.path.join(db_path, db_mvts_filename)
            MVT_DB_old = open_db(prev_dbfilename_MVTS)
            new_MVT_DB = pd.concat([MVT_DB_old, MVT_DB], axis=0)
        
        new_db_mvt_filename = os.path.join(db_path, f'{_MVT_DB_PREFIX_} {date_range_db} (saved {datetime_db_creation}).pkl')
        save_db(new_MVT_DB, new_db_mvt_filename)
        
        if one_db:
            if prev_dbfilename_MVTS != '':
                os.remove(prev_dbfilename_MVTS)
        
    else:
        new_db_mvt_filename = '(Movements not saved)'
    
    # Save database of tracked products
    new_db_items = os.path.join(db_path, f'{_TRACKED_ITEMS_DB_PREFIX_} {date_range_db} (saved {datetime_db_creation}).pkl')
    save_db(tracked_items, new_db_items)
    
    if one_db:
        if prev_dbfilename_items != '':
            os.remove(prev_dbfilename_items)
    
    print(f'Done. Output files:\n  {new_db_mvt_filename}\n  {new_db_items}\nMain computation finished.')
    # profiler.print_stats()  # LINE_PROFILER
    return new_db_items

