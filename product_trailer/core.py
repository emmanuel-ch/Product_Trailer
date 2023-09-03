""" core.py
Core functions defining the algorithm.

Functions:
    scan_new_input
    process_mvt_file
    prep_mvt_tracking_db
    extract_items
    process_items_group
    compute_route
    compute_hop
    compute_plus_mvts
    construct_new_item
    find_minus_lines
    find_plus_lines
    find_plus_lines_nobatch
"""

import pandas as pd
import numpy as np
import tqdm
import os

_WAYPOINTS_DEF_ = ['Posting Date', 'Company', 'SLOC', 'Sold to', 'Mvt Code', 'Batch']  # Order matters!


# Enable # @profiler + Saving at end of process_mvt_file()
# import line_profiler
# profiler = line_profiler.LineProfiler()


def scan_new_input(foldername: str, config: str, prefix_input_files: str = '') -> None | pd.DataFrame:
    unprocessed_raw_files = config.find_unprocessed_files(foldername, prefix_input_files)
    print(f'Detected {len(unprocessed_raw_files)} file(s) not processed.')

    tracked = None
    for filename in unprocessed_raw_files:
        tracked = process_mvt_file(os.path.join(foldername, filename), config)
        config.record_inputfile_processed(filename)

    return tracked


def process_mvt_file(filepath: str, config) -> bool:
    print(f"\n##### Process new movements: {filepath}", end='')
    
    # ############################## PART 1 ##############################
    new_raw_mvt = config.import_movements(filepath)
    MVT_DB = prep_mvt_tracking_db(new_raw_mvt, config)
    max_MVT_date = MVT_DB['Posting Date'].max().strftime("%Y-%m-%d")
    print(f' [x{MVT_DB.shape[0]}]')

    # BUG TO CORRECT: Do NOT open a DB which already contains the Items we want to track
    print('Preparing items... ', end='')
    new_tracked_items = extract_items(new_raw_mvt, config)
    saved_items = config.fetch_saved_items()
    if isinstance(saved_items, pd.DataFrame):
        tracked_items = pd.concat([saved_items, new_tracked_items])
        print(f'{saved_items.shape[0]} saved + {new_tracked_items.shape[0]} new = Total {tracked_items.shape[0]}')
    else:
        tracked_items = new_tracked_items
        print(f'0 saved + {new_tracked_items.shape[0]} new = Total {tracked_items.shape[0]}')
    
    # ############################## PART 2 ##############################
    Items_open = tracked_items.loc[tracked_items['Open'].fillna(True)].copy()
    list_computed_items = [tracked_items.loc[~tracked_items['Open'].fillna(True)].copy()] # List of df for Items which are closed. Will be toped up with the ones we will work on.
    
    tasks_queue = (
        Items_open
        .value_counts(['SKU'])
        .loc[Items_open.value_counts(['SKU']).gt(0)]
        .reset_index()['SKU']
        .to_list()
    )  # 1 task = 1 SKU
    
    MVT_DB = MVT_DB.loc[MVT_DB['SKU'].isin(tasks_queue)]  # Select the combinations which are interesting
    MVT_DB['Company_SLOC_Batch'] = MVT_DB['Company'].astype(str) + '-' + MVT_DB['SLOC'].astype(str) + '-' + MVT_DB['Batch']
    list_computed_MVTS = [] # Empty list for now

    # Looping through the tasks
    for task in (pbar := tqdm.tqdm(tasks_queue, desc='Crunching... ')):
        pbar.set_postfix({'SKU': task}, refresh=False)

        task_Items_open = Items_open.loc[(Items_open['SKU'] == task)].copy()
        task_Movements = MVT_DB.loc[(MVT_DB['SKU'] == task)].copy()
        out_items, out_MVTs = process_items_group(task_Items_open, task_Movements)
        list_computed_items.append(out_items)
        if config.db_config['save_movements']:
            list_computed_MVTS.append(out_MVTs)
    
    tracked_items = pd.concat(list_computed_items, axis=0)
    
    # ############################## PART 3 ##############################
    print(f'Saving {tracked_items.shape[0]} items... ', end='')
    date_range_db = tracked_items['Return_Date'].min().strftime("%Y-%m-%d") + "..." + max_MVT_date
    config.save_items(tracked_items, date_range_db)
    config.save_movements(list_computed_MVTS, date_range_db)
    print(f'Done. Input file successfully processed.')

    # For profiling only
    # import contextlib
    # with open(f'profile.txt', 'w') as f:
    #     with contextlib.redirect_stdout(f):
    #         profiler.print_stats()
    
    return True


# @profiler
def prep_mvt_tracking_db(new_raw_mvt: pd.DataFrame, config) -> pd.DataFrame:
    new_mvts = (
        new_raw_mvt
        .copy()
        .drop(columns=[*config.input_features['company_features'], 
                       *config.input_features['sku_features'], 
                       'Special Stock Ind Code', 'Unit_Value'])
        .assign(QTY_Unallocated = lambda df: df['QTY'].apply(abs))
        .assign(Items_Allocated = lambda df: df.apply(lambda _: set(), result_type='reduce', axis=1))
    )
    return new_mvts

# @profiler
def extract_items(raw_mvt: pd.DataFrame, config) -> pd.DataFrame:

    ID_definition = ['Company', 'SLOC', 'Sold to', 'Mvt Code', 'Posting Date', 'SKU', 'Batch']
    company_features = ['Company', *config.input_features['company_features']]
    sku_features = ['SKU', *config.input_features['sku_features']]

    def build_ID(item):
        return f"_{item['Company']}/{item['SLOC']}/{item['Sold to'][4:11]}_" \
                + f"{item['Mvt Code']}/{item['Posting Date']:%Y-%m-%d}_" \
                + f"{item['SKU']}:{item['Batch']}"

    trailed_products = (
        raw_mvt
        .copy()
        .pipe(lambda df: df.loc[df.apply(config.is_entry_point, axis=1)])
        .pivot_table(observed=True, values=['Unit_Value', 'QTY'], aggfunc={'Unit_Value': 'mean', 'QTY': 'sum'}, index=ID_definition)
        .reset_index()
        .assign(ID = lambda df: df.apply(build_ID, axis=1))
        .merge(raw_mvt.value_counts(company_features).reset_index()[company_features].drop_duplicates(keep='first'), on='Company')
        .merge(raw_mvt.value_counts(sku_features).reset_index()[sku_features].drop_duplicates(keep='first'), on='SKU')
        .set_index('ID')
        .assign(Open = True)
        .assign(QTY = lambda df: -df['QTY'])
        .assign(Waypoints = lambda df: df.apply(lambda row: [row[_WAYPOINTS_DEF_].values.tolist()], axis=1))
        .rename(columns={
            'Posting Date': 'Return_Date',
            'Country': 'First_Country',
            'Company': 'First_Company',
            'SLOC': 'First_SLOC',
            'Sold to': 'First_Soldto',
            'Batch': 'First_Batch'
        })
    )

    return trailed_products


# @profiler
def process_items_group(task_items: pd.DataFrame, task_MVTs: pd.DataFrame) -> (pd.DataFrame, pd.DataFrame):
    if len(task_MVTs) == 0:  # No mvt => Skip this
        return task_items, task_MVTs
    
    # items_computed is a list of pd.Series
    items_computed = []
    for _, row in task_items.iterrows():
        items_computed.extend(compute_route(row, task_MVTs))
    df_items_computed = pd.DataFrame(items_computed) 
    
    return df_items_computed, task_MVTs


# @profiler
def compute_route(item: pd.Series, task_MVTs: pd.DataFrame) -> list:

    list_new_items = compute_hop(item, task_MVTs)

    if len(list_new_items) == 0:
        return []
    elif len(list_new_items) == 1:
        if item.equals(list_new_items[0]):  # The product didn't travel further, that's all we see.
            return [item]
        
    out =  [an_item for new_item in list_new_items for an_item in compute_route(new_item, task_MVTs)]
    return out


# @profiler
def compute_hop(item: pd.Series, task_MVTs: pd.DataFrame) -> list:

    this_is_first_step = len(item.Waypoints) == 1

    if not np.isnan(item.Open):
        minus_mvts = find_minus_lines(this_is_first_step, item.Waypoints[-1], task_MVTs, item.name)
    else:
        minus_mvts = pd.DataFrame({
            'Posting Date': [item.Waypoints[-1][0]],
            'Batch': [item.Waypoints[-1][5]],
            'PO': [item.Waypoints[-1][4]],
            'Mvt Code': ['PO'],
            'Company': [item.Waypoints[-1][1]], 
            'SLOC': [item.Waypoints[-1][2]], 
            'Sold to': [item.Waypoints[-1][3]],
            'QTY': [-item.QTY],
            'QTY_Unallocated': [item.QTY],
            'Items_Allocated': [set()]
        })

    if len(minus_mvts) == 0:  # Nothing found: The product didn't move
        if this_is_first_step:
            # DOUBLE-COUNTING PREVENTION
            # If we are here, it means the tracked product passes by an "entry point".
            # To avoid double-counting, decision was made to keep tracking the already tracked product, 
            # and do not register the new entry.
            return []
        return [item]
    
    new_items = []
    
    multiple_minuses = -minus_mvts.iloc[0].QTY < item.QTY
    sub_ID_lv1 = 0
    QTY_covered = 0

    for minus_idx, minus_mvt in minus_mvts.iterrows():
        hop_minus_QTY = min(item.QTY - QTY_covered, -minus_mvt.QTY)  # This value is >0
        plus_resolved = compute_plus_mvts(minus_mvt, hop_minus_QTY, task_MVTs, item.name)

        for sub_ID_2, this_plus_resolved in enumerate(plus_resolved):
            if multiple_minuses:
                sub_ID = str(sub_ID_lv1) + ('.' + str(sub_ID_2) if (len(plus_resolved) > 1) else '')
            else:
                sub_ID = str(sub_ID_2) if (len(plus_resolved) > 1) else False

            new_item = construct_new_item(item, instruction = 'standard',
                                          data = {'minus_mvt': minus_mvt, 'qty': this_plus_resolved['qty'], 'plus_mvt': this_plus_resolved['plus_mvt']},
                                          sub_ID=sub_ID)
            new_items.append(new_item)

        if minus_mvt['Mvt Code'] != 'PO':
            task_MVTs.loc[minus_idx, 'QTY_Unallocated'] -= hop_minus_QTY
            task_MVTs.loc[minus_idx, 'Items_Allocated'].add(item.name)
        
        
        QTY_covered += hop_minus_QTY
        if QTY_covered == item.QTY:
            break
        elif QTY_covered > item.QTY:
            raise Exception(f'Over-cover! Covered {QTY_covered} [-] for item qty {item.QTY}')

        sub_ID_lv1 += 1
    
    if QTY_covered < item.QTY:
        new_item = construct_new_item(item, instruction = 'LastMinusNeeds_subID',
                                      data = {'qty': item.QTY - QTY_covered},
                                      sub_ID=str(sub_ID_lv1))
        new_items.append(new_item)
    
    return new_items


# @profiler
def compute_plus_mvts(minus_mvt: pd.Series, desired_QTY: int, task_MVTs: pd.DataFrame, ID: str) -> list:
    """Tried to find the [+] mvts: where the product has moved to.
    minus_mvt: Info about the [-] mvt
    desired_QTY: the quantity we track
    task_MVTs: A df containing movements"""
    plus_mvts = find_plus_lines(minus_mvt, task_MVTs)

    if len(plus_mvts) == 0:  # No 1st-pass result for a +1: we widen the search
        if minus_mvt['PO'] != '-2':  # Except if we were looking for 2nd half of PO but didn't find it (maybe in next report?)
            return [{'qty': desired_QTY, 'plus_mvt': 'PO2ndPartMissing'}]
        
        plus_mvts = find_plus_lines_nobatch(minus_mvt, task_MVTs) # Last chance to find a [+]: let's remove the filter on batch#
        if len(plus_mvts) == 0:  # Part is burnt or is on a PO
            return [{'qty': desired_QTY, 'plus_mvt': 'BURNT'}]
    
    QTY_covered = 0
    plus_resolved = []
    for plus_idx, plus_mvt in plus_mvts.iterrows():
        addnl_cover_QTY = min(plus_mvt.QTY, desired_QTY-QTY_covered)
        plus_resolved.append({'qty': addnl_cover_QTY, 'plus_mvt': plus_mvt})

        task_MVTs.loc[plus_idx, 'QTY_Unallocated'] -= addnl_cover_QTY
        task_MVTs.loc[plus_idx, 'Items_Allocated'].add(ID)
        

        QTY_covered += addnl_cover_QTY
        if QTY_covered >= desired_QTY:
            break
    
    if QTY_covered < desired_QTY:  # Ex: If we found 4 [+] for 5 [-], we assume the last unit was burnt
        plus_resolved.append({'qty': desired_QTY-QTY_covered, 'plus_mvt': 'BURNT'})
    
    return plus_resolved
    

# @profiler
def construct_new_item(item: pd.Series, instruction: str, data: dict, sub_ID: bool | str) -> pd.Series:
    new_item = item.copy(deep=True)
    new_item.Waypoints = item.Waypoints.copy()  # Needed to make a separate copy of the list of Waypoints.
    # Note the waypoints themselves still link to the same memory space. copy.deepcopy() would solve this, if this was an issue.

    if instruction == 'LastMinusNeeds_subID':
        new_item.QTY = data['qty']
        new_item.name = new_item.name + '.' + sub_ID
        return new_item
    
    # Not needed condition: elif instruction == 'standard':
    if isinstance(data['plus_mvt'], str):
        if data['plus_mvt'] == 'BURNT':
            new_item.Open = False
            new_wpt = data['minus_mvt'][_WAYPOINTS_DEF_].tolist()
            new_wpt[2] = f"BURNT {data['minus_mvt']['SLOC']}"
        elif data['plus_mvt'] == 'PO2ndPartMissing':
            if data['minus_mvt']['SLOC'][:7] == 'PO FROM':  # Don't add a waypoint if we haven't found 2nd part of the PO for 2+ times in a row
                return new_item
            new_item.Open = np.nan
            new_wpt = data['minus_mvt'][_WAYPOINTS_DEF_].tolist()
            new_wpt[2] = f"PO FROM {data['minus_mvt']['SLOC']}, mvt {data['minus_mvt']['Mvt Code']}"
            new_wpt[4] = data['minus_mvt']['PO']
        else:
            raise Exception('Unexpected [+] mvt resolution type.')
    else:
        new_item.Open = True

        if len(new_item.Waypoints) == 1:
            new_item.Waypoints[0][0] = pd.Timestamp(0)
            new_item.Waypoints[0][4] = ''

        new_wpt = data['plus_mvt'][_WAYPOINTS_DEF_].tolist()
        if new_wpt[2] != 'NA': # Remove SoldTo if the SLOC isn't a Consignment
            new_wpt[3] = np.nan
        
        if data['minus_mvt']['Mvt Code'] != new_wpt[4]: # Combination of codes
            new_wpt[4] = data['minus_mvt']['Mvt Code'] + '/' + new_wpt[4]

    new_item.Waypoints.append(new_wpt)

    new_item.QTY = data['qty']
    if sub_ID:
        new_item.name = new_item.name + '.' + sub_ID

    return new_item


# @profiler
def find_minus_lines(this_is_first_step: bool, latest_wpt: list, MVT_DB: pd.DataFrame, ID: str) -> pd.DataFrame:
    if not this_is_first_step:
        if latest_wpt[2] == 'NA': # Add filter on SoldTo if SKU is in consignment
            minus1_line = MVT_DB.loc[
                (MVT_DB['Posting Date'].values >= latest_wpt[0]) & \
                (MVT_DB['Company_SLOC_Batch'].values == latest_wpt[1] + '-' + latest_wpt[2] + '-' + latest_wpt[5]) & \
                (MVT_DB['Sold to'].values == latest_wpt[3]) & \
                (MVT_DB['Items_Allocated'].apply(lambda Item_Allocated: ID not in Item_Allocated)) & \
                (MVT_DB['QTY'].values <= -1) & \
                (MVT_DB['QTY_Unallocated'].values >= 1)
            ]
        else:
            minus1_line = MVT_DB.loc[
                (MVT_DB['Posting Date'].values >= latest_wpt[0]) & \
                (MVT_DB['Company_SLOC_Batch'].values == latest_wpt[1] + '-' + latest_wpt[2] + '-' + latest_wpt[5]) & \
                (MVT_DB['Items_Allocated'].apply(lambda Item_Allocated: ID not in Item_Allocated)) & \
                (MVT_DB['QTY'].values <= -1) & \
                (MVT_DB['QTY_Unallocated'].values >= 1)
            ]
    else: # We're looking for the 1st movement of the tracked product
        minus1_line = MVT_DB.loc[
            (MVT_DB['Posting Date'].values == latest_wpt[0]) & \
            (MVT_DB['Company_SLOC_Batch'].values == latest_wpt[1] + '-' + latest_wpt[2] + '-' + latest_wpt[5]) & \
            (MVT_DB['Sold to'].values == latest_wpt[3]) & \
            (MVT_DB['Mvt Code'].values == latest_wpt[4]) & \
            (MVT_DB['QTY'].values <= -1) & \
            (MVT_DB['QTY_Unallocated'].values >= 1)
        ]
    return minus1_line


# @profiler
def find_plus_lines(minus1_line: pd.Series, MVT_DB: pd.DataFrame) -> pd.DataFrame:  # We start with the exceptions, and the general case is down
    if minus1_line['Mvt Code'] == '956': # Change of SoldTo
        plus1_lines = MVT_DB.loc[
            (MVT_DB['Posting Date'].values == minus1_line['Posting Date']) & \
            (MVT_DB['Company'].values == minus1_line['Company']) & \
            (MVT_DB['Mvt Code'].values == '955') & \
            (MVT_DB['Batch'].values == minus1_line['Batch']) & \
            (MVT_DB['QTY'].values >= 1) & \
            (MVT_DB['QTY_Unallocated'].values >= 1)
        ]
    elif minus1_line['Mvt Code'] == '702': # Change of batch number. Product remains in same SLOC & SoldTo
        plus1_lines = MVT_DB.loc[
            (MVT_DB['Posting Date'].values == minus1_line['Posting Date']) & \
            (MVT_DB['Company'].values == minus1_line['Company']) & \
            (MVT_DB['SLOC'].values == minus1_line['SLOC']) & \
            (MVT_DB['Sold to'].values == minus1_line['Sold to']) & \
            (MVT_DB['Mvt Code'].values == '701') & \
            (MVT_DB['QTY'].values >= 1) & \
            (MVT_DB['QTY_Unallocated'].values >= 1)
        ]
    elif minus1_line['PO'] != '-2': # If it's a PO, we don't check with the mvt codes
        plus1_lines = MVT_DB.loc[
            (MVT_DB['Posting Date'].values >= minus1_line['Posting Date']) & \
            (MVT_DB['Batch'].values == minus1_line['Batch']) & \
            (MVT_DB['PO'].values == minus1_line['PO']) & \
            (MVT_DB['QTY'].values >= 1) & \
            (MVT_DB['QTY_Unallocated'].values >= 1)
        ]
    else:  # Standard mvt
        plus1_lines = MVT_DB.loc[
            (MVT_DB['Posting Date'].values == minus1_line['Posting Date']) & \
            (MVT_DB['Company'].values == minus1_line['Company']) & \
            (MVT_DB['Sold to'].values == minus1_line['Sold to']) & \
            (MVT_DB['Batch'].values == minus1_line['Batch']) & \
            (MVT_DB['Mvt Code'].values == minus1_line['Mvt Code']) & \
            (MVT_DB['Document'].values == minus1_line['Document']) & \
            (MVT_DB['QTY'].values >= 1) & \
            (MVT_DB['QTY_Unallocated'].values >= 1)
        ]
    
    return plus1_lines


# @profiler
def find_plus_lines_nobatch(minus1_line: pd.Series, MVT_DB: pd.DataFrame) -> pd.DataFrame:
    plus1_lines = MVT_DB.loc[
            (MVT_DB['Posting Date'].values == minus1_line['Posting Date']) & \
            (MVT_DB['Company'].values == minus1_line['Company']) & \
            (MVT_DB['Sold to'].values == minus1_line['Sold to']) & \
            (MVT_DB['Mvt Code'].values == minus1_line['Mvt Code']) & \
            (MVT_DB['Document'].values == minus1_line['Document']) & \
            (MVT_DB['QTY'].values >= 1) & \
            (MVT_DB['QTY_Unallocated'].values >= 1)
        ]
    return plus1_lines