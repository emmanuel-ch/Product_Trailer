""" basetracker.py
Base class for all tracking mechanisms

Class BaseTracker - methods:
    .__init__
    .prepare
    .run
    ._extract_items
    ._do_task
    ._make_route
"""


import numpy as np
import pandas as pd
import tqdm


class BaseTracker:
    WAYPOINT_DEF = ['Posting Date', 'Company', 'SLOC', 'Sold to', 'Mvt Code', 'Batch']
    
    def __init__(self, config):
        self.config = config
    

    def prepare(self, new_raw_data):
        items, num_retrieved = self._prep_item(new_raw_data)
        self.items_todo = items.loc[items['Open'].fillna(True)].copy()
        self.items_done = [items.loc[~items['Open'].fillna(True)].copy()]

        self.tasklist = (
            self.items_todo
            .value_counts(['SKU'])
            .loc[self.items_todo.value_counts(['SKU']).gt(0)]
            .reset_index()['SKU']
            .to_list()
        )

        self.mvts = self._prep_mvt(new_raw_data)
        self.mvts_done = []

        return {
            'items': (
                '%s items incl. %s retrieved. %s to do, %s closed.'
                % (
                    len(items),
                    num_retrieved,
                    len(self.items_todo),
                    len(self.items_done)
                )
            ),
            'mvts': (
                'total %s mvts utilized of total %s'
                % (self.mvts.shape[0], new_raw_data.shape[0])
            ),
        }

    
    def run(self):
        for task in (pbar := tqdm.tqdm(self.tasklist, desc='Crunching... ')):
            pbar.set_postfix({'Object': task}, refresh=False)
            add_items, add_mvts = self._do_task(
                self.items_todo.loc[(self.items_todo['SKU'] == task)],
                self.mvts.loc[(self.mvts['SKU'] == task)]
            )
            self.items_done.append(add_items)
            if self.config.db_config['save_movements']:
                self.mvts_done.append(add_mvts)
        
        all_items = pd.concat(self.items_done, axis=0)
        return all_items, self.mvts_done
    

    #
    # NON-USER INTERFACE METHODS
    #
    def _extract_items(self, raw_mvt: pd.DataFrame) -> pd.DataFrame:
        ID_definition = [
            'Company',
            'SLOC',
            'Sold to',
            'Mvt Code',
            'Posting Date',
            'SKU',
            'Batch'
        ]
        company_features = ['Company', *self.config.input_features['company_features']]
        sku_features = ['SKU', *self.config.input_features['sku_features']]

        def build_ID(item):
            return (
                f"_{item['Company']}/{item['SLOC']}/{item['Sold to'][4:11]}_"
                + f"{item['Mvt Code']}/{item['Posting Date']:%Y-%m-%d}_"
                + f"{item['SKU']}:{item['Batch']}"
            )
        
        trailed_products = (
            raw_mvt.copy()
            .pipe(lambda df: df.loc[self.config.is_entry_point(df)])
            .pivot_table(
                observed=True,
                values=['Unit_Value', 'QTY'],
                aggfunc={'Unit_Value': 'mean', 'QTY': 'sum'},
                index=ID_definition
            )
            .reset_index()
            .assign(ID = lambda df: df.apply(build_ID, axis=1))
            .merge(
                raw_mvt
                .value_counts(company_features)
                .reset_index()[company_features]
                .drop_duplicates(keep='first'), on='Company')
            .merge(
                raw_mvt
                .value_counts(sku_features)
                .reset_index()[sku_features]
                .drop_duplicates(keep='first'), on='SKU')
            .set_index('ID')
            .assign(
                Open = True,
                QTY = lambda df: -df['QTY'],
                Waypoints = lambda df: df.apply(
                    lambda row: [list(row.loc[BaseTracker.WAYPOINT_DEF].values)],
                    axis=1
                    )
            )
            .rename(columns={'Country': 'First_Country'})
            [
                [
                    'First_Country',
                    'SKU',
                    'QTY',
                    'Open',
                    'Waypoints',
                    'Unit_Value',
                    'Brand',
                    'Category'
                ]
            ]
        )
        return trailed_products

    def _do_task(
            self, task_items: pd.DataFrame, task_MVTs: pd.DataFrame
        ) -> (pd.DataFrame, pd.DataFrame):
        if len(task_MVTs) == 0:  # No mvt => Skip this
            return task_items, task_MVTs
        items_computed = []  # list of pd.Series
        for _, row in task_items.iterrows():
            items_computed.extend(self._make_route(row, task_MVTs))
        df_items_computed = pd.DataFrame(items_computed)
        
        return df_items_computed, task_MVTs

    def _make_route(self, item: pd.Series, mvts: pd.DataFrame) -> list:
        new_items = self._make_hop(item, mvts)
        if len(new_items) == 0:
            return []
        elif len(new_items) == 1:
            if item.equals(new_items[0]):  # Product didn't travel further
                return [item]
        return [
            an_item
            for new_item in new_items
            for an_item in self._make_route(new_item, mvts)
        ]
