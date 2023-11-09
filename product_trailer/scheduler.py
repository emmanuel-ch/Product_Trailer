""" scheduler.py
Administration of computational tasks.

Class Scheduler - methods:
    .__init__
    .prepare
    .run
    ._prep_item
    ._prep_mvt
    ._extract_items
"""


import pandas as pd
import tqdm

from product_trailer.forwardtracker import ForwardTracker
from product_trailer.item import Item


class Scheduler:
    DEF_WPT = ['Posting Date','Company','SLOC','Sold to','Mvt Code','Batch']
    
    def __init__(self, profile):
        self.profile = profile
    
    def prepare(self, new_raw_data):
        items, num_retrieved = self._prep_item(new_raw_data)
        self.items_todo = items.loc[items['Open'].fillna(True)].copy()
        self.items_done = [items.loc[~items['Open'].fillna(True)].copy()]
        
        self._make_todo_dict()

        self.tasklist = self._make_tasklist()
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
        for task in (pbar := tqdm.tqdm(self.tasklist, desc='Crunching...')):
            pbar.set_postfix({'Object': task}, refresh=False)
            add_items, add_mvts = (
                ForwardTracker(
                    Scheduler.DEF_WPT,
                    self.mvts.loc[(self.mvts['SKU'] == task)]
                )
                .do_task(self.items_todo.loc[(self.items_todo['SKU'] == task)])
            )
            self.items_done.append(add_items)
            if self.profile.db_config['save_movements']:
                self.mvts_done.append(add_mvts)
        
        all_items = pd.concat(self.items_done, axis=0)
        return all_items, self.mvts_done
    

    #
    # NON-USER INTERFACE METHODS
    #

    def _prep_item(self, new_raw_data: pd.DataFrame) -> pd.DataFrame:
        new_tracked_items = self._extract_items(new_raw_data)
        saved_items = self.profile.fetch_items()
        if isinstance(saved_items, pd.DataFrame):
            tracked_items = pd.concat([saved_items, new_tracked_items])
            return tracked_items, saved_items.shape[0]
        return new_tracked_items, 0
    
    def _make_todo_dict(self):
        self.todo_dict = {}
        self.items_todo.groupby('SKU', observed=True).apply(self._todo_add)
    
    def _todo_add(self, df):
        items = [Item(*row) for row in df.reset_index().to_numpy()]
        self.todo_dict[df.name] = items

    def _make_tasklist(self):
        return (
            self.items_todo
            .value_counts(['SKU'])
            .loc[self.items_todo.value_counts(['SKU']).gt(0)]
            .reset_index()['SKU']
            .to_list()
        )
    
    def _prep_mvt(self, new_raw_mvt: pd.DataFrame) -> pd.DataFrame:
        return (
            new_raw_mvt.loc[new_raw_mvt['SKU'].isin(self.tasklist)]
            .copy()
            .drop(
                columns=[
                    *self.profile.input['company_features'],
                    *self.profile.input['sku_features'],
                    'Special Stock Ind Code',
                    'Unit_Value',
                ]
            )
            .assign(
                QTY_Unallocated=lambda df: df['QTY'].apply(abs),
                Items_Allocated=lambda df: df.apply(
                    lambda _: set(), result_type='reduce', axis=1
                ),
                Company_SLOC_Batch=lambda df: df[['Company', 'SLOC', 'Batch']].apply(
                    lambda row: '-'.join(row), axis=1
                ),
            )
        )

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
        company_features = ['Company', *self.profile.input['company_features']]
        sku_features = ['SKU', *self.profile.input['sku_features']]
        def build_ID(item):
            return (
                f"_{item['Company']}/{item['SLOC']}/{item['Sold to'][4:11]}_"
                + f"{item['Mvt Code']}/{item['Posting Date']:%Y-%m-%d}_"
                + f"{item['SKU']}:{item['Batch']}"
            )
        
        trailed_products = (
            raw_mvt.copy()
            .pipe(lambda df: df.loc[self.profile.is_entry_point(df)])
            .pivot_table(
                observed=True,
                values=['Unit_Value', 'QTY'],
                aggfunc={'Unit_Value': 'mean','QTY': 'sum'},
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
                    lambda row: [list(row.loc[Scheduler.DEF_WPT].values)],
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
