""" forwardtracker.py
Forward tracking mechanism

Class ForwardTracker - methods:
    .__init__
    .do_task
    ._make_route
    ._make_hop
    ._compute_incr
    ._build_item
    ._find_decr
    ._find_incr
"""

from copy import deepcopy

import numpy as np
import pandas as pd

from product_trailer.item import Item


class ForwardTracker():
    def __init__(
        self,
        defwpt: list[str],
        task_mvts: pd.DataFrame,
        save_mvts: bool
    ) -> None:
        self.mvts = task_mvts
        self.defwpt = defwpt
        self.save_mvts = save_mvts
    
    
    def do_task(self, task_items: list[Item]) -> (list[Item], pd.DataFrame):
        if len(self.mvts) == 0:  # No mvt => Skip this
            return task_items, self.mvts
        
        items_computed = []
        for item in task_items:
            items_computed.extend(self._make_route(item))
        if self.save_mvts:
            return items_computed, self.mvts
        return items_computed, None

    
    def _make_route(self, item: Item) -> list[Item]:
        new_items = self._make_hop(item)
        if len(new_items) == 0:
            return []
        elif len(new_items) == 1:
            if item == new_items[0]:  # Product didn't travel further
                return [item]
        return [
            an_item
            for new_item in new_items
            for an_item in self._make_route(new_item)
        ]
    
    
    def _make_hop(self, item: Item) -> list:
        first_step = len(item.waypoints) == 1

        if not np.isnan(item.open):
            minus_mvts = self._find_decr(
                first_step, item.waypoints[-1],
                item.id
                )
        else:
            minus_mvts = pd.DataFrame(
                {
                    'Posting Date': [item.waypoints[-1][0]],
                    'Batch': [item.waypoints[-1][5]],
                    'PO': [item.waypoints[-1][4]],
                    'Mvt Code': ['PO'],
                    'Company': [item.waypoints[-1][1]], 
                    'SLOC': [item.waypoints[-1][2]], 
                    'Sold to': [item.waypoints[-1][3]],
                    'QTY': [-item.qty],
                    'QTY_Unallocated': [item.qty],
                    'Items_Allocated': [set()]
                }
            )

        if len(minus_mvts) == 0:  # Nothing found: The product didn't move
            if first_step:
                # DOUBLE-COUNTING PREVENTION
                # If we are here, it means the tracked product passes by an 
                # "entry point". To avoid double-counting, decision was made to 
                # keep tracking the already tracked product, and do not register 
                # the new entry.
                return []
            return [item]
        
        new_items = []
        multiple_minuses = -minus_mvts.iloc[0]['QTY'] < item.qty
        sub_ID_lv1 = 0
        QTY_covered = 0

        for minus_idx, minus_mvt in minus_mvts.iterrows():
            # Positive int:
            hop_minus_QTY = min(item.qty - QTY_covered, -minus_mvt['QTY'])
            plus_resolved = self._compute_incr(
                minus_mvt, hop_minus_QTY, item.id
            )

            for sub_ID_2, this_plus_resolved in enumerate(plus_resolved):
                if multiple_minuses:
                    sub_ID = str(sub_ID_lv1) + (
                        '.' + str(sub_ID_2) if (len(plus_resolved) > 1) else ''
                    )
                else:
                    sub_ID = str(sub_ID_2) if (len(plus_resolved) > 1) else False

                new_item = self._build_item(
                    item,
                    instruction = 'standard',
                    data = {
                        'minus_mvt': minus_mvt,
                        'qty': this_plus_resolved['qty'],
                        'plus_mvt': this_plus_resolved['plus_mvt']
                    },
                    sub_ID=sub_ID
                )
                new_items.append(new_item)

            if minus_mvt['Mvt Code'] != 'PO':
                self.mvts.loc[minus_idx, 'QTY_Unallocated'] -= hop_minus_QTY
                self.mvts.loc[minus_idx, 'Items_Allocated'].add(item.id)
            
            
            QTY_covered += hop_minus_QTY
            if QTY_covered == item.qty:
                break
            elif QTY_covered > item.qty:
                raise Exception((f'Over-cover! Covered {QTY_covered} [-]',
                                f' for item qty {item.qty}'))
            sub_ID_lv1 += 1
        
        if QTY_covered < item.qty:
            new_items.append(
                self._build_item(
                    item,
                    instruction='LastMinusNeeds_subID',
                    data = {'qty': item.qty - QTY_covered},
                    sub_ID=str(sub_ID_lv1)
                )
            )
        return new_items

    
    def _build_item(
        self, item: Item, instruction: str, data: dict, sub_ID: bool | str
    ) -> Item:
        new_item = deepcopy(item)
        # new_item['Waypoints'] = item['Waypoints'].copy()  # Lists are mutable
        # Note the waypoints themselves still link to the same memory space.
        # copy.deepcopy() would solve this, if this was an issue.

        if instruction == 'LastMinusNeeds_subID':
            new_item.qty = data['qty']
            new_item.id += '.' + sub_ID
            return new_item
        
        if isinstance(data['plus_mvt'], str):  # instruction == 'standard'
            if data['plus_mvt'] == 'BURNT':
                new_item.open = False
                new_wpt = list(data['minus_mvt'][self.defwpt])
                new_wpt[2] = f"BURNT {data['minus_mvt']['SLOC']}"
            elif data['plus_mvt'] == 'PO2ndPartMissing':
                if data['minus_mvt']['SLOC'].startswith('PO FROM'):
                    # Don't add a waypoint if we haven't found 2nd part of 
                    # the PO for 2+ times in a row
                    return new_item
                new_item.open = np.nan
                new_wpt = list(data['minus_mvt'][self.defwpt])
                new_wpt[2] = 'PO FROM %s, mvt %s' % (
                    data['minus_mvt']['SLOC'],
                    data['minus_mvt']['Mvt Code']
                )
                new_wpt[4] = data['minus_mvt']['PO']
            else:
                raise Exception('Unexpected [+] mvt resolution type')
        else:
            if len(new_item.waypoints) == 1:
                new_item.waypoints[0][0] = pd.NaT
                new_item.waypoints[0][4] = ''
            
            new_item.open = True
            new_wpt = list(data['plus_mvt'].loc[self.defwpt])
            if new_wpt[2] != 'NA': # Remove SoldTo if SLOC isn't a Consignment
                new_wpt[3] = np.nan
            if data['minus_mvt']['Mvt Code'] != new_wpt[4]: # Combination
                new_wpt[4] = data['minus_mvt']['Mvt Code'] + '/' + new_wpt[4]

        new_item.waypoints.append(new_wpt)
        new_item.qty = data['qty']
        if sub_ID:
            new_item.id = new_item.id + '.' + sub_ID
        return new_item

    
    def _compute_incr(
        self,
        minus_mvt: pd.Series,
        desired_QTY: int,
        ID: str
    ) -> list:
        """Tried to find the [+] mvts: where the product has moved to.
        minus_mvt: Info about the [-] mvt
        desired_QTY: the quantity we track
        task_MVTs: A df containing movements"""
        plus_mvts = self._find_incr(minus_mvt)

        if len(plus_mvts) == 0:  # No 1st-pass result for a +1: we widen the search
            # Except if we were looking for 2nd half of PO
            if minus_mvt['PO'] != '-2':
                return [{'qty': desired_QTY, 'plus_mvt': 'PO2ndPartMissing'}]
            # Last chance to find a [+]: we remove the filter on batch#
            plus_mvts = self._find_incr(minus_mvt, True) 
            if len(plus_mvts) == 0:  # Part is burnt or is on a PO
                return [{'qty': desired_QTY, 'plus_mvt': 'BURNT'}]
        
        QTY_covered = 0
        plus_resolved = []
        for plus_idx, plus_mvt in plus_mvts.iterrows():
            addnl_cover_QTY = min(plus_mvt.QTY, desired_QTY-QTY_covered)
            plus_resolved.append({'qty': addnl_cover_QTY, 'plus_mvt': plus_mvt})
            self.mvts.loc[plus_idx, 'QTY_Unallocated'] -= addnl_cover_QTY
            self.mvts.loc[plus_idx, 'Items_Allocated'].add(ID)
            QTY_covered += addnl_cover_QTY
            if QTY_covered >= desired_QTY:
                break
        
        if QTY_covered < desired_QTY:
            # Ex: found 4 [+] for 5 [-]: assume the last unit was burnt
            plus_resolved.append(
                {'qty': desired_QTY-QTY_covered, 'plus_mvt': 'BURNT'}
            )
        
        return plus_resolved

    
    def _find_decr(
        self, first_step: bool, wpt: list, ID: str
    ) -> pd.DataFrame:
        if not first_step:
            if wpt[2] == 'NA':  # Add filter on SoldTo if SKU in consignment
                decrement = self.mvts.loc[
                    (self.mvts['Posting Date'].values >= wpt[0])
                    & (self.mvts['Company_SLOC_Batch'].values == (
                        wpt[1] + '-' + wpt[2] + '-' + wpt[5]
                    ))
                    & (self.mvts['Sold to'].values == wpt[3])
                    & (self.mvts['Items_Allocated'].apply(
                        lambda Item_Allocated: ID not in Item_Allocated
                        ))
                    & (self.mvts['QTY'].values <= -1)
                    & (self.mvts['QTY_Unallocated'].values >= 1)
                ]
            else:
                decrement = self.mvts.loc[
                    (self.mvts['Posting Date'].values >= wpt[0])
                    & (self.mvts['Company_SLOC_Batch'].values == (
                        wpt[1] + '-' + wpt[2] + '-' + wpt[5]
                    ))
                    & (self.mvts['Items_Allocated'].apply(
                        lambda Item_Allocated: ID not in Item_Allocated
                        ))
                    & (self.mvts['QTY'].values <= -1)
                    & (self.mvts['QTY_Unallocated'].values >= 1)
                ]
        else:  # We're looking for the 1st movement of the tracked product
            decrement = self.mvts.loc[
                (self.mvts['Posting Date'].values == wpt[0])
                & (self.mvts['Company_SLOC_Batch'].values == (
                    wpt[1] + '-' + wpt[2] + '-' + wpt[5]
                    ))
                & (self.mvts['Sold to'].values == wpt[3])
                & (self.mvts['Mvt Code'].values == wpt[4])
                & (self.mvts['QTY'].values <= -1)
                & (self.mvts['QTY_Unallocated'].values >= 1)
            ]
        return decrement
    
    
    def _find_incr(
        self, decr: pd.Series, nobatch: bool = False
    ) -> pd.DataFrame:
        # Exceptions on top, general case is down
        if decr['Mvt Code'] == '956':  # Change of SoldTo
            return self.mvts.loc[
                (self.mvts['Posting Date'].values == decr['Posting Date'])
                & (self.mvts['Company'].values == decr['Company'])
                & (self.mvts['Mvt Code'].values == '955')
                & (self.mvts['Batch'].values == decr['Batch'])
                & (self.mvts['QTY'].values >= 1)
                & (self.mvts['QTY_Unallocated'].values >= 1)
            ]
        elif decr['Mvt Code'] == '702':  # Change of batch number
            return self.mvts.loc[
                (self.mvts['Posting Date'].values == decr['Posting Date'])
                & (self.mvts['Company'].values == decr['Company'])
                & (self.mvts['SLOC'].values == decr['SLOC'])
                & (self.mvts['Sold to'].values == decr['Sold to'])
                & (self.mvts['Mvt Code'].values == '701')
                & (self.mvts['QTY'].values >= 1)
                & (self.mvts['QTY_Unallocated'].values >= 1)
            ]
        elif decr['PO'] != '-2':  # PO: PO number checked but not mvt code
            return self.mvts.loc[
                (self.mvts['Posting Date'].values >= decr['Posting Date'])
                & (self.mvts['Batch'].values == decr['Batch'])
                & (self.mvts['PO'].values == decr['PO'])
                & (self.mvts['QTY'].values >= 1)
                & (self.mvts['QTY_Unallocated'].values >= 1)
            ]
        elif nobatch:  # Loosen search if can't find [+] mvt
            return self.mvts.loc[
                (self.mvts['Posting Date'].values == decr['Posting Date'])
                & (self.mvts['Company'].values == decr['Company'])
                & (self.mvts['Sold to'].values == decr['Sold to'])
                & (self.mvts['Mvt Code'].values == decr['Mvt Code'])
                & (self.mvts['Document'].values == decr['Document'])
                & (self.mvts['QTY'].values >= 1)
                & (self.mvts['QTY_Unallocated'].values >= 1)
            ]
        else:  # Standard mvt
            return self.mvts.loc[
                (self.mvts['Posting Date'].values == decr['Posting Date'])
                & (self.mvts['Company'].values == decr['Company'])
                & (self.mvts['Sold to'].values == decr['Sold to'])
                & (self.mvts['Batch'].values == decr['Batch'])
                & (self.mvts['Mvt Code'].values == decr['Mvt Code'])
                & (self.mvts['Document'].values == decr['Document'])
                & (self.mvts['QTY'].values >= 1)
                & (self.mvts['QTY_Unallocated'].values >= 1)
            ]
