""" postprocessing.py
Defines user-defined instructions for postprocessing.

Functions:
    postprocess
"""


from datetime import datetime
from itertools import groupby
import numpy as np
import pandas as pd



def postprocess(self, tracked_items: pd.DataFrame) -> bool:
    TI = tracked_items.copy()
    TI['Route'] = TI['Waypoints'].apply(lambda wpts: ' > '.join(list(map('.'.join, np.array(wpts)[:, 1:3]))))
    TI['DCs'] = TI['Waypoints'].apply(lambda wpts: [i[0] for i in groupby(np.array(wpts)[:,1])])

    TI['Return_Month'] = TI['Return_Date'].dt.strftime('%Y/%m').astype(str)

    TI['Last_Company'] = TI['Waypoints'].apply(lambda wpts: wpts[-1][1])
    TI['Last_SLOC'] = TI['Waypoints'].apply(lambda wpts: wpts[-1][2])
    TI['Last_Mvt'] = TI['Waypoints'].apply(lambda wpts: wpts[-1][4])

    TI['Num_Steps'] = TI['Waypoints'].apply(len) -1
    TI['Num_DCs'] = TI['DCs'].apply(lambda DCs: len(DCs))
    
    Max_date = max(TI['Waypoints'].apply(lambda wpts: np.array(wpts)[-1,0]))
    TI['Num_Days_Open'] = np.where(
        TI['Open'].fillna(False),
        Max_date - TI['Return_Date'],
        TI['Waypoints'].apply(lambda wpts: wpts[-1][0]) - TI['Return_Date']
    )

    TI['Num_Returns'] = TI['Waypoints'].apply(lambda wpts: np.count_nonzero(np.isin(np.array(wpts).flatten(), ['632', '932', '956/955'])))
    
    # Data formating
    TI['Return_Date'] = TI['Return_Date'].dt.strftime('%Y-%m-%d')
    TI['DCs'] = TI['DCs'].apply(lambda DCs: ' > '.join(DCs))

    def decorate_wpts(wpts):
        return '  >>>  '.join(list(map(lambda x: ', '.join(map(str, [x[0].strftime('%Y-%m-%d'), *x[1:]])), wpts)))
    TI['Waypoints'] = TI['Waypoints'].apply(lambda wpts: decorate_wpts(wpts))
    

    # Make a detailed view of the route
    from product_trailer.postprocessing_tk import make_exportable_hist
    detailed_view = make_exportable_hist(tracked_items)


    # Saving to Excel
    date_range = TI['Return_Date'].min()+ ".." + TI['Return_Date'].max()
    dt_now = datetime.today().strftime("%Y-%m-%d %Hh%M")
    out_filename = f'Returns -- Saved {dt_now} -- Range {date_range}.xlsx'
    self.report_to_excel({'summary': TI, 'details': detailed_view}, out_filename)

    return True

