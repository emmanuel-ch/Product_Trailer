import os
import numpy as np
import pandas as pd
from datetime import datetime
from itertools import groupby
from .standards import *


def post_process(tracked_Items_analysis):
    TI = tracked_Items_analysis.copy()
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
    
    # Data formating
    TI['Return_Date'] = TI['Return_Date'].dt.strftime('%Y-%m-%d')
    TI['DCs'] = TI['DCs'].apply(lambda DCs: ' > '.join(DCs))

    def decorate_wpts(wpts):
        return '  >>>  '.join(list(map(lambda x: ', '.join(map(str, [x[0].strftime('%Y-%m-%d'), *x[1:]])), wpts)))
    TI['Waypoints'] = TI['Waypoints'].apply(lambda wpts: decorate_wpts(wpts))

    return TI


def save_report(tracked_Items_analysis, dest, tab_with_waypoints=False):
    date_range = tracked_Items_analysis['Return_Date'].min()+ ".." + tracked_Items_analysis['Return_Date'].max()
    dt_now = datetime.today().strftime("%Y-%m-%d %Hh%M")
    outp_filename = os.path.join(dest, f'Returns -- Saved {dt_now} -- Returns range {date_range}.xlsx')
    
    with pd.ExcelWriter(outp_filename) as writer:
        sheetname = 'All returns'
        tracked_Items_analysis.to_excel(writer, sheet_name=sheetname, index=True, freeze_panes=(1,0))
        
        if tab_with_waypoints:
            sheetname = 'All returns - Waypoints'
            make_exportable_hist(tracked_Items_analysis) \
                .to_excel(writer, sheet_name=sheetname, index=True, freeze_panes=(1,0))
    
    return outp_filename


def make_exportable_hist(tracked_Items_analysis):
    tobe_rtn = (tracked_Items_analysis \
                .explode('Waypoints') \
                .assign(WaypointNo = lambda df_: 1+df_.groupby('ID').cumcount(), \
                        Landing_Date = lambda df_: df_['Waypoints'].apply(lambda row: row[0]), \
                        Landing_Code = lambda df_: df_['Waypoints'].apply(lambda row: row[3]), \
                        SLOC = lambda df_: df_['Waypoints'].apply(lambda row: row[1]), \
                        Soldto = lambda df_: df_['Waypoints'].apply(lambda row: row[2]), \
                        Batch_ = lambda df_: df_['Waypoints'].apply(lambda row: row[4]), \
                        Depart_Date = lambda df_: df_.groupby('ID')['Landing_Date'].shift(-1))
                .drop(columns=['Waypoints'])
               )
    tobe_rtn['Landing_Date'] = tobe_rtn.apply(lambda row: np.nan if row['WaypointNo']==1 else row['Landing_Date'], axis=1)
    tobe_rtn['Landing_Code'] = tobe_rtn.apply(lambda row: np.nan if row['WaypointNo']==1 else row['Landing_Code'], axis=1)
    
    return tobe_rtn
