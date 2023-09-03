""" postprocessing_tk.py
Ready-made postprocessing functions.

Functions:
    make_exportable_hist
"""


import numpy as np
import pandas as pd


def make_exportable_hist(tracked_Items: pd.DataFrame) -> pd.DataFrame:
    tobe_rtn = (tracked_Items
                .explode('Waypoints')
                .reset_index(names='ID')
                .assign(WaypointNo = lambda df_: 1+df_.groupby('ID').cumcount(),
                        Landing_Date = lambda df_: df_['Waypoints'].apply(lambda row: row[0]),
                        Landing_Code = lambda df_: df_['Waypoints'].apply(lambda row: row[3]),
                        SLOC = lambda df_: df_['Waypoints'].apply(lambda row: row[1]),
                        Soldto = lambda df_: df_['Waypoints'].apply(lambda row: row[2]),
                        Batch_ = lambda df_: df_['Waypoints'].apply(lambda row: row[4]),
                        Depart_Date = lambda df_: df_.groupby('ID')['Landing_Date'].shift(-1))
                .drop(columns=['Waypoints'])
               )
    tobe_rtn['Landing_Date'] = tobe_rtn.apply(lambda row: np.nan if row['WaypointNo']==1 else row['Landing_Date'], axis=1)
    tobe_rtn['Landing_Code'] = tobe_rtn.apply(lambda row: np.nan if row['WaypointNo']==1 else row['Landing_Code'], axis=1)
    
    return tobe_rtn

