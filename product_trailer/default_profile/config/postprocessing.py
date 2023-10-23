""" postprocessing.py
Defines user-defined instructions for postprocessing.

Functions:
    postprocess
    customize_std_report
"""


from datetime import datetime

import numpy as np
import pandas as pd

import product_trailer.postprocessing_tk as pp_tk


def postprocess(self, tracked_items: pd.DataFrame) -> bool:
    # Make the standard report
    std_report = pp_tk.make_standard_report(tracked_items)
    std_report = customize_std_report(std_report)  # Customization by user
    detailed_view = pp_tk.make_exportable_hist(tracked_items)

    stock_move = pp_tk.collect_stock_move(
        tracked_items[['QTY', 'Waypoints']],
        'company'
        )
    fig = pp_tk.generate_stock_move_diagram(stock_move)


    # Saving
    date_range = (std_report['Return_Date'].min() 
                  + ".." + std_report['Return_Date'].max())
    dt_now = datetime.today().strftime("%Y-%m-%d %Hh%M")
    fsuffix = f'-- Saved {dt_now} -- Range {date_range}'

    self.report_to_excel(
        {'summary': std_report, 'details': detailed_view},
        f'Tracked products' + fsuffix + '.xlsx'
        )
    
    self.save_figure(fig, f'Network diagram' + fsuffix + '.png')


def customize_std_report(tracked_Items: pd.DataFrame) -> pd.DataFrame:
    TI = tracked_Items.copy(deep=True)
    TI['Num_Returns_Cngmt'] = TI['Waypoints'].apply(
        lambda wpts: np.count_nonzero(np.isin(
            np.array(wpts).flatten(),
            ['632', '932', '956/955']
            )))
    return TI
    
