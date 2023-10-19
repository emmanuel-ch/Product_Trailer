""" postprocessing.py
Defines user-defined instructions for postprocessing.

Functions:
    postprocess
"""


from datetime import datetime
import numpy as np
import pandas as pd

from product_trailer import postprocessing_tk



def postprocess(self, tracked_items: pd.DataFrame) -> bool:

    # Make the standard report
    std_report = postprocessing_tk.make_standard_report(tracked_items)
    std_report = customize_std_report(std_report)  # Customization by user

    # Make a detailed view of the route (1 line per tracked item)
    detailed_view = postprocessing_tk.make_exportable_hist(tracked_items)

    # Save to Excel
    date_range = (
        std_report['Return_Date'].min() 
        + ".."
        + std_report['Return_Date'].max()
    )
    dt_now = datetime.today().strftime("%Y-%m-%d %Hh%M")
    out_filename = (
        f'Tracked products'
        + f'-- Saved {dt_now}'
        + f'-- Range {date_range}.xlsx'
    )
    self.report_to_excel(
        {'summary': std_report, 'details': detailed_view},
        out_filename
        )
    
    # Generate network diagram
    stock_move = postprocessing_tk.collect_stock_move(
        tracked_items[['QTY', 'Waypoints']],
        'company'
        )
    out_filename = (
        f'Network mapping'
        + f'-- Saved {dt_now}'
        + f'-- Range {date_range}.png'
    )
    postprocessing_tk.generate_stock_move_map(
        stock_move,
        out_filename
        )


def customize_std_report(tracked_Items: pd.DataFrame) -> pd.DataFrame:
    TI = tracked_Items.copy(deep=True)
    TI['Num_Returns'] = TI['Waypoints'].apply(
        lambda wpts: np.count_nonzero(np.isin(
            np.array(wpts).flatten(),
            ['632', '932', '956/955']
            )))
    return TI
    
