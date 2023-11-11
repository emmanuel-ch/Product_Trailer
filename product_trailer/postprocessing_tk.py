""" postprocessing_tk.py
Ready-made postprocessing functions.

Functions:
    make_standard_report
    make_exportable_hist
    collect_stock_move
    generate_stock_move_diagram
"""


from itertools import groupby
import functools
import numpy as np
import pandas as pd
import networkx as nx
import matplotlib
import matplotlib.pyplot as plt



def make_standard_report(tracked_items: pd.DataFrame) -> pd.DataFrame:
    def make_features(item):
        wpts = item['waypoints']
        list_companies = [i[0] for i in groupby(np.array(wpts)[:,1])]
        return {
            'Route': ' > '.join(list(map('.'.join, np.array(wpts)[:, 1:3]))),
            'DCs': list_companies,
            'Return_Date': wpts[1][0],
            'Return_Month': wpts[1][0].strftime('%Y/%m'),
            'Company(first)': wpts[0][1],
            'SLOC(first)': wpts[0][2],
            'SoldTo(first)': wpts[0][3],
            'Company(last)': wpts[-1][1],
            'SLOC(last)': wpts[-1][2],
            'SoldTo(last)': wpts[-1][3],
            'Num_Steps': len(wpts),
            'Num_Companies': len(list_companies)
        }
    new_cols = tracked_items[['waypoints']].apply(make_features,
                                                  axis=1, result_type='expand')
    ti = pd.concat([tracked_items, new_cols], axis='columns')

    item_max_date = max(ti['waypoints'].apply(lambda wpts: np.array(wpts)[-1,0]))
    ti['Num_Days_Open'] = np.where(
        ti['open'].fillna(False),
        item_max_date - ti['Return_Date'],
        ti['waypoints'].apply(lambda wpts: wpts[-1][0]) - ti['Return_Date']
    )

    # Formating
    ti['Return_Date'] = ti['Return_Date'].dt.strftime('%Y-%m-%d')
    ti['DCs'] = ti['DCs'].apply(lambda DCs: ' > '.join(DCs))
    def decorate_wpts(wpts):
        return '  >>>  '.join(
            [', '.join(map(str, ['-', *wpts[0][1:]]))]
            + list(map(
                lambda x: ', '.join(map(str, [x[0].strftime('%Y-%m-%d'), *x[1:]])),
                wpts[1:]
                ))
            )
    ti['waypoints'] = ti['waypoints'].apply(lambda wpts: decorate_wpts(wpts))
    return ti.reset_index()


def make_exportable_hist(tracked_Items: pd.DataFrame) -> pd.DataFrame:
    tobe_rtn = (
        tracked_Items
        .explode('waypoints')
        .reset_index(names='id')
        .assign(WaypointNo = lambda df_: 1+df_.groupby('id').cumcount(),
                Landing_Date = lambda df_: df_['waypoints'].apply(lambda row: row[0]),
                Landing_Code = lambda df_: df_['waypoints'].apply(lambda row: row[3]),
                SLOC = lambda df_: df_['waypoints'].apply(lambda row: row[1]),
                Soldto = lambda df_: df_['waypoints'].apply(lambda row: row[2]),
                Batch_ = lambda df_: df_['waypoints'].apply(lambda row: row[4]),
                Depart_Date = lambda df_: df_.groupby('id')['Landing_Date'].shift(-1))
        .drop(columns=['waypoints'])
    )
    tobe_rtn['Landing_Date'] = tobe_rtn.apply(
        lambda row: np.nan if row['WaypointNo']==1 else row['Landing_Date'],
        axis=1
        )
    tobe_rtn['Landing_Code'] = tobe_rtn.apply(
        lambda row: np.nan if row['WaypointNo']==1 else row['Landing_Code'],
        axis=1
        )
    
    return tobe_rtn


def collect_stock_move(df: pd.DataFrame, node_level: str) -> dict:
    stock_move = dict()
    def node_name(wpt, node_level):
            if node_level == 'company':
                return wpt[1]
    node_name = functools.partial(node_name, node_level=node_level)

    def assign_move(series):
        qty = series['qty']
        wpts = series['waypoints']
        
        for i, wpt in enumerate(wpts):
            if i == 0:
                continue
            node_from, node_to = node_name(wpts[i-1]), node_name(wpt)
            if node_from != node_to:
                move_name = node_from + '-' + node_to
                if move_name in stock_move.keys():
                    stock_move[move_name][2] += qty
                else:
                    stock_move[move_name] = [node_from, node_to, qty]
    
    df.apply(assign_move, axis=1)
    return stock_move


def generate_stock_move_diagram(stock_move: dict,
                            max_edge_width: int=4) -> plt.figure:
    G = nx.DiGraph()
    edges =  [item for item in stock_move.values()]
    G.add_weighted_edges_from(edges)
    
    # Edges width normalization
    edges_widths = [item[2] for item in edges]
    edges_widths = [max(1, int(max_edge_width*item/max(edges_widths)))
                    for item in edges_widths]

    # Colours to nodes
    colourmap = matplotlib.colormaps['tab20']
    normalize = matplotlib.colors.Normalize(vmin=0, vmax=len(list(G.nodes))-1)
    nodes_colours = [colourmap(normalize(i)) for i in range(len(G.nodes))]

    # Make representation
    fig, ax = plt.subplots(nrows=1, ncols=1, figsize=(16, 9))
    ax.set_title(f'Network diagram with {len(G.nodes)} nodes')
    fig.tight_layout()
    
    pos = nx.spring_layout(G, k=3, seed=42)
    # circular_layout for networks with nodes of similar importance
    
    nx.draw_networkx_edges(G, pos, arrows=True, alpha=1,
                           edge_color="lightgrey",
                           connectionstyle="arc3,rad=0.2",
                           width=edges_widths,
                           arrowsize=20, node_size=1000)
    nx.draw_networkx_nodes(G, pos, alpha=1, node_size=1000,
                           node_color=nodes_colours)
    nx.draw_networkx_labels(G, pos, font_size=8,
                            bbox={"fc": "white", "alpha": 0.5, 'pad': 3,
                                  'boxstyle': 'Round, pad=0.2',
                                  'edgecolor':'none'})
    return fig

