""" postprocessing_tk.py
Ready-made postprocessing functions.

Functions:
    make_standard_report
    make_exportable_hist
"""


from itertools import groupby
import functools
import numpy as np
import pandas as pd
import networkx as nx
import matplotlib
import matplotlib.pyplot as plt



def make_standard_report(tracked_Items: pd.DataFrame) -> pd.DataFrame:
    TI = tracked_Items.copy(deep=True)
    TI['Route'] = TI['Waypoints'].apply(
        lambda wpts: ' > '.join(list(map('.'.join, np.array(wpts)[:, 1:3])))
        )
    TI['DCs'] = TI['Waypoints'].apply(
        lambda wpts: [i[0] for i in groupby(np.array(wpts)[:,1])]
        )

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
        return '  >>>  '.join(list(map(
            lambda x: ', '.join(map(str, [x[0].strftime('%Y-%m-%d'), *x[1:]])),
            wpts)))
    TI['Waypoints'] = TI['Waypoints'].apply(lambda wpts: decorate_wpts(wpts))

    return TI


def make_exportable_hist(tracked_Items: pd.DataFrame) -> pd.DataFrame:
    tobe_rtn = (
        tracked_Items
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
    tobe_rtn['Landing_Date'] = tobe_rtn.apply(
        lambda row: np.nan if row['WaypointNo']==1 else row['Landing_Date'],
        axis=1
        )
    tobe_rtn['Landing_Code'] = tobe_rtn.apply(
        lambda row: np.nan if row['WaypointNo']==1 else row['Landing_Code'],
        axis=1
        )
    
    return tobe_rtn


def collect_stock_move(df, node_level):
    stock_move = dict()
    def node_name(wpt, node_level):
            if node_level == 'company':
                return wpt[1]
    node_name = functools.partial(node_name, node_level=node_level)

    def assign_move(series):
        qty = series['QTY']
        wpts = series['Waypoints']
        
        for i, wpt in enumerate(wpts):
            if i == 0:
                continue
            node_from = node_name(wpts[i-1])
            node_to = node_name(wpt)
            if node_from != node_to:
                move_name = node_from + '-' + node_to
                if move_name in stock_move.keys():
                    stock_move[move_name][2] += qty
                else:
                    stock_move[move_name] = [node_from, node_to, qty]
    
    df.apply(assign_move, axis=1)
    return stock_move


def generate_stock_move_map(stock_move, fname, max_edge_width=4):
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

    plt.savefig(fname, format='png')