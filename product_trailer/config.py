""" config.py
Defines class Config: Manager of all interactions between user-profile
(the config) and the main program.

validate_configname
Class Config - methods:
    .__init__
    .import_config
    .postprocess
    .find_unprocessed_files
    .record_inputfile_processed
    .fetch_saved_items
    .save_items
    .save_movements
    .report_to_excel
"""

import string
import tomllib
import importlib
from datetime import datetime
from pathlib import Path
import pickle
import pandas as pd
import matplotlib.pyplot as plt

from product_trailer.user_data import UserData


def validate_configname(config_name: str) -> bool:
    valid_chars = "-_.,()" + string.ascii_letters + string.digits
    acceptable_name = ''.join(
        char for char in config_name if char in valid_chars
        )
    return (
        (config_name == acceptable_name)
        and (len(config_name) > 0)
        and (len(config_name) < 30)
    )



class Config():

    def __init__(self, config_name: str) -> None:
        self.profile_name = config_name
        self.profile_path = Path('profiles') / self.profile_name
        self.database_path = self.profile_path / 'database'
        if not self.database_path.is_dir():
            self.database_path.mkdir(parents=True)

        self.user_data = UserData(self.database_path)
        
        self.config_path = self.profile_path / 'config'
        self.custom_modules = f'profiles.{self.profile_name}.config'
        if not self.config_path.is_dir():
            self.config_path = Path('./product_trailer/default_profile/config')
            self.custom_modules = f'product_trailer.default_profile.config'

        # Import config parameters
        with open(self.config_path / 'config.toml', mode="rb") as fp:
            cfg = tomllib.load(fp)
        self.db_config = cfg['database']

        # Report path setup
        self.reports_path = self.profile_path/cfg['database']['reports_path']
        if not self.reports_path.is_dir():
            self.reports_path.mkdir(parents=True, exist_ok=True)
        
        # Custom features in input file
        self.input_features = cfg['input_data']

        # Custom tools
        processing = importlib.import_module(
            self.custom_modules + '.processing'
            )
        self.import_movements = processing.import_movements
        self.is_entry_point = processing.is_entry_point
    

    def postprocess(self, tracked_items: pd.DataFrame) -> bool:
        custom_postprocessing = importlib.import_module(
            self.custom_modules + '.postprocessing'
            )
        custom_postprocessing.postprocess(self, tracked_items)
        return True

    
    def find_unprocessed_files(self, foldername: str, prefix: str) -> set:
        processedf = self.user_data.fetch()
        if 'processedf' in processedf.keys():
            self.raw_processed = set(processedf['processedf'])
        else:
            self.raw_processed = set()
        
        all_raw_files = set(map(str, Path(foldername).glob(prefix + '*')))
        return sorted(all_raw_files.difference(self.raw_processed))
    

    def record_inputfile_processed(self, filename: str) -> None:
        self.raw_processed.add(str(filename))
        self.user_data.set({'processedf': list(self.raw_processed)})
    

    def fetch_saved_items(self) -> None | pd.DataFrame:
        possible_db = list(
            self.database_path.glob(self.db_config['fname_tracked_items']+'*')
            )
        if len(possible_db) == 0:
            self.item_db_filepath = ''
            return None
        
        else:
            self.item_db_filepath = sorted(possible_db)[-1]
            return pd.read_pickle(self.item_db_filepath)
        
    
    def save_items(self, tracked_items: pd.DataFrame) -> None:
        datetime_db = datetime.today().strftime("%Y-%m-%d %Hh%M")
        filename = (
            f"{self.db_config['fname_tracked_items']} {datetime_db}.pkl"
        )
        new_db_filename = self.database_path / filename
        
        tracked_items.to_pickle(new_db_filename)
        
        if self.db_config['only_keep_latest_version']:
            if self.item_db_filepath != '':
                self.item_db_filepath.unlink()
        self.item_db_filepath = new_db_filename
    

    def save_movements(self, list_computed_MVTS: pd.DataFrame) -> None:
        if self.db_config['save_movements']:
            MVT_DB = pd.concat(list_computed_MVTS, axis=0)
            possible_mvt_db = list(self.database_path.glob(self.db_config['fname_movements']+'*'))
            if len(possible_mvt_db) == 0:
                new_MVT_DB = MVT_DB
                prev_dbfilename_MVTS = ''
            else:
                prev_dbfilename_MVTS = sorted(possible_mvt_db)[-1]
                new_MVT_DB = pd.concat([
                    pd.read_pickle(prev_dbfilename_MVTS), MVT_DB
                    ], axis=0)
            
            datetime_db = datetime.today().strftime("%Y-%m-%d %Hh%M")
            filename = (
                f"{self.db_config['fname_movements']} {datetime_db}.pkl"
            )
            new_db_mvt_filepath = self.database_path / filename
            new_MVT_DB.to_pickle(new_db_mvt_filepath)
            
            if self.db_config['only_keep_latest_version']:
                if prev_dbfilename_MVTS != '':
                    prev_dbfilename_MVTS.unlink()
            
        else:
            new_db_mvt_filepath = '(Movements not saved)'
    

    def report_to_excel(self, data: pd.DataFrame, fname: str) -> None:
        fpath = self.reports_path / fname

        if isinstance(data, pd.DataFrame):
            data.to_excel(fpath, index=False, freeze_panes=(1,0))
        
        if isinstance(data, dict):
            with pd.ExcelWriter(fpath) as writer:
                for sheetname, df in data.items():
                    df.to_excel(writer, sheet_name=sheetname,
                                index=False, freeze_panes=(1,0))

    def save_figure(self, figure: plt.figure, fname: str) -> None:
        fpath = self.reports_path / fname
        figure.savefig(fpath, format='png')

