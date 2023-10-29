""" profile.py
Defines class Profile: Manager of interactions between user-profile, 
which can contains user customizations, and the main program.

Class Profile - methods:
    .__init__
    .validate_profilename
    .incr_run_count
    .postprocess
    .find_unread
    .add_read
    .fetch_items
    .save_items
    .save_movements
    .save_excel
    .save_figure
"""

import string
import tomllib
import importlib
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt

from product_trailer.user_data import UserData


class Profile():

    def __init__(self, profile_name: str) -> None:
        self.name = profile_name
        self.path = Path('profiles') / self.name
        self.data_path = self.path / 'data'
        if not self.data_path.is_dir():
            self.data_path.mkdir(parents=True)
        
        self.user_data = UserData(self.data_path)
        
        self.config_path = self.path / 'config'
        self.custom_modules = f'profiles.{self.name}.config'
        if not self.config_path.is_dir():
            self.config_path = Path('./product_trailer/default_profile/config')
            self.custom_modules = f'product_trailer.default_profile.config'

        # Import parameters
        with open(self.config_path / 'config.toml', mode="rb") as fp:
            cfg = tomllib.load(fp)
        self.db_config = cfg['data']

        # Report path setup
        self.output_path = self.path/cfg['output']['path']
        if not self.output_path.is_dir():
            self.output_path.mkdir(parents=True, exist_ok=True)
        
        # Custom features in input file
        self.input = cfg['input']

        # Custom tools
        processing = importlib.import_module(
            self.custom_modules + '.processing'
            )
        self.import_movements = processing.import_movements
        self.is_entry_point = processing.is_entry_point
    

    @staticmethod
    def validate_profilename(name: str) -> bool:
        valid_chars = "-_.,()" + string.ascii_letters + string.digits
        acceptable_name = ''.join(
            char for char in name if char in valid_chars
            )
        return (
            (name == acceptable_name)
            and (len(name) > 0)
            and (len(name) < 30)
        )


    def incr_run_count(self):
        self.run_count = self.user_data.fetch('run_count', 0) + 1
        self.user_data.set({'run_count': self.run_count})


    def postprocess(self, items: pd.DataFrame) -> bool:
        custom_postprocessing = importlib.import_module(
            self.custom_modules + '.postprocessing'
            )
        custom_postprocessing.postprocess(self, items)
        return True

    
    def find_unread(self, foldername: str, prefix: str) -> set:
        filesread = self.user_data.fetch('read', set())
        all_raw_files = set(map(str, Path(foldername).glob(prefix + '*')))
        return sorted(all_raw_files.difference(filesread))
    
    def add_read(self, filename: str) -> None:
        filesread = set(self.user_data.fetch('read', []))
        filesread.add(str(filename))
        self.user_data.set({'read': list(filesread)})
    

    def fetch_items(self) -> None | pd.DataFrame:
        possible_db = list(
            self.data_path.glob(self.db_config['fname_items']+'*')
            )
        if len(possible_db) == 0:
            self.last_itemdb_path = ''
            return None
        
        else:
            self.last_itemdb_path = sorted(possible_db)[-1]
            return pd.read_pickle(self.last_itemdb_path)
        
    def save_items(self, items: pd.DataFrame) -> None:
        filename = f"{self.db_config['fname_items']}{self.run_count}.pkl"
        new_itemdb_path = self.data_path / filename
        items.to_pickle(new_itemdb_path)
        
        if self.db_config['no_history']:
            if self.last_itemdb_path != '':
                self.last_itemdb_path.unlink()
        self.last_itemdb_path = new_itemdb_path
    

    def save_movements(self, list_computed_mvts: pd.DataFrame) -> None:
        if self.db_config['save_movements']:
            mvts = pd.concat(list_computed_mvts, axis=0)
            all_mvt_db = list(
                self.data_path.glob(self.db_config['fname_movements']+'*')
            )
            if len(all_mvt_db) == 0:
                new_mvts = mvts
                last_mvtdb_path = ''
            else:
                last_mvtdb_path = sorted(all_mvt_db)[-1]
                new_mvts = pd.concat([
                    pd.read_pickle(last_mvtdb_path), mvts
                    ], axis=0)
            
            fname = f"{self.db_config['fname_movements']}{self.run_count}.pkl"
            new_mvtdb_path = self.data_path / fname
            new_mvts.to_pickle(new_mvtdb_path)
            
            if self.db_config['no_history']:
                if last_mvtdb_path != '':
                    last_mvtdb_path.unlink()
            
        else:
            new_mvtdb_path = '(Movements not saved)'
    

    def save_excel(self, data: pd.DataFrame, fname: str) -> None:
        fpath = self.output_path / (fname+'.xlsx')

        if isinstance(data, pd.DataFrame):
            data.to_excel(fpath, index=False, freeze_panes=(1,0))
        
        if isinstance(data, dict):
            with pd.ExcelWriter(fpath) as writer:
                for sheetname, df in data.items():
                    df.to_excel(writer, sheet_name=sheetname,
                                index=False, freeze_panes=(1,0))


    def save_figure(self, figure: plt.figure, fname: str) -> None:
        fpath = self.output_path / (fname+'.png')
        figure.savefig(fpath, format='png')
