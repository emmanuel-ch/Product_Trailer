""" config.py
Defines class Config: Manager of all interactions between user-profile
(the config) and the main program.

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
import shutil
import tomllib
import importlib
from datetime import datetime
from pathlib import Path
import pickle
import pandas as pd



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
        self.config_path = self.profile_path / 'config'
        self.import_config()


    def import_config(self) -> bool:
        # Copy default config if none exists
        default = './profiles/default_profile/'
        if not self.profile_path.is_dir():
            shutil.copytree(default, self.profile_path)
        if not self.database_path.is_dir():
            shutil.copytree(default + 'database', self.profile_path/'database')
        if not self.config_path.is_dir():
            shutil.copytree(default + 'config', self.profile_path/'config')

        # Import config parameters
        with open(self.config_path / 'config.toml', mode="rb") as fp:
            imported_config = tomllib.load(fp)
        self.db_config = imported_config['database']
        self.files_processed_dbpath = self.database_path/imported_config['database']['filename_files_processed']

        # Report path setup
        self.reports_path = self.profile_path/imported_config['database']['reports_path']
        if not self.reports_path.is_dir():
            self.reports_path.mkdir(parents=True, exist_ok=True)
        
        # Custom features in input file
        self.input_features = imported_config['input_data']

        # Custom tools
        processing = importlib.import_module(
            f'profiles.{self.profile_name}.config.processing'
            )
        self.import_movements = processing.import_movements
        self.is_entry_point = processing.is_entry_point
        return True
    

    def postprocess(self, tracked_items: pd.DataFrame) -> bool:
        custom_postprocessing = importlib.import_module(
            f'profiles.{self.profile_name}.config.postprocessing'
            )
        custom_postprocessing.postprocess(self, tracked_items)
        return True

    
    def find_unprocessed_files(self, foldername: str,
                               prefix_input_files: str) -> set:
        if self.files_processed_dbpath.is_file():
            with open(self.files_processed_dbpath, 'rb') as f:
                self.input_files_processed = pickle.load(f)
        else:
            self.input_files_processed = set()
        
        all_raw_files = set(Path(foldername).glob(prefix_input_files + '*'))
        return sorted(all_raw_files.difference(self.input_files_processed))
    

    def record_inputfile_processed(self, filename: str) -> None:
        self.input_files_processed.add(filename)

        with open(self.files_processed_dbpath, 'wb') as f:
            pickle.dump(self.input_files_processed, f)
    

    def fetch_saved_items(self) -> None | pd.DataFrame:
        possible_db = list(
            self.database_path.glob(self.db_config['filename_tracked_items']+'*')
            )
        if len(possible_db) == 0:
            self.item_db_filepath = ''
            return None
        
        else:
            self.item_db_filepath = sorted(possible_db)[-1]
            return pd.read_pickle(self.item_db_filepath)
        
    
    def save_items(self, tracked_items: pd.DataFrame,
                   date_range_db: str) -> None:
        datetime_db_creation = datetime.today().strftime("%Y-%m-%d %Hh%M")
        new_db_filename = self.database_path/(
            f"{self.db_config['filename_tracked_items']} {date_range_db} ",
            f"(saved {datetime_db_creation}).pkl"
        )
        tracked_items.to_pickle(new_db_filename)
        
        if self.db_config['only_keep_latest_version']:
            if self.item_db_filepath != '':
                self.item_db_filepath.unlink()
        self.item_db_filepath = new_db_filename
    

    def save_movements(self, list_computed_MVTS: pd.DataFrame,
                       date_range_db: str) -> None:
        if self.db_config['save_movements']:
            MVT_DB = pd.concat(list_computed_MVTS, axis=0)
            possible_mvt_db = list(self.database_path.glob(self.db_config['filename_movements']+'*'))
            if len(possible_mvt_db) == 0:
                new_MVT_DB = MVT_DB
                prev_dbfilename_MVTS = ''
            else:
                prev_dbfilename_MVTS = sorted(possible_mvt_db)[-1]
                new_MVT_DB = pd.concat([
                    pd.read_pickle(prev_dbfilename_MVTS), MVT_DB
                    ], axis=0)
            
            datetime_db_creation = datetime.today().strftime("%Y-%m-%d %Hh%M")
            new_db_mvt_filepath = self.database_path/(
                f"{self.db_config['filename_movements']} {date_range_db}",
                f"(saved {datetime_db_creation}).pkl"
            )
            new_MVT_DB.to_pickle(new_db_mvt_filepath)
            
            if self.db_config['only_keep_latest_version']:
                if prev_dbfilename_MVTS != '':
                    prev_dbfilename_MVTS.unlink()
            
        else:
            new_db_mvt_filepath = '(Movements not saved)'
    

    def report_to_excel(self, data: pd.DataFrame, filename: str) -> None:
        outp_filename = self.reports_path / filename

        if isinstance(data, pd.DataFrame):
            data.to_excel(outp_filename, index=False, freeze_panes=(1,0))
        
        if isinstance(data, dict):
            with pd.ExcelWriter(outp_filename) as writer:
                for sheetname, df in data.items():
                    df.to_excel(writer, sheet_name=sheetname,
                                index=False, freeze_panes=(1,0))

