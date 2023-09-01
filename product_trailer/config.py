"""Configuration class"""
import os
import shutil
import tomllib
import importlib
import pandas as pd

class Config():

    def __init__(self, config_name) -> None:
        self.profile_name = config_name
        self.profile_path = os.path.join('profiles', self.profile_name)
        self.config_path = os.path.join('profiles', self.profile_name, 'config.toml')
        self.database_path = os.path.join('profiles', self.profile_name, 'database')
        self.import_config()


    def import_config(self):
        # Copy default config if none exists
        if not os.path.isdir(self.profile_path):
            shutil.copytree('./profiles/default_profile/', self.profile_path)

        # Import config parameters
        with open(self.config_path, mode="rb") as fp:
            imported_config = tomllib.load(fp)
        self.db_config = imported_config['database']
        self.files_processed_dbpath = os.path.join(self.database_path, imported_config['database']['filename_files_processed'])

        # Report path setup
        self.reports_path = os.path.join('profiles', self.profile_name, imported_config['database']['reports_path'])
        if not os.path.isdir(self.reports_path):
            os.makedirs(self.reports_path)
        
        # Custom features in input file
        self.input_features = imported_config['input_data']

        # Custom tools
        custom_tools = importlib.import_module(f'profiles.{self.profile_name}.custom_tools')
        self.import_movements = custom_tools.import_movements
        self.is_entry_point = custom_tools.is_entry_point
        return True
    

    def postprocess(self, tracked_items):
        custom_postprocessing = importlib.import_module(f'profiles.{self.profile_name}.postprocessing')
        custom_postprocessing.postprocess(self, tracked_items)
        return True

    
    def find_unprocessed_files(self, foldername, prefix_input_files):
        import os
        import pickle

        if os.path.isfile(self.files_processed_dbpath):
            with open(self.files_processed_dbpath, 'rb') as f:
                self.input_files_processed = pickle.load(f)
        else:
            self.input_files_processed = set()
        
        all_raw_files = {filename for filename in os.listdir(foldername) if filename.startswith(prefix_input_files)}

        return sorted(all_raw_files.difference(self.input_files_processed))
    

    def record_inputfile_processed(self, filename):
        import pickle

        self.input_files_processed.add(filename)

        with open(self.files_processed_dbpath, 'wb') as f:
            pickle.dump(self.input_files_processed, f)
    

    def fetch_saved_items(self):
        import os
        import pandas as pd

        possible_db = [filename for filename in ['nope'] + os.listdir(self.database_path) if filename.startswith(self.db_config['filename_tracked_items'])]
        if len(possible_db) == 0:
            self.item_db_filepath = ''
            return None
        
        else:
            db_trackedItems_filename = sorted(possible_db)[-1]
            self.item_db_filepath = os.path.join(self.database_path, db_trackedItems_filename)
            return pd.read_pickle(self.item_db_filepath)
        
    
    def save_items(self, tracked_items, date_range_db):
        import os
        from datetime import datetime

        datetime_db_creation = datetime.today().strftime("%Y-%m-%d %Hh%M")
        new_db_filename = os.path.join(
            self.database_path, 
            f"{self.db_config['filename_tracked_items']} {date_range_db} (saved {datetime_db_creation}).pkl"
            )
        tracked_items.to_pickle(new_db_filename)
        
        if self.db_config['only_keep_latest_version']:
            if self.item_db_filepath != '':
                os.remove(self.item_db_filepath)
        
        self.item_db_filepath = new_db_filename
    

    def save_movements(self, list_computed_MVTS, date_range_db):
        import os
        import pandas as pd
        from datetime import datetime

        if self.db_config['save_movements']:
            MVT_DB = pd.concat(list_computed_MVTS, axis=0)
            possible_mvt_db = [filename for filename in ['nope'] + os.listdir(self.database_path) if filename.startswith(self.db_config['filename_movements'])]
            if len(possible_mvt_db) == 0:
                new_MVT_DB = MVT_DB
                prev_dbfilename_MVTS = ''
            else:
                db_mvts_filename = sorted(possible_mvt_db)[-1]
                prev_dbfilename_MVTS = os.path.join(self.database_path, db_mvts_filename)
                MVT_DB_old = pd.read_pickle(prev_dbfilename_MVTS)
                new_MVT_DB = pd.concat([MVT_DB_old, MVT_DB], axis=0)
            
            datetime_db_creation = datetime.today().strftime("%Y-%m-%d %Hh%M")
            new_db_mvt_filename = os.path.join(
                self.database_path, 
                f"{self.db_config['filename_movements']} {date_range_db} (saved {datetime_db_creation}).pkl"
                )
            new_MVT_DB.to_pickle(new_db_mvt_filename)
            
            if self.db_config['only_keep_latest_version']:
                if prev_dbfilename_MVTS != '':
                    os.remove(prev_dbfilename_MVTS)
            
        else:
            new_db_mvt_filename = '(Movements not saved)'
    

    def report_to_excel(self, data: pd.DataFrame, filename: str):

        if isinstance(data, pd.DataFrame):
            outp_filename = os.path.join(self.reports_path, filename + '.xlsx')
            data.to_excel(outp_filename, index=False, freeze_panes=(1,0))
        
        if isinstance(data, dict):
            outp_filename = os.path.join(self.reports_path, filename + '.xlsx')
            with pd.ExcelWriter(outp_filename) as writer:
                for sheetname, df in data.items():
                    df.to_excel(writer, sheet_name=sheetname, index=False, freeze_panes=(1,0))

