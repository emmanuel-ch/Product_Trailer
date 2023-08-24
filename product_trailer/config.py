"""Configuration class"""
import os
import shutil
import tomllib

class Config():

    def __init__(self, config_name) -> None:
        self.profile_name = config_name
        self.profile_path = os.path.join('profiles', self.profile_name)
        self.config_path = os.path.join('profiles', self.profile_name, 'config.toml')
        self.database_path = os.path.join('profiles', self.profile_name, 'database')
        self.import_config()


    def import_config(self):
        if not os.path.isdir(self.profile_path):
            os.makedirs(self.profile_path)
            os.makedirs(self.database_path)
        if not os.path.isfile(self.config_path):
            shutil.copy('./product_trailer/default_config.toml', self.config_path)  # Copy default config

        with open(self.config_path, mode="rb") as fp:
            imported_config = tomllib.load(fp)
        
        self.items_saved_db_prefix = imported_config['database']['filename_tracked_items']
        self.movement_db_prefix = imported_config['database']['filename_movements']
        self.one_db = imported_config['database']['only_keep_latest_version']
        self.save_mvts = imported_config['database']['save_movements']

        self.files_processed_dbpath = os.path.join(self.database_path, imported_config['database']['filename_files_processed'])

        self.reports_path = os.path.join('profiles', self.profile_name, imported_config['database']['reports_path'])
        if not os.path.isdir(self.reports_path):
            os.makedirs(self.reports_path)

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

        possible_db = [filename for filename in ['nope'] + os.listdir(self.database_path) if filename.startswith(self.items_saved_db_prefix)]
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
            f'{self.items_saved_db_prefix} {date_range_db} (saved {datetime_db_creation}).pkl'
            )
        tracked_items.to_pickle(new_db_filename)
        
        if self.one_db:
            if self.item_db_filepath != '':
                os.remove(self.item_db_filepath)
        
        self.item_db_filepath = new_db_filename
    

    def save_movements(self, list_computed_MVTS, date_range_db):
        import os
        import pandas as pd
        from datetime import datetime

        if self.save_mvts:
            MVT_DB = pd.concat(list_computed_MVTS, axis=0)
            possible_mvt_db = [filename for filename in ['nope'] + os.listdir(self.database_path) if filename.startswith(self.movement_db_prefix)]
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
                f'{self.movement_db_prefix} {date_range_db} (saved {datetime_db_creation}).pkl'
                )
            new_MVT_DB.to_pickle(new_db_mvt_filename)
            
            if self.one_db:
                if prev_dbfilename_MVTS != '':
                    os.remove(prev_dbfilename_MVTS)
            
        else:
            new_db_mvt_filename = '(Movements not saved)'