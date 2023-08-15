"""Configuration class"""


class Config():

    def __init__(self, config_name, _DB_DIR_) -> None:
        import os

        self.config_name = config_name

        self.db_dir = _DB_DIR_
        self.db_path = os.path.join(self.db_dir, self.config_name)
        if not os.path.isdir(self.db_path):
            os.makedirs(self.db_path)

        self.input_files_processed_filename = 'input_files_processed.pkl'
        self.input_files_processed_dbpath = os.path.join(self.db_dir, self.config_name, self.input_files_processed_filename)

        self.items_saved_db_prefix = 'Tracked _'

    
    def find_unprocessed_files(self, foldername, prefix_input_files):
        import os
        import pickle

        if os.path.isfile(self.input_files_processed_dbpath):
            with open(self.input_files_processed_dbpath, 'rb') as f:
                self.input_files_processed = pickle.load(f)
        else:
            self.input_files_processed = set()
        
        all_raw_files = {filename for filename in os.listdir(foldername) if filename.startswith(prefix_input_files)}

        return sorted(all_raw_files.difference(self.input_files_processed))
    

    def record_inputfile_processed(self, filename):
        import pickle

        self.input_files_processed.add(filename)

        with open(self.input_files_processed_dbpath, 'wb') as f:
            pickle.dump(self.input_files_processed, f)
    

    def fetch_saved_items(self):
        import os
        import pandas as pd

        possible_db = [filename for filename in ['nope'] + os.listdir(self.db_path) if filename.startswith(self.items_saved_db_prefix)]
        if len(possible_db) == 0:
            return None, ''
        
        else:
            db_trackedItems_filename = sorted(possible_db)[-1]
            prev_dbfilename_items = os.path.join(self.db_path, db_trackedItems_filename)
            return pd.read_pickle(prev_dbfilename_items), prev_dbfilename_items
        