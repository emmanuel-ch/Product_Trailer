"""Configuration class"""


class Config():

    def __init__(self, config_name, _DB_DIR_, one_db, save_mvts) -> None:
        import os

        self.config_name = config_name

        self.one_db = one_db
        self.save_mvts = save_mvts

        self.db_dir = _DB_DIR_
        self.db_path = os.path.join(self.db_dir, self.config_name)
        if not os.path.isdir(self.db_path):
            os.makedirs(self.db_path)

        self.input_files_processed_filename = 'input_files_processed.pkl'
        self.input_files_processed_dbpath = os.path.join(self.db_dir, self.config_name, self.input_files_processed_filename)

        self.items_saved_db_prefix = 'Tracked _'


        self.movement_db_prefix = 'All movements _'

    
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
            self.item_db_filepath = ''
            return None
        
        else:
            db_trackedItems_filename = sorted(possible_db)[-1]
            self.item_db_filepath = os.path.join(self.db_path, db_trackedItems_filename)
            return pd.read_pickle(self.item_db_filepath)
        
    
    def save_items(self, tracked_items, date_range_db):
        import os
        from datetime import datetime

        datetime_db_creation = datetime.today().strftime("%Y-%m-%d %Hh%M")
        new_db_filename = os.path.join(
            self.db_path, 
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
            possible_mvt_db = [filename for filename in ['nope'] + os.listdir(self.db_path) if filename.startswith(self.movement_db_prefix)]
            if len(possible_mvt_db) == 0:
                new_MVT_DB = MVT_DB
                prev_dbfilename_MVTS = ''
            else:
                db_mvts_filename = sorted(possible_mvt_db)[-1]
                prev_dbfilename_MVTS = os.path.join(self.db_path, db_mvts_filename)
                MVT_DB_old = pd.read_pickle(prev_dbfilename_MVTS)
                new_MVT_DB = pd.concat([MVT_DB_old, MVT_DB], axis=0)
            
            datetime_db_creation = datetime.today().strftime("%Y-%m-%d %Hh%M")
            new_db_mvt_filename = os.path.join(
                self.db_path, 
                f'{self.movement_db_prefix} {date_range_db} (saved {datetime_db_creation}).pkl'
                )
            new_MVT_DB.to_pickle(new_db_mvt_filename)
            
            if self.one_db:
                if prev_dbfilename_MVTS != '':
                    os.remove(prev_dbfilename_MVTS)
            
        else:
            new_db_mvt_filename = '(Movements not saved)'