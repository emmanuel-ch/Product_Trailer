""" user_data.py
Defines class UserData: Handles user-data file.

Class UserData - methods:
    .__init__
"""


import json


class UserData:
    def __init__(self, dirpath):
        self.datafp = dirpath / 'userdata.json'
    
    def fetch(self):
        if self.datafp.is_file():
            with open(self.datafp, 'r') as read_file:
                return json.load(read_file)
        return {}
    
    def set(self, new_data: dict):
        data = self.fetch()
        for key, val in new_data.items():
            data[key] = val
        with open(self.datafp, 'w') as write_file:
            json.dump(data, write_file, indent=4)

