""" user_data.py
Defines class UserData: Handles user-data file.

Class UserData - methods:
    .__init__
    .fetch
    .set
"""


from typing import Any
import json


class UserData:
    def __init__(self, dirpath):
        self.datafp = dirpath / 'userdata.json'
    
    def fetch(self, item: str | None = None, default_value: Any = None):
        if self.datafp.is_file():
            with open(self.datafp, 'r') as read_file:
                data = json.load(read_file)
        else:
            data = {}
    
        if item is None:
            return data
        else:
            if item in data.keys():
                return data[item]
            else:
                return default_value
    
    def set(self, new_data: dict):
        data = self.fetch()
        for key, val in new_data.items():
            data[key] = val
        with open(self.datafp, 'w') as write_file:
            json.dump(data, write_file, indent=4)

