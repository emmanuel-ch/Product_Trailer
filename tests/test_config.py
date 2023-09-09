""" test_config.py
Tests on validate_configname, Config class.
"""

from pathlib import Path
import shutil
import pytest
from product_trailer.config import validate_configname, Config



@pytest.mark.parametrize("test_input,expected", [
    ('valid-_.,()', True), 
    ('no#', False), ('no&', False), ('no<>', False), ('no!@~`', False), ('no+=*/|\\$', False), ('not valid', False)
    ])
def test_validate_configname(test_input, expected):
    assert validate_configname(test_input) == expected


class TestConfigCreation():
    
    def test_create_profile_directory(self):
        config_name = 'a_test_config_for_testing'
        _ = Config(config_name)

        profile_path = Path('profiles') / config_name
        profile_exists = profile_path.is_dir()

        if profile_exists:
            shutil.rmtree(profile_path)

        assert profile_exists
    

    def test_recover_profile_no_database(self):
        config_name = 'a_test_config_for_testing'
        _ = Config(config_name)
        profile_db_path = Path('profiles') / config_name / 'database'
        shutil.rmtree(profile_db_path)

        _ = Config(config_name)
        db_exists = profile_db_path.is_dir()

        shutil.rmtree((Path('profiles') / config_name))

        assert db_exists
    

    def test_recover_profile_no_config(self):
        config_name = 'a_test_config_for_testing'
        _ = Config(config_name)
        profile_config_path = Path('profiles') / config_name / 'config'
        shutil.rmtree(profile_config_path)

        _ = Config(config_name)
        db_exists = profile_config_path.is_dir()

        shutil.rmtree((Path('profiles') / config_name))

        assert db_exists
    
    