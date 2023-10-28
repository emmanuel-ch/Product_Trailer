""" test_config.py
Tests on Config class.
"""

from pathlib import Path
import shutil
import pytest
from product_trailer.config import Config



@pytest.mark.parametrize(
    "test_input,expected",
    [
        ("valid-_.,()", True),
        ("no#", False),
        ("no&", False),
        ("no<>", False),
        ("no!@~`", False),
        ("no+=*/|\\$", False),
        ("not valid", False),
    ],
)
def test_validate_configname(test_input, expected):
    assert Config.validate_configname(test_input) == expected


def test_create_profile_directory():
    config_name = 'test_config'
    _ = Config(config_name)
    profile_path = Path('profiles') / config_name
    profile_exists = profile_path.is_dir()

    if profile_exists:
        shutil.rmtree(profile_path)
    assert profile_exists

def test_use_standard_config():
    config_name = 'test_config'
    profile_path = Path('profiles') / config_name
    testcfg = Config(config_name)
    test = testcfg.config_path == Path('./product_trailer/default_profile/config')
    shutil.rmtree(profile_path)
    assert test

def test_use_custom_config():
    config_name = 'test_config'
    defaultconfigp = Path('./product_trailer/default_profile')
    profile_path = Path('profiles') / config_name
    shutil.copytree(defaultconfigp, profile_path)
    testcfg = Config(config_name)
    test = testcfg.config_path == Path(f'./profiles/{config_name}/config')
    shutil.rmtree(profile_path)
    assert test

def test_incr_run_count():
    config_name = 'test_config'
    profile_path = Path('profiles') / config_name
    testcfg = Config(config_name)
    testcfg.incr_run_count()
    test = testcfg.user_data.fetch('run_count') == 1
    shutil.rmtree(profile_path)
    assert test

def test_find_unprocessed(tmp_path):
    config_name = 'test_config'
    profile_path = Path('profiles') / config_name
    testcfg = Config(config_name)
    fprefix = 'Some '
    newfilep = tmp_path / (fprefix + 'file 42.txt')
    with open(newfilep, 'w') as newfile:
        newfile.write('bar')
    test = testcfg.find_unprocessed_files(tmp_path, fprefix) == [str(newfilep)]
    shutil.rmtree(profile_path)
    newfilep.unlink()
    assert test

def test_find_unprocessed_with_preexisting_file(tmp_path):
    config_name = 'test_config'
    profile_path = Path('profiles') / config_name
    testcfg = Config(config_name)
    fprefix = 'Some '
    newfilep1 = tmp_path / (fprefix + 'file 41.txt')
    testcfg.record_inputfile_processed(str(newfilep1))
    newfilep2 = tmp_path / (fprefix + 'file 42.txt')
    with open(newfilep1, 'w') as newfile:
        newfile.write('bar')
    with open(newfilep2, 'w') as newfile:
        newfile.write('bar')
    test = testcfg.find_unprocessed_files(tmp_path, fprefix) == [str(newfilep2)]
    shutil.rmtree(profile_path)
    newfilep1.unlink()
    newfilep2.unlink()
    assert test
