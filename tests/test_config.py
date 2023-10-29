""" test_config.py
Tests on Config class.
"""

from pathlib import Path
import shutil
import pytest

from product_trailer.config import Config


@pytest.fixture
def dummy_config():
    config_name = 'test_config'
    profile_path = Path('profiles') / config_name
    testcfg = Config(config_name)
    yield testcfg
    shutil.rmtree(profile_path)


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

def test_use_standard_config(dummy_config):
    assert dummy_config.config_path == Path('./product_trailer/default_profile/config')

def test_use_custom_config():
    config_name = 'test_config'
    defaultconfigp = Path('./product_trailer/default_profile')
    profile_path = Path('profiles') / config_name
    shutil.copytree(defaultconfigp, profile_path)
    testcfg = Config(config_name)
    test = testcfg.config_path == Path(f'./profiles/{config_name}/config')
    shutil.rmtree(profile_path)
    assert test

def test_incr_run_count(dummy_config):
    dummy_config.incr_run_count()
    assert dummy_config.user_data.fetch('run_count') == 1

def test_find_unprocessed(tmp_path, dummy_config):
    fprefix = 'Some '
    newfilep = tmp_path / (fprefix + 'file 42.txt')
    with open(newfilep, 'w') as newfile:
        newfile.write('bar')
    assert dummy_config.find_unprocessed_files(tmp_path, fprefix) == [str(newfilep)]

def test_find_unprocessed_with_preexisting_file(tmp_path, dummy_config):
    fprefix = 'Some '
    newfilep1 = tmp_path / (fprefix + 'file 41.txt')
    dummy_config.record_inputfile_processed(str(newfilep1))
    newfilep2 = tmp_path / (fprefix + 'file 42.txt')
    with open(newfilep1, 'w') as newfile:
        newfile.write('bar')
    with open(newfilep2, 'w') as newfile:
        newfile.write('bar')
    assert dummy_config.find_unprocessed_files(tmp_path, fprefix) == [str(newfilep2)]
