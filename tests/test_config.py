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
    