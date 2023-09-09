""" test_config.py
Tests on validate_configname, Config class.
"""

import pytest
from product_trailer.config import validate_configname



@pytest.mark.parametrize("test_input,expected", [
    ('valid-_.,()', True), 
    ('no#', False), ('no&', False), ('no<>', False), ('no!@~`', False), ('no+=*/|\\$', False), ('not valid', False)
    ])
def test_validate_configname(test_input, expected):
    assert validate_configname(test_input) == expected

