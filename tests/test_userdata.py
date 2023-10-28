""" test_userdata.py
Tests on Userdata class.
"""

from product_trailer.user_data import UserData


def test_createfile(tmp_path):
    userdata = UserData(tmp_path)
    userdata.set({'something': 'bar'})
    assert (tmp_path / 'userdata.json').is_file()

def test_setreaddata(tmp_path):
    userdata = UserData(tmp_path)
    userdata.set({'something': 'bar'})
    assert userdata.fetch('something') == 'bar'

def test_appenddata(tmp_path):
    userdata = UserData(tmp_path)
    userdata.set({'something': 'bar'})
    userdata.set({'another_thing': 'buz'})
    assert (
        userdata.fetch()
        == {'something': 'bar', 'another_thing': 'buz'}
    )

def test_read_nokey(tmp_path):
    userdata = UserData(tmp_path)
    userdata.set({'something': 'bar'})
    assert userdata.fetch('another_thing', 42) == 42
