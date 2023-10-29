""" test_profile.py
Tests on Profile class.
"""

from pathlib import Path
import shutil

import pytest
import pandas as pd
import matplotlib.pyplot as plt

from product_trailer.profile import Profile


@pytest.fixture
def dummy_profile():
    config_name = 'test_profile'
    profile_path = Path('profiles') / config_name
    testcfg = Profile(config_name)
    yield testcfg
    shutil.rmtree(profile_path)


@pytest.mark.parametrize(
    "test_in,expected_out",
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
def test_validate_profilename(test_in, expected_out):
    assert Profile.validate_profilename(test_in) == expected_out


def test_create_profile_directory(dummy_profile):
    profile_path = Path('profiles/test_profile')
    assert  profile_path.is_dir()

def test_use_standard_config(dummy_profile):
    assert dummy_profile.config_path == Path('./product_trailer/default_profile/config')

def test_use_custom_config():
    config_name = 'test_profile'
    defaultconfigp = Path('./product_trailer/default_profile')
    profile_path = Path('profiles/test_profile')
    shutil.copytree(defaultconfigp, profile_path)
    testcfg = Profile(config_name)
    test = testcfg.config_path == Path(f'./profiles/{config_name}/config')
    shutil.rmtree(profile_path)
    assert test

def test_incr_run_count(dummy_profile):
    dummy_profile.incr_run_count()
    assert dummy_profile.user_data.fetch('run_count') == 1

def test_add_read(dummy_profile):
    dummy_profile.add_read('somefilepath')
    assert dummy_profile.user_data.fetch('read') == ['somefilepath']

def test_find_unread(tmp_path, dummy_profile):
    fprefix = 'Some '
    newfilep = tmp_path / (fprefix + 'file 42.txt')
    with open(newfilep, 'w') as newfile:
        newfile.write('bar')
    assert dummy_profile.find_unread(tmp_path, fprefix) == [str(newfilep)]

def test_find_unprocessed_with_preexisting_file(tmp_path, dummy_profile):
    fprefix = 'Some '
    newfilep1 = tmp_path / (fprefix + 'file 41.txt')
    dummy_profile.add_read(str(newfilep1))
    newfilep2 = tmp_path / (fprefix + 'file 42.txt')
    with open(newfilep1, 'w') as newfile:
        newfile.write('bar')
    with open(newfilep2, 'w') as newfile:
        newfile.write('bar')
    assert dummy_profile.find_unread(tmp_path, fprefix) == [str(newfilep2)]

def test_fetch_items_nothing(dummy_profile):
    assert dummy_profile.fetch_items() is None

def test_fetch_items_1file(dummy_profile):
    itemdbp = (
        dummy_profile.data_path
        / (dummy_profile.db_config['fname_items'] + '42')
    )
    itemdb = pd.DataFrame({'a': [1, 2, 3], 'b': [9, 8, 7]})
    itemdb.to_pickle(itemdbp)
    assert dummy_profile.fetch_items().equals(itemdb)

def test_fetch_saved_items_2files(dummy_profile):
    itemdbp1 = (
        dummy_profile.data_path
        / (dummy_profile.db_config['fname_items'] + '42')
    )
    itemdb1 = pd.DataFrame({'a': [1, 2, 3], 'b': [9, 8, 7]})
    itemdb1.to_pickle(itemdbp1)
    itemdbp2 = (
        dummy_profile.data_path
        / (dummy_profile.db_config['fname_items'] + '43')
    )
    itemdb2 = pd.DataFrame({'c': [4, 5, 6], 'd': [7, 8, 9]})
    itemdb2.to_pickle(itemdbp2)
    assert dummy_profile.fetch_items().equals(itemdb2)

def test_save_items(dummy_profile):
    itemdb = pd.DataFrame({'a': [1, 2, 3], 'b': [9, 8, 7]})
    dummy_profile.incr_run_count()
    dummy_profile.last_itemdb_path = ''
    dummy_profile.save_items(itemdb)
    database_path = Path('profiles/test_profile/data/')
    itemdb_filen = dummy_profile.db_config['fname_items']+'1.pkl'
    assert (database_path/itemdb_filen).is_file()

def test_save_movements(dummy_profile):
    dummy_profile.incr_run_count()
    dummy_profile.db_config['save_movements'] = True
    list_mvts = [pd.DataFrame({'a': [1, 2, 3], 'b': [9, 8, 7]})]
    dummy_profile.save_movements(list_mvts)
    database_path = Path('profiles/test_profile/data/')
    mvtdb_filen = dummy_profile.db_config['fname_movements']+'1.pkl'
    assert (database_path/mvtdb_filen).is_file()

def test_report_to_excel(dummy_profile):
    somedf = pd.DataFrame({'a': [1, 2, 3], 'b': [9, 8, 7]})
    dummy_profile.save_excel(somedf, fname='some_report')
    expectedfp = Path('profiles/test_profile/') / 'some_report.xlsx'
    assert expectedfp.is_file()

def test_save_figure(dummy_profile):
    fig, ax = plt.subplots(nrows=1, ncols=1, figsize=(3, 3))
    ax.set_title(f'Some title')
    dummy_profile.save_figure(fig, 'some_figure')
    expectedfp = Path('profiles/test_profile/') / 'some_figure.png'
    assert expectedfp.is_file()
