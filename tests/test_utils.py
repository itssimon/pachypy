import os

import pandas as pd


def test_wildcard_filter():
    from pachypy.utils import wildcard_filter, wildcard_match
    x = ['a', 'ab', 'b']
    assert wildcard_match('', None) is True
    assert wildcard_match('', '*') is True
    assert wildcard_match('', '?') is False
    assert wildcard_match('a', '*') is True
    assert wildcard_match('a', 'a') is True
    assert wildcard_match('b', 'a') is False
    assert wildcard_filter(x, '*') == x
    assert wildcard_filter(x, None) == x
    assert wildcard_filter(x, 'a') == ['a']
    assert wildcard_filter(x, ['a']) == ['a']
    assert wildcard_filter(x, [['a']]) == ['a']
    assert wildcard_filter(x, 'a*') == ['a', 'ab']
    assert wildcard_filter(x, 'a?') == ['ab']
    assert wildcard_filter(x, [['a*'], 'b']) == x
    assert wildcard_filter(x, ['*a', '*b']) == x
    assert wildcard_filter(x, ['a', 'b']) == ['a', 'b']


def test_expand_files():
    from pathlib import Path
    from pachypy.utils import expand_files
    mock_dir = lambda glob: os.path.join(os.path.dirname(__file__), 'mock', glob)
    assert len(expand_files(None)) == 0
    assert len(expand_files(mock_dir('*.csv'))) == 7
    assert len(expand_files(Path(mock_dir('*.csv')))) == 7
    assert len(expand_files([mock_dir('list_*.csv'), Path(mock_dir('get_*.csv'))])) == 7


def test_invert_dict():
    from pachypy.utils import invert_dict
    assert invert_dict({'a': '1'}) == {'1': ['a']}
    assert invert_dict({'a': '1', 'b': '1'}) == {'1': ['a', 'b']}
    assert invert_dict({'a': '1', 'b': '2'}) == {'1': ['a'], '2': ['b']}


def test_to_timestamp():
    from pachypy.utils import to_timestamp
    assert to_timestamp(0, 0) is None
    assert to_timestamp(1554878996, 0) == pd.Timestamp('2019-04-10 06:49:56', tz='utc')


def test_to_timedelta():
    from pachypy.utils import to_timedelta
    assert to_timedelta(0, 0) == pd.Timedelta(0, unit='s')
    assert to_timedelta(0, 1) == pd.Timedelta(0.1, unit='s')
