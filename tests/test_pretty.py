import pytest

from test_client import patch_adapter


@pytest.fixture(scope='module')
def pretty_client():
    from pachypy.pretty import PrettyPachydermClient
    return PrettyPachydermClient()


def test_pipeline_sort_key():
    from pachypy.pretty import _pipeline_sort_key
    assert _pipeline_sort_key({'a': [], 'b': ['a']}) == {'a': 'a/0', 'b': 'a/1'}
    assert _pipeline_sort_key({'b': [], 'a': ['b']}) == {'b': 'a/0', 'a': 'a/1'}
    assert _pipeline_sort_key({'a': [], 'b': [], 'c': ['a', 'b'], 'd': []}) == {'a': 'a/0', 'b': 'a/0', 'c': 'a/1', 'd': 'd/0'}
    assert _pipeline_sort_key({'a': [], 'b': ['a'], 'c': ['a'], 'd': ['b']}) == {'a': 'a/0', 'b': 'a/1', 'c': 'a/1', 'd': 'a/2'}


def test_fa():
    from pachypy.pretty import _fa
    assert 'fa-some-icon' in _fa('some-icon')


def test_style_job_progress():
    import pandas as pd
    from pachypy.pretty import _style_job_progress
    assert not _style_job_progress(pd.Series(['0% | 0 + 0 / 2']))[0].endswith('0.0%)')
    assert _style_job_progress(pd.Series(['50% | 1 + 0 / 2']))[0].endswith('50.0%)')
    assert _style_job_progress(pd.Series(['100% | 1 + 1 / 2']))[0] == ''


def test_format_datetime():
    from datetime import datetime
    from dateutil.relativedelta import relativedelta as rd
    from pachypy.pretty import _format_datetime
    t = datetime.now()
    t = [t.year, t.month, t.day]
    assert _format_datetime(None) == ''
    assert _format_datetime(datetime(2019, 1, 2, 11, 11, 23)) == '2 Jan 2019 at 11:11'
    assert _format_datetime(datetime(2019, 2, 3, 23, 11, 59)) == '3 Feb 2019 at 23:11'
    assert _format_datetime(datetime(*t + [11, 11])) == 'Today at 11:11'
    assert _format_datetime(datetime(*t + [11, 11]) - rd(days=1)) == 'Yesterday at 11:11'
    assert _format_datetime(datetime(*t + [11, 11]) + rd(days=1)) == 'Tomorrow at 11:11'


def test_format_date():
    from datetime import date
    from dateutil.relativedelta import relativedelta as rd
    from pachypy.pretty import _format_date
    assert _format_date(None) == ''
    assert _format_date(date(2019, 1, 2)) == '2 Jan 2019'
    assert _format_date(date(2019, 2, 3)) == '3 Feb 2019'
    assert _format_date(date.today()) == 'Today'
    assert _format_date(date.today() - rd(days=1)) == 'Yesterday'
    assert _format_date(date.today() + rd(days=1)) == 'Tomorrow'


def test_format_duration():
    from pachypy.pretty import _format_duration
    assert _format_duration(None) == ''
    assert _format_duration(0) == ''
    assert _format_duration(0.001) == '1 ms'
    assert _format_duration(1) == '1 sec'
    assert _format_duration(2) == '2 secs'
    assert _format_duration(2.1) == '2 secs'
    assert _format_duration(60) == '1 min'
    assert _format_duration(120) == '2 mins'
    assert _format_duration(121) == '2 mins'
    assert _format_duration(3600) == '1 hour'
    assert _format_duration(7200) == '2 hours'
    assert _format_duration(3660) == '1 hour, 1 min'
    assert _format_duration(7260) == '2 hours, 1 min'
    assert _format_duration(7320) == '2 hours, 2 mins'
    assert _format_duration(7322) == '2 hours, 2 mins'


def test_format_size():
    from pachypy.pretty import _format_size
    assert _format_size(0) == '0 bytes'
    assert _format_size(1) == '1 byte'
    assert _format_size(1000) == '1.0 KB'
    assert _format_size(10100) == '10.1 KB'
    assert _format_size(1200000) == '1.2 MB'
    assert _format_size(100300000) == '100.3 MB'
    assert _format_size(1400000000) == '1.4 GB'
    assert _format_size(1500000000000) == '1.5 TB'
    assert _format_size(1600000000000000) == '1.6 PB'


@patch_adapter()
def test_list_repos(pretty_client, **mocks):
    del mocks
    html = pretty_client.list_repos()
    assert 'use.fontawesome.com' in html.data
    assert '<table' in html.data and html.data.count('<tr') == 7


@patch_adapter()
def test_list_commits(pretty_client, **mocks):
    del mocks
    html = pretty_client.list_commits('test_x_pipeline_3')
    assert 'use.fontawesome.com' in html.data
    assert '<table' in html.data and html.data.count('<tr') == 11


@patch_adapter()
def test_list_files(pretty_client, **mocks):
    del mocks
    html = pretty_client.list_files('test_x_pipeline_3', files_only=False)
    assert 'use.fontawesome.com' in html.data
    assert '<table' in html.data and html.data.count('<tr') == 8


@patch_adapter()
def test_list_pipelines(pretty_client, **mocks):
    del mocks
    html = pretty_client.list_pipelines()
    assert 'use.fontawesome.com' in html.data
    assert '<table' in html.data and html.data.count('<tr') == 6


@patch_adapter()
def test_list_jobs(pretty_client, **mocks):
    del mocks
    html = pretty_client.list_jobs()
    assert 'use.fontawesome.com' in html.data
    assert '<table' in html.data and html.data.count('<tr') == 9


@patch_adapter()
def test_list_datums(pretty_client, **mocks):
    del mocks
    html = pretty_client.list_datums('e26ccf65131b4b3d9087cebc2f944279')
    assert 'use.fontawesome.com' in html.data
    assert '<table' in html.data and html.data.count('<tr') == 11


@patch_adapter()
def test_get_logs(pretty_client, capsys, **mocks):
    del mocks
    pretty_client.get_logs('test_x_pipeline_5', last_job_only=False)
    output = capsys.readouterr().out
    assert output.count('\n') == 25
    assert output.count(' | Job ') == 2
