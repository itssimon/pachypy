import pytest

from pachypy.pretty import PrettyPachydermClient
from test_client import patch_adapter


@pytest.fixture(scope='module')
def pretty_client():
    return PrettyPachydermClient()


def test_list_repos(pretty_client: PrettyPachydermClient):
    with patch_adapter():
        html = pretty_client.list_repos()
        assert 'use.fontawesome.com' in html.data
        assert '<table' in html.data and html.data.count('<tr') == 7
    with patch_adapter(empty=True):
        html = pretty_client.list_repos()
        assert '<table' in html.data and html.data.count('<tr') == 1


def test_list_commits(pretty_client: PrettyPachydermClient):
    with patch_adapter():
        html = pretty_client.list_commits('test_x_pipeline_3')
        assert 'use.fontawesome.com' in html.data
        assert '<table' in html.data and html.data.count('<tr') == 11
    with patch_adapter(empty=True):
        html = pretty_client.list_commits('test_x_pipeline_3')
        assert '<table' in html.data and html.data.count('<tr') == 1


def test_list_files(pretty_client: PrettyPachydermClient):
    with patch_adapter():
        html = pretty_client.list_files('test_x_pipeline_3', files_only=False)
        assert 'use.fontawesome.com' in html.data
        assert '<table' in html.data and html.data.count('<tr') == 8
    with patch_adapter(empty=True):
        html = pretty_client.list_files('test_x_pipeline_3', files_only=False)
        assert '<table' in html.data and html.data.count('<tr') == 1


def test_list_pipelines(pretty_client: PrettyPachydermClient):
    with patch_adapter():
        html = pretty_client.list_pipelines()
        assert 'use.fontawesome.com' in html.data
        assert '<table' in html.data and html.data.count('<tr') == 6
    with patch_adapter(empty=True):
        html = pretty_client.list_pipelines()
        assert '<table' in html.data and html.data.count('<tr') == 1


def test_list_jobs(pretty_client: PrettyPachydermClient):
    with patch_adapter():
        html = pretty_client.list_jobs()
        assert 'use.fontawesome.com' in html.data
        assert '<table' in html.data and html.data.count('<tr') == 9
    with patch_adapter(empty=True):
        html = pretty_client.list_jobs()
        assert '<table' in html.data and html.data.count('<tr') == 1


def test_list_datums(pretty_client: PrettyPachydermClient):
    with patch_adapter():
        html = pretty_client.list_datums('e26ccf65131b4b3d9087cebc2f944279')
        assert 'use.fontawesome.com' in html.data
        assert '<table' in html.data and html.data.count('<tr') == 11
    with patch_adapter(empty=True):
        html = pretty_client.list_datums('e26ccf65131b4b3d9087cebc2f944279')
        assert '<table' in html.data and html.data.count('<tr') == 1


def test_get_logs(pretty_client: PrettyPachydermClient, capsys):
    with patch_adapter():
        pretty_client.get_logs('test_x_pipeline_5', last_job_only=False)
        output = capsys.readouterr().out
        assert output.count('\n') == 25
        assert output.count(' | Job ') == 2
    with patch_adapter(empty=True):
        pretty_client.get_logs('test_x_pipeline_5', last_job_only=False)
        assert output.count('\n') == 25


@patch_adapter()
def test_inspect_pipeline(pretty_client: PrettyPachydermClient, **mocks):
    del mocks
    html = pretty_client.inspect_pipeline('pipeline')
    assert '<pre' in html.data
    assert isinstance(html.raw, dict)


@patch_adapter()
def test_inspect_job(pretty_client: PrettyPachydermClient, **mocks):
    del mocks
    html = pretty_client.inspect_job('abcd1234')
    assert '<pre' in html.data
    assert isinstance(html.raw, dict)


@patch_adapter()
def test_inspect_datum(pretty_client: PrettyPachydermClient, **mocks):
    del mocks
    html = pretty_client.inspect_datum('abcd1234', 'abcdefgh12345678')
    assert '<pre' in html.data
    assert isinstance(html.raw, dict)


def test_pipeline_sort_key():
    from pachypy.pretty import PrettyPachydermClient
    assert PrettyPachydermClient._calc_pipeline_sort_key({'a': [], 'b': ['a']}) == {'a': 'a/0', 'b': 'a/1'}
    assert PrettyPachydermClient._calc_pipeline_sort_key({'b': [], 'a': ['b']}) == {'b': 'a/0', 'a': 'a/1'}
    assert PrettyPachydermClient._calc_pipeline_sort_key({'a': [], 'b': [], 'c': ['a', 'b'], 'd': []}) == {'a': 'a/0', 'b': 'a/0', 'c': 'a/1', 'd': 'd/0'}
    assert PrettyPachydermClient._calc_pipeline_sort_key({'a': [], 'b': ['a'], 'c': ['a'], 'd': ['b']}) == {'a': 'a/0', 'b': 'a/1', 'c': 'a/1', 'd': 'a/2'}


def test_style_job_progress():
    import pandas as pd
    assert not PrettyPachydermClient._style_job_progress(pd.Series(['0% | 0 + 0 / 2']))[0].endswith('0.0%)')
    assert PrettyPachydermClient._style_job_progress(pd.Series(['50% | 1 + 0 / 2']))[0].endswith('50.0%)')
    assert PrettyPachydermClient._style_job_progress(pd.Series(['100% | 1 + 1 / 2']))[0] == ''


def test_format_datetime(pretty_client):
    from datetime import datetime
    from dateutil.relativedelta import relativedelta as rd
    t = datetime.now(pretty_client.user_timezone)
    t = [t.year, t.month, t.day]
    assert pretty_client._format_datetime(None) == ''
    assert pretty_client._format_datetime(datetime(2019, 1, 2, 11, 11, 23)) == '2 Jan 2019 at 11:11'
    assert pretty_client._format_datetime(datetime(2019, 2, 3, 23, 11, 59)) == '3 Feb 2019 at 23:11'
    assert pretty_client._format_datetime(datetime(*t + [11, 11])) == 'Today at 11:11'
    assert pretty_client._format_datetime(datetime(*t + [11, 11]) - rd(days=1)) == 'Yesterday at 11:11'
    assert pretty_client._format_datetime(datetime(*t + [11, 11]) + rd(days=1)) == 'Tomorrow at 11:11'


def test_format_duration():
    assert PrettyPachydermClient._format_duration(None) == ''
    assert PrettyPachydermClient._format_duration(0) == ''
    assert PrettyPachydermClient._format_duration(0.001) == '1 ms'
    assert PrettyPachydermClient._format_duration(1) == '1 sec'
    assert PrettyPachydermClient._format_duration(2) == '2 secs'
    assert PrettyPachydermClient._format_duration(2.1) == '2 secs'
    assert PrettyPachydermClient._format_duration(60) == '1 min'
    assert PrettyPachydermClient._format_duration(120) == '2 mins'
    assert PrettyPachydermClient._format_duration(121) == '2 mins'
    assert PrettyPachydermClient._format_duration(3600) == '1 hour'
    assert PrettyPachydermClient._format_duration(7200) == '2 hours'
    assert PrettyPachydermClient._format_duration(3660) == '1 hour, 1 min'
    assert PrettyPachydermClient._format_duration(7260) == '2 hours, 1 min'
    assert PrettyPachydermClient._format_duration(7320) == '2 hours, 2 mins'
    assert PrettyPachydermClient._format_duration(7322) == '2 hours, 2 mins'
    assert PrettyPachydermClient._format_duration(86400) == '1 day'
    assert PrettyPachydermClient._format_duration(90000) == '1 day, 1 hour'


def test_format_size():
    assert PrettyPachydermClient._format_size(0) == '0 bytes'
    assert PrettyPachydermClient._format_size(1) == '1 byte'
    assert PrettyPachydermClient._format_size(1000) == '1.0 KB'
    assert PrettyPachydermClient._format_size(10100) == '10.1 KB'
    assert PrettyPachydermClient._format_size(1200000) == '1.2 MB'
    assert PrettyPachydermClient._format_size(100300000) == '100.3 MB'
    assert PrettyPachydermClient._format_size(1400000000) == '1.4 GB'
    assert PrettyPachydermClient._format_size(1500000000000) == '1.5 TB'
    assert PrettyPachydermClient._format_size(1600000000000000) == '1.6 PB'
