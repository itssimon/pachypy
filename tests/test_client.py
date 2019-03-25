import os
import json
import pytest
import pandas as pd
from unittest.mock import patch, MagicMock, DEFAULT


def get_mock_from_csv(file, datetime_cols=[], timedelta_cols=[], json_cols=[]):
    file_path = os.path.join(os.path.dirname(__file__), 'mock', file)
    df = pd.read_csv(file_path, parse_dates=datetime_cols, converters={c: json.loads for c in json_cols})
    for c in timedelta_cols:
        df[c] = pd.to_timedelta(df[c])
    return df


def mock_list_jobs(_, pipeline=None, n=20):
    df = get_mock_from_csv('list_jobs.csv', datetime_cols=['started', 'finished'], timedelta_cols=['download_time', 'process_time', 'upload_time'])
    return (df[df['pipeline'] == pipeline] if pipeline is not None else df).head(n)


def mock_get_logs(_, pipeline=None):
    df = get_mock_from_csv('get_logs.csv', datetime_cols=['ts'])
    return df[df['pipeline'] == pipeline] if pipeline is not None else df


def patch_adapter():
    return patch.multiple(
        'pachypy.adapter.PachydermClientAdapter',
        _list_repos=lambda _: get_mock_from_csv('list_repos.csv', datetime_cols=['created']),
        _list_pipelines=lambda _: get_mock_from_csv('list_pipelines.csv', datetime_cols=['created']),
        _list_pipeline_names=lambda _: get_mock_from_csv('list_pipelines.csv')['pipeline'].tolist(),
        _list_jobs=mock_list_jobs,
        _get_logs=mock_get_logs,
        _create_pipeline=DEFAULT,
        _update_pipeline=DEFAULT,
        _delete_pipeline=DEFAULT,
        _start_pipeline=DEFAULT,
        _stop_pipeline=DEFAULT,
    )


@pytest.fixture(scope='module')
def pipeline_spec_files_path():
    return os.path.join(os.path.dirname(__file__), 'pipelines')


@pytest.fixture(scope='function')
def client(pipeline_spec_files_path):
    from pachypy.client import PachydermClient
    return PachydermClient(
        pipeline_spec_files=os.path.join(pipeline_spec_files_path, '*.yaml'),
        update_image_digests=False
    )


def test_init():
    from pachypy.client import PachydermClient
    from pachypy.registry import DockerRegistry, AmazonECRRegistry
    assert isinstance(PachydermClient(container_registry='docker').container_registry, DockerRegistry)
    assert isinstance(PachydermClient(container_registry='ecr').container_registry, AmazonECRRegistry)
    assert isinstance(PachydermClient(container_registry='registry.example.com').container_registry, DockerRegistry)
    assert PachydermClient(container_registry=None).container_registry is None


@patch_adapter()
def test_list_repos(client, **kwargs):
    df = client.list_repos()
    assert len(df) == 6
    assert df['is_tick'].sum() == 1
    assert client.list_repos('*tick').shape[0] == 1


@patch_adapter()
def test_list_pipelines(client, **kwargs):
    assert len(client.list_pipelines()) == 5
    assert len(client.list_pipelines('test_x_pipeline_?')) == 5
    assert len(client.list_pipelines('test_x_pipeline_1')) == 1


@patch_adapter()
def test_list_jobs(client, **kwargs):
    df = client.list_jobs()
    assert len(df) == 8
    assert df['duration'].isna().sum() == 0
    assert df['progress'].max() <= 1
    assert len(client.list_jobs(n=5)) == 5
    assert len(client.list_jobs('test_x_pipeline_5')) == 2
    assert len(client.list_jobs('test_x_pipeline_*', n=1)) == 1


@patch_adapter()
def test_get_logs(client, **kwargs):
    assert len(client.get_logs('test_x_pipeline_5')) == 10
    assert len(client.get_logs('test_x_pipeline_5', user_only=True)) == 7
    assert len(client.get_logs('test_x_pipeline_5', last_job_only=False)) == 20
    assert len(client.get_logs('test_x_pipeline_5', user_only=True, last_job_only=False)) == 14


def test_pipeline_spec_files(client, pipeline_spec_files_path):
    assert len(client.pipeline_spec_files) == 2
    client.pipeline_spec_files = [os.path.join(pipeline_spec_files_path, 'test_a.yaml')]
    assert len(client.pipeline_spec_files) == 1
    client.pipeline_spec_files = [os.path.join(pipeline_spec_files_path, 'te*.yaml'), os.path.join(pipeline_spec_files_path, 't*.yaml')]
    assert len(client.pipeline_spec_files) == 2
    client.pipeline_spec_files = None
    assert len(client.pipeline_spec_files) == 0


def test_read_pipeline_specs(client):
    def custom_transform_pipeline_spec(pipeline_spec):
        pipeline_spec['test'] = True
        return pipeline_spec
    client.pipeline_spec_transformer = custom_transform_pipeline_spec
    assert len(client.read_pipeline_specs('test*')) == 4
    pipeline_specs = client.read_pipeline_specs('test_a*')
    assert isinstance(pipeline_specs, list) and len(pipeline_specs) == 2
    assert pipeline_specs[0]['pipeline']['name'] == 'test_a_pipeline_1'
    assert pipeline_specs[0]['test'] is True


@patch_adapter()
def test_create_update_delete_pipelines(client, **kwargs):
    _list_pipeline_names = 'pachypy.adapter.PachydermClientAdapter._list_pipeline_names'
    pipelines = ['test_a_pipeline_1', 'test_a_pipeline_2']
    with patch(_list_pipeline_names, MagicMock(return_value=[])):
        assert client.create_pipelines('test_a*') == (pipelines, [])
    with patch(_list_pipeline_names, MagicMock(return_value=pipelines)):
        assert client.update_pipelines('test_a*') == (pipelines, [], [])
        assert client.update_pipelines('test_a*', recreate=True) == ([], pipelines, pipelines[::-1])
        assert client.delete_pipelines('test_a*') == pipelines[::-1]


@patch_adapter()
def test_stop_start_pipelines(client, **kwargs):
    _list_pipeline_names = 'pachypy.adapter.PachydermClientAdapter._list_pipeline_names'
    pipelines = ['test_a_pipeline_1', 'test_a_pipeline_2']
    with patch(_list_pipeline_names, MagicMock(return_value=pipelines)):
        assert client.stop_pipelines('test_a*') == pipelines
        assert client.start_pipelines('test_a*') == pipelines


def test_split_image_string():
    from pachypy.client import _split_image_string
    assert _split_image_string('a.b/c:d@sha1:e') == ('a.b/c', 'd', 'sha1:e')
    assert _split_image_string('a.b/c:d') == ('a.b/c', 'd', None)
    assert _split_image_string('a.b/c') == ('a.b/c', None, None)


def test_wildcard_filter():
    from pachypy.client import _wildcard_filter
    x = ['a', 'ab', 'b']
    assert _wildcard_filter(x, '*') == x
    assert _wildcard_filter(x, None) == x
    assert _wildcard_filter(x, 'a') == ['a']
    assert _wildcard_filter(x, ['a']) == ['a']
    assert _wildcard_filter(x, [['a']]) == ['a']
    assert _wildcard_filter(x, 'a*') == ['a', 'ab']
    assert _wildcard_filter(x, 'a?') == ['ab']
    assert _wildcard_filter(x, [['a*'], 'b']) == x
    assert _wildcard_filter(x, ['*a', '*b']) == x
    assert _wildcard_filter(x, ['a', 'b']) == ['a', 'b']
