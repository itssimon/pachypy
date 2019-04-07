import os
import json
import pytest
import pandas as pd
from unittest.mock import patch, MagicMock, PropertyMock, DEFAULT

from pachypy.client import PachydermClient, PachydermClientError


def get_mock_from_csv(file, datetime_cols=None, timedelta_cols=None, json_cols=None):
    file_path = os.path.join(os.path.dirname(__file__), 'mock', file)
    df = pd.read_csv(file_path, parse_dates=datetime_cols, converters={c: json.loads for c in json_cols or []})
    for c in timedelta_cols or []:
        df[c] = pd.to_timedelta(df[c])
    return df


def mock_list_commits(_, repo, n=20):
    df = get_mock_from_csv('list_commits.csv', datetime_cols=['started', 'finished'], json_cols=['branches'])
    return df[df['repo'] == repo].head(n)


def mock_list_branch_heads(_, repo):
    df = mock_list_commits(_, repo, n=1)
    return {df['branches'].iloc[0][0]: df['commit'].iloc[0]}


def mock_list_files(_, repo, **kwargs):
    del kwargs
    df = get_mock_from_csv('list_files.csv', datetime_cols=['committed'], json_cols=['branches'])
    return df[df['repo'] == repo]


def mock_list_jobs(_, pipeline=None, n=20):
    df = get_mock_from_csv('list_jobs.csv', datetime_cols=['started', 'finished'], timedelta_cols=['download_time', 'process_time', 'upload_time'])
    return (df[df['pipeline'] == pipeline] if pipeline is not None else df).head(n)


def mock_list_datums(_, job):
    df = get_mock_from_csv('list_datums.csv', datetime_cols=['committed'])
    return df[df['job'] == job]


def mock_get_logs(_, pipeline=None, **kwargs):
    del kwargs
    df = get_mock_from_csv('get_logs.csv', datetime_cols=['ts'])
    return df[df['pipeline'] == pipeline] if pipeline is not None else df


def mock_get_file(_, repo, path, **kwargs):
    del repo, kwargs
    if 'random' in path:
        yield os.urandom(512)
        yield os.urandom(512)
    else:
        yield b'te'
        yield b'st'


def patch_adapter():
    return patch.multiple(
        'pachypy.adapter.PachydermAdapter',
        list_repos=lambda _: get_mock_from_csv('list_repos.csv', datetime_cols=['created']),
        list_repo_names=lambda _: get_mock_from_csv('list_repos.csv')['repo'].tolist(),
        list_commits=mock_list_commits,
        list_branch_heads=mock_list_branch_heads,
        list_files=mock_list_files,
        list_pipelines=lambda _: get_mock_from_csv('list_pipelines.csv', datetime_cols=['created']),
        list_pipeline_names=lambda _: get_mock_from_csv('list_pipelines.csv')['pipeline'].tolist(),
        list_jobs=mock_list_jobs,
        list_datums=mock_list_datums,
        get_logs=mock_get_logs,
        get_file=mock_get_file,
        create_repo=DEFAULT,
        delete_repo=DEFAULT,
        create_branch=DEFAULT,
        delete_branch=DEFAULT,
        delete_commit=DEFAULT,
        create_pipeline=DEFAULT,
        update_pipeline=DEFAULT,
        delete_pipeline=DEFAULT,
        start_pipeline=DEFAULT,
        stop_pipeline=DEFAULT,
        delete_job=DEFAULT,
        get_pipeline_cron_specs=DEFAULT
    )


def patch_commit_adapter():
    return patch.multiple(
        'pachypy.adapter.PachydermCommitAdapter',
        commit=PropertyMock,
        start=DEFAULT,
        finish=DEFAULT,
        delete=DEFAULT,
        put_file_bytes=DEFAULT,
        put_file_url=DEFAULT,
        delete_file=DEFAULT,
        _list_file_paths=DEFAULT,
    )


@pytest.fixture(scope='module')
def pipeline_spec_files_path():
    return os.path.join(os.path.dirname(__file__), 'pipelines')


@pytest.fixture(scope='function')
def client(pipeline_spec_files_path):
    return PachydermClient(
        pipeline_spec_files=os.path.join(pipeline_spec_files_path, '*.yaml'),
        add_image_digests=False,
        build_images=False
    )


@pytest.fixture(scope='module')
def docker_registry():
    import docker
    client = docker.DockerClient()
    try:
        container = client.containers.get('test_registry')
        if container.status != 'running':
            container.restart()
    except docker.errors.NotFound:
        container = client.containers.run('registry:2', name='test_registry', detach=True, ports={'5000/tcp': 5000})
    yield container
    container.stop()
    container.remove()


def test_init_registry_adapters(client: PachydermClient):
    from pachypy.registry import DockerRegistryAdapter, AmazonECRAdapter
    assert isinstance(client.docker_registry, DockerRegistryAdapter)
    assert isinstance(client.amazon_ecr, AmazonECRAdapter)


@patch_adapter()
def test_list_repos(client: PachydermClient, **mocks):
    del mocks
    df = client.list_repos()
    assert len(df) == 6
    assert df['is_tick'].sum() == 1
    assert client.list_repos('*tick').shape[0] == 1


@patch_adapter()
def test_list_commits(client: PachydermClient, **mocks):
    del mocks
    df = client.list_commits('test_x_pipeline_*')
    assert len(df) == 10
    assert df['size_bytes'].gt(0).all()
    with pytest.raises(PachydermClientError):
        assert client.list_commits('repo_that_doesnt_exist')


@patch_adapter()
def test_list_files(client: PachydermClient, **mocks):
    del mocks
    df = client.list_files('test_x_pipeline_*', files_only=True)
    assert len(df) == 4
    assert df['type'].eq('file').all()
    assert df['size_bytes'].gt(0).all()
    df = client.list_files('test_x_pipeline_3', commit='e1d7e6912d5d4a3289e9fb7c82eec6b5', files_only=False)
    assert len(df) == 7
    assert df['type'].eq('dir').any()
    df = client.list_files('test_x_pipeline_3', branch=None, commit=None, files_only=False)
    assert len(df) == 7
    with pytest.raises(PachydermClientError):
        assert client.list_files('test_x_pipeline_*', commit='e1d7e6912d5d4a3289e9fb7c82eec6b5')
    with pytest.raises(PachydermClientError):
        assert client.list_files('repo_that_doesnt_exist')


@patch_adapter()
def test_list_pipelines(client: PachydermClient, **mocks):
    del mocks
    assert len(client.list_pipelines()) == 5
    assert len(client.list_pipelines('test_x_pipeline_?')) == 5
    assert len(client.list_pipelines('test_x_pipeline_1')) == 1


@patch_adapter()
def test_list_jobs(client: PachydermClient, **mocks):
    del mocks
    df = client.list_jobs()
    assert len(df) == 8
    assert df['duration'].isna().sum() == 0
    assert df['progress'].max() <= 1
    assert len(client.list_jobs(n=5)) == 5
    assert len(client.list_jobs('test_x_pipeline_5')) == 2
    assert len(client.list_jobs('test_x_pipeline_*', n=1)) == 1
    with pytest.raises(PachydermClientError):
        assert client.list_jobs('pipeline_that_doesnt_exist')


@patch_adapter()
def test_list_datums(client: PachydermClient, **mocks):
    del mocks
    df = client.list_datums('e26ccf65131b4b3d9087cebc2f944279')
    assert len(df) == 10
    assert df.index.nunique() == 5
    assert df['repo'].nunique() == 2
    assert df['size_bytes'].gt(0).all()
    assert len(client.list_datums('job_id_with_no_datums')) == 0


@patch_adapter()
def test_get_logs(client: PachydermClient, **mocks):
    del mocks
    assert len(client.get_logs('test_x_pipeline_5')) == 10
    assert len(client.get_logs('test_x_pipeline_5', user_only=True)) == 7
    assert len(client.get_logs('test_x_pipeline_5', last_job_only=False)) == 20
    assert len(client.get_logs('test_x_pipeline_5', user_only=True, last_job_only=False)) == 14
    assert len(client.get_logs('test_x_pipeline_5', datum='e52eea8fb37eafbfa9d04257f41f1e403b156d63b8135cb179cf142b5f1a08d5', last_job_only=False)) == 10
    with pytest.raises(PachydermClientError):
        assert client.get_logs('pipeline_that_doesnt_exist')


@patch_adapter()
def test_create_delete_repos(client: PachydermClient, **mocks):
    del mocks
    list_repo_names = 'pachypy.adapter.PachydermAdapter.list_repo_names'
    repos = ['test_repo_1', 'test_repo_2']
    with patch(list_repo_names, MagicMock(return_value=[])):
        assert client.create_repos(repos[0]) == repos[:1]
        assert client.create_repos(repos) == repos
        assert client.delete_repos('test_repo_*') == []
    with patch(list_repo_names, MagicMock(return_value=repos[1:])):
        assert client.create_repos(repos[1]) == []
        assert client.create_repos(repos) == repos[:1]
        assert client.delete_repos('test_repo_*') == repos[1:]
    with patch(list_repo_names, MagicMock(return_value=repos)):
        assert client.create_repos(repos) == []
        assert client.delete_repos('test_repo_*') == repos
        assert client.delete_repos(repos) == repos


@patch_adapter()
def test_delete_job(client: PachydermClient, **mocks):
    job = 'a1b2c3d4'
    client.delete_job(job)
    mocks['delete_job'].assert_called_once_with(job)


@patch_commit_adapter()
def test_commit_put_files(client: PachydermClient, **mocks):
    mock_file = lambda f: os.path.join(os.path.dirname(__file__), 'mock', f)
    with client.commit('test_repo') as c:
        c.put_file(mock_file('list_commits.csv'))
        mocks['put_file_bytes'].assert_called_once()
        assert mocks['put_file_bytes'].call_args[0][1] == '/list_commits.csv'
        c.put_file(mock_file('list_files.csv'), path='/folder/')
        assert mocks['put_file_bytes'].call_args[0][1] == '/folder/list_files.csv'
        c.put_file(mock_file('list_files.csv'), path='/list_files_2.csv')
        assert mocks['put_file_bytes'].call_args[0][1] == '/list_files_2.csv'
        mocks['put_file_bytes'].reset_mock()

        with pytest.raises(ValueError):
            with open(mock_file('list_commits.csv')) as f:
                c.put_file(f)
        with open(mock_file('list_commits.csv')) as f:
            c.put_file(f, path='list_commits.csv')
        mocks['put_file_bytes'].assert_called_once()
        assert mocks['put_file_bytes'].call_args[0][1] == 'list_commits.csv'
        mocks['put_file_bytes'].reset_mock()

        c.put_files(mock_file('*.csv'))
        assert mocks['put_file_bytes'].call_count == 7


@patch_commit_adapter()
def test_put_timestamp_file(client: PachydermClient, **mocks):
    client.put_timestamp_file('test_repo', overwrite=True)
    mocks['delete_file'].assert_called_once()
    mocks['put_file_bytes'].assert_called_once()
    assert json.loads(mocks['put_file_bytes'].call_args[0][0])
    assert mocks['put_file_bytes'].call_args[0][1] == 'time'


@patch_commit_adapter()
@patch_adapter()
def test_trigger_pipeline(client: PachydermClient, **mocks):
    mocks['get_pipeline_cron_specs'].return_value = []
    with pytest.raises(PachydermClientError):
        client.trigger_pipeline('pipeline_without_cron_input')

    mocks['get_pipeline_cron_specs'].return_value = [{'repo': 'pipeline_tick', 'overwrite': True}]
    client.trigger_pipeline('pipeline')
    mocks['delete_file'].assert_called_once()
    mocks['delete_file'].reset_mock()
    mocks['put_file_bytes'].assert_called_once()

    mocks['delete_file'].reset_mock()
    mocks['put_file_bytes'].reset_mock()

    mocks['get_pipeline_cron_specs'].return_value = [{'repo': 'pipeline_tick', 'overwrite': False}]
    client.trigger_pipeline('pipeline')
    assert not mocks['delete_file'].called
    mocks['put_file_bytes'].assert_called_once()


@patch_adapter()
def test_create_delete_branch_commit(client: PachydermClient, **mocks):
    del mocks
    client.create_branch('test_repo', 'a1b2c4', 'test_branch')
    client.delete_branch('test_repo', 'test_branch')
    client.delete_commit('test_repo', 'a1b2c4')


@patch_commit_adapter()
def test_commit_delete_files(client: PachydermClient, **mocks):
    mocks['_list_file_paths'].return_value = ['/test_file_1', '/test_file_2']
    with client.commit('test_repo') as c:
        c.delete_files('test_file_*')
        assert mocks['delete_file'].call_count == 2


@patch_adapter()
def test_get_file(client: PachydermClient, tmp_path, **mocks):
    del mocks
    repo = 'test_repo'
    file = 'test_file'

    client.get_file(repo, file, destination=tmp_path)
    assert os.path.exists(tmp_path / file) and os.path.getsize(tmp_path / file) == 4

    client.get_file(repo, file, destination=tmp_path / 'renamed_test_file')
    assert os.path.exists(tmp_path / 'renamed_test_file')

    assert client.get_file_content(repo, file) == b'test'
    assert client.get_file_content(repo, file, encoding='auto') == 'test'
    assert client.get_file_content(repo, file, encoding='utf-8') == 'test'

    with pytest.raises(ValueError):
        client.get_file_content(repo, 'random_file', encoding='auto')

    client.get_files('test_x_pipeline_3', destination=tmp_path)
    files = set(tmp_path.glob('**/*.avro'))
    assert len(files) == 4
    assert tmp_path / '1/2019-03-31/1554087629/1554056117887.avro' in files


def test_read_pipeline_specs(client: PachydermClient, pipeline_spec_files_path):
    def custom_transform_pipeline_spec(pipeline_spec):
        pipeline_spec['test'] = True
        return pipeline_spec
    client.pipeline_spec_transformer = custom_transform_pipeline_spec
    assert len(client.read_pipeline_specs('test*')) == 7
    assert len(client.read_pipeline_specs('not_a_test*')) == 0
    pipeline_specs = client.read_pipeline_specs('test_a*')
    assert isinstance(pipeline_specs, list) and len(pipeline_specs) == 2
    assert pipeline_specs[0]['pipeline']['name'] == 'test_a_pipeline_1'
    assert pipeline_specs[0]['transform']['image'] == pipeline_specs[1]['transform']['image']
    assert pipeline_specs[0]['test'] is True

    client.pipeline_spec_files = os.path.join(pipeline_spec_files_path, 'test_c.json')  # type: ignore
    assert len(client.read_pipeline_specs('*')) == 1

    with pytest.raises(ValueError):
        client.pipeline_spec_files = os.path.join(pipeline_spec_files_path, 'test_d.json')  # type: ignore
        client.read_pipeline_specs('*')


def test_build_push_image(client: PachydermClient, docker_registry):
    del docker_registry
    pipeline_specs = client.read_pipeline_specs('test_e_*')
    for pipeline_spec in pipeline_specs:
        client._build_image(pipeline_spec)
        image = pipeline_spec['transform']['image']
        assert image in client._built_images
        assert image in client._image_digests
        assert client._image_digests[image].startswith('sha')
        assert client.docker_registry.get_image_digest(image) == client._image_digests[image]
    client.clear_cache()
    assert len(client._built_images) == len(client._image_digests) == 0


@patch_adapter()
def test_create_update_delete_pipelines(client: PachydermClient, **mocks):
    del mocks
    list_pipeline_names = 'pachypy.adapter.PachydermAdapter.list_pipeline_names'
    pipelines = ['test_a_pipeline_1', 'test_a_pipeline_2']
    with patch(list_pipeline_names, MagicMock(return_value=[])):
        assert client.create_pipelines('test_a*') == (pipelines, [], [])
    with patch(list_pipeline_names, MagicMock(return_value=pipelines)):
        assert client.create_pipelines('test_a*') == ([], [], [])
        assert client.update_pipelines('test_a*') == ([], pipelines, [])
        assert client.update_pipelines('test_a*', recreate=True) == (pipelines, [], pipelines[::-1])
        assert client.delete_pipelines('test_a*') == pipelines[::-1]


@patch_adapter()
def test_stop_start_pipelines(client: PachydermClient, **mocks):
    del mocks
    list_pipeline_names = 'pachypy.adapter.PachydermAdapter.list_pipeline_names'
    pipelines = ['test_a_pipeline_1', 'test_a_pipeline_2']
    with patch(list_pipeline_names, MagicMock(return_value=pipelines)):
        assert client.stop_pipelines('test_a*') == pipelines
        assert client.start_pipelines('test_a*') == pipelines


def test_add_image_digest(client: PachydermClient):
    digest = 'sha1:123'
    tag = 'tag'
    with patch('pachypy.registry.DockerRegistryAdapter.get_image_digest', MagicMock(return_value=digest)) as mock:
        repo = 'user/repo'
        assert client._add_image_digest(f'{repo}:{tag}') == f'{repo}:{tag}@{digest}'
        assert client._add_image_digest(f'{repo}:{tag}') == f'{repo}:{tag}@{digest}'
        assert client._add_image_digest(f'{repo}:{tag}@sha1:000') == f'{repo}:{tag}@sha1:000'
        mock.assert_called_once_with(f'{repo}:{tag}')
    with patch('pachypy.registry.AmazonECRAdapter.get_image_digest', MagicMock(return_value=digest)) as mock:
        repo = '112233445566.dkr.ecr.xxx.amazonaws.com/repo'
        assert client._add_image_digest(f'{repo}:{tag}') == f'{repo}:{tag}@{digest}'
        mock.assert_called_once_with(f'{repo}:{tag}')
    with patch('pachypy.registry.DockerRegistryAdapter.get_image_digest', MagicMock(return_value=None)) as mock:
        for image in ['user/repo2:tag', 'user/repo2:tag@sha1:000']:
            assert client._add_image_digest(image) == image


def test_wildcard_filter():
    from pachypy.client import _wildcard_filter, _wildcard_match
    x = ['a', 'ab', 'b']
    assert _wildcard_match(x, '*') is True
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


def test_expand_files():
    from pathlib import Path
    from pachypy.client import _expand_files
    mock_dir = lambda glob: os.path.join(os.path.dirname(__file__), 'mock', glob)
    assert len(_expand_files(None)) == 0
    assert len(_expand_files(mock_dir('*.csv'))) == 7
    assert len(_expand_files(Path(mock_dir('*.csv')))) == 7
    assert len(_expand_files([mock_dir('list_*.csv'), Path(mock_dir('get_*.csv'))])) == 7
