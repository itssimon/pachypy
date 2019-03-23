import os
import pytest
from unittest import mock


@pytest.fixture(scope='module')
def client_base():
    from pachypy.base import PachydermClientBase
    return PachydermClientBase()


@mock.patch.dict(os.environ, {'PACHD_ADDRESS': 'test_host:12345'})
def test_base_init_env():
    from pachypy.base import PachydermClientBase
    client_base = PachydermClientBase()
    channel_target = client_base.pps_client.channel._channel.target().decode()
    assert channel_target == 'test_host:12345'


def test_base_init():
    from pachypy.base import PachydermClientBase
    client_base = PachydermClientBase(host='test_host')
    channel_target = client_base.pps_client.channel._channel.target().decode()
    assert channel_target == 'test_host:30650'


def test_base_check_connectivity():
    from pachypy.base import PachydermClientBase
    client_base = PachydermClientBase(host='host_that_does_not_exist')
    assert client_base.check_connectivity() is False
    client_base = PachydermClientBase(host='google.com')
    assert client_base.check_connectivity() is False


def test_pachyderm_available(client_base):
    if not client_base.check_connectivity():
        pytest.skip('Pachyderm cluster is not available')
    assert True


@pytest.mark.dependency(depends=['test_pachyderm_available'])
def test_base_list_repos(client_base):
    df = client_base._list_repos()
    assert df.shape[1] == 4
    assert all([c in df.columns for c in ['repo', 'size_bytes', 'branches', 'created']])


@pytest.mark.dependency(depends=['test_pachyderm_available'])
def test_base_list_pipelines(client_base):
    df = client_base._list_pipelines()
    assert df.shape[1] == 14
    assert all([c in df.columns for c in [
        'pipeline', 'image', 'cron_spec', 'input', 'input_repos', 'output_branch',
        'parallelism_constant', 'parallelism_coefficient', 'datum_tries',
        'jobs_running', 'jobs_success', 'jobs_failure',
        'created', 'state'
    ]])


@pytest.mark.dependency(depends=['test_pachyderm_available'])
def test_base_list_jobs(client_base):
    df = client_base._list_jobs()
    assert df.shape[1] == 14
    assert all([c in df.columns for c in [
        'job', 'pipeline', 'state', 'started', 'finished', 'restart',
        'data_processed', 'data_skipped', 'data_total',
        'download_time', 'process_time', 'upload_time',
        'download_bytes', 'upload_bytes'
    ]])


@pytest.mark.dependency(depends=['test_pachyderm_available'])
def test_base_create_update_delete_pipeline(client_base):
    try:
        client_base._delete_pipeline('test_pipeline')
    except:
        pass
    pipeline_spec = {
        'pipeline': {'name': 'test_pipeline'},
        'transform': {
            'image': 'alpine:latest',
            'cmd': ['/bin/sh', '-c', 'date > /pfs/out/date']
        },
        'input': {
            'cron': {
                'name': 'tick',
                'spec': '0 * * * *'
            }
        }
    }
    client_base._create_pipeline(pipeline_spec)
    pipelines = client_base._list_pipelines()
    assert len(pipelines) > 0 and 'test_pipeline' in set(pipelines.pipeline)
    assert pipelines.loc[pipelines.pipeline == 'test_pipeline', 'image'].iloc[0] == 'alpine:latest'
    assert pipelines.loc[pipelines.pipeline == 'test_pipeline', 'cron_input'].iloc[0]

    repos = client_base._list_repos()
    assert len(repos) > 0 and 'test_pipeline' in set(repos.repo)

    pipeline_spec['transform']['image'] = 'alpine:edge'
    client_base._update_pipeline(pipeline_spec)
    pipelines = client_base._list_pipelines()
    assert pipelines.loc[pipelines.pipeline == 'test_pipeline', 'image'].iloc[0] == 'alpine:edge'

    client_base._delete_pipeline('test_pipeline')
    pipelines = client_base._list_pipelines()
    assert len(pipelines) == 0 or 'test_pipeline' not in set(pipelines.pipeline)


def test_base_delete_pipeline_exception(client_base):
    from pachypy.base import PachydermException
    with pytest.raises(PachydermException):
        client_base._delete_pipeline('pipeline_that_does_not_exist')
