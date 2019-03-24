import os
import time
import pytest
from unittest import mock


@pytest.fixture(scope='module')
def client_base():
    from pachypy.base import PachydermClientBase
    return PachydermClientBase()


@pytest.fixture(scope='module')
def pipeline_spec_1():
    return {
        'pipeline': {'name': 'test_pipeline_1'},
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


@pytest.fixture(scope='module')
def pipeline_spec_2():
    return {
        'pipeline': {'name': 'test_pipeline_2'},
        'transform': {
            'image': 'alpine:latest',
            'cmd': ['/bin/sh', '-c', 'cat /pfs/test_pipeline_1/*']
        },
        'input': {
            'pfs': {
                'repo': 'test_pipeline_1',
                'glob': '*'
            }
        }
    }


def skip_if_pachyderm_unavailable(client_base):
    if not client_base.check_connectivity():
        pytest.skip('Pachyderm cluster is not available')


def delete_pipeline_if_exists(client_base, pipeline_name):
    from pachypy.base import PachydermException
    try:
        client_base._delete_pipeline(pipeline_name)
    except PachydermException:
        pass


def delete_repo_if_exists(client_base, repo_name):
    from pachypy.base import PachydermException
    try:
        client_base._delete_repo(repo_name)
    except PachydermException:
        pass


def await_pipeline_new_state(client_base, pipeline_name, initial_state='starting', timeout=30):
    start_time = time.time()
    state = initial_state
    while state == initial_state and time.time() - start_time < timeout:
        time.sleep(2)
        pipelines = client_base._list_pipelines()
        state = pipelines.loc[pipelines.pipeline == pipeline_name, 'state'].iloc[0]
    return state


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


def test_base_list_repos(client_base):
    skip_if_pachyderm_unavailable(client_base)
    df = client_base._list_repos()
    assert df.shape[1] == 4
    assert all([c in df.columns for c in ['repo', 'size_bytes', 'branches', 'created']])


def test_base_list_pipelines(client_base):
    skip_if_pachyderm_unavailable(client_base)
    df = client_base._list_pipelines()
    assert df.shape[1] == 14
    assert all([c in df.columns for c in [
        'pipeline', 'image', 'cron_spec', 'input', 'input_repos', 'output_branch',
        'parallelism_constant', 'parallelism_coefficient', 'datum_tries',
        'jobs_running', 'jobs_success', 'jobs_failure',
        'created', 'state'
    ]])


def test_base_list_jobs(client_base):
    skip_if_pachyderm_unavailable(client_base)
    df = client_base._list_jobs()
    assert df.shape[1] == 14
    assert all([c in df.columns for c in [
        'job', 'pipeline', 'state', 'started', 'finished', 'restart',
        'data_processed', 'data_skipped', 'data_total',
        'download_time', 'process_time', 'upload_time',
        'download_bytes', 'upload_bytes'
    ]])


def test_base_create_update_delete_pipeline(client_base, pipeline_spec_1):
    skip_if_pachyderm_unavailable(client_base)
    pipeline_name = pipeline_spec_1['pipeline']['name']
    delete_pipeline_if_exists(client_base, pipeline_name)

    client_base._create_pipeline(pipeline_spec_1)
    pipelines = client_base._list_pipelines()
    assert len(pipelines) > 0 and pipeline_name in set(pipelines.pipeline)
    assert pipelines.loc[pipelines.pipeline == pipeline_name, 'image'].iloc[0] == pipeline_spec_1['transform']['image']
    assert pipelines.loc[pipelines.pipeline == pipeline_name, 'cron_spec'].iloc[0] == pipeline_spec_1['input']['cron']['spec']

    repos = client_base._list_repos()
    assert len(repos) > 0 and pipeline_name in set(repos.repo)

    pipeline_spec_1['transform']['image'] = 'alpine:edge'
    client_base._update_pipeline(pipeline_spec_1)
    pipelines = client_base._list_pipelines()
    assert pipelines.loc[pipelines.pipeline == pipeline_name, 'image'].iloc[0] == 'alpine:edge'

    client_base._delete_pipeline(pipeline_name)
    assert pipeline_name not in set(client_base._list_pipelines().pipeline)


def test_base_stop_start_pipeline(client_base, pipeline_spec_1):
    skip_if_pachyderm_unavailable(client_base)
    pipeline_name = pipeline_spec_1['pipeline']['name']
    delete_pipeline_if_exists(client_base, pipeline_name)

    client_base._create_pipeline(pipeline_spec_1)
    assert await_pipeline_new_state(client_base, pipeline_name, initial_state='starting') == 'running'

    client_base._stop_pipeline(pipeline_name)
    assert await_pipeline_new_state(client_base, pipeline_name, initial_state='running') == 'paused'

    client_base._start_pipeline(pipeline_name)
    assert await_pipeline_new_state(client_base, pipeline_name, initial_state='paused') == 'running'

    client_base._delete_pipeline(pipeline_name)
    assert pipeline_name not in set(client_base._list_pipelines().pipeline)


def test_base_create_delete_repo(client_base):
    skip_if_pachyderm_unavailable(client_base)
    repo_name = 'test_repo_1'
    delete_repo_if_exists(client_base, repo_name)

    client_base._create_repo(repo_name)
    assert repo_name in set(client_base._list_repos().repo)

    client_base._delete_repo(repo_name)
    assert repo_name not in set(client_base._list_repos().repo)


def test_base_delete_pipeline_exception(client_base):
    from pachypy.base import PachydermException
    with pytest.raises(PachydermException):
        client_base._delete_pipeline('pipeline_that_does_not_exist')
