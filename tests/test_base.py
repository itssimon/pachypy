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
            'cmd': ['/bin/sh', '-c', 'echo "test"']
        },
        'input': {
            'cron': {
                'name': 'tick',
                'spec': '0 * * * *'
            }
        }
    }


@pytest.fixture(scope='module')
def pipeline_spec_3():
    return {
        'pipeline': {'name': 'test_pipeline_3'},
        'transform': {
            'image': 'alpine:latest',
            'cmd': ['/bin/sh', '-c', 'cat /pfs/*/*']
        },
        'input': {
            'union': [{
                'pfs': {
                    'repo': 'test_pipeline_1',
                    'glob': '*'
                }
            }, {
                'pfs': {
                    'repo': 'test_pipeline_2',
                    'glob': '*'
                }
            }]
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
        time.sleep(1)
        pipelines = client_base._list_pipelines()
        state = pipelines.loc[pipelines.pipeline == pipeline_name, 'state'].iloc[0]
    return state


def await_job_completed_state(client_base, pipeline_name, timeout=30):
    start_time = time.time()
    state = 'starting'
    while state in {'unknown', 'starting', 'running'} and time.time() - start_time < timeout:
        time.sleep(1)
        jobs = client_base._list_jobs(pipeline=pipeline_name, n=1)
        if len(jobs):
            state = jobs['state'].iloc[0]
    return state


@mock.patch.dict(os.environ, {'PACHD_ADDRESS': 'test_host:12345'})
def test_init_env():
    from pachypy.base import PachydermClientBase
    client_base = PachydermClientBase()
    channel_target = client_base.pps_client.channel._channel.target().decode()
    assert channel_target == 'test_host:12345'


def test_init():
    from pachypy.base import PachydermClientBase
    client_base = PachydermClientBase(host='test_host')
    channel_target = client_base.pps_client.channel._channel.target().decode()
    assert channel_target == 'test_host:30650'


def test_check_connectivity():
    from pachypy.base import PachydermClientBase
    client_base = PachydermClientBase(host='host_that_does_not_exist')
    assert client_base.check_connectivity() is False
    client_base = PachydermClientBase(host='google.com')
    assert client_base.check_connectivity() is False


def test_list_repos(client_base):
    skip_if_pachyderm_unavailable(client_base)
    df = client_base._list_repos()
    assert df.shape[1] == 4
    assert all([c in df.columns for c in ['repo', 'size_bytes', 'branches', 'created']])


def test_list_pipelines(client_base):
    skip_if_pachyderm_unavailable(client_base)
    df = client_base._list_pipelines()
    assert df.shape[1] == 14
    assert all([c in df.columns for c in [
        'pipeline', 'image', 'cron_spec', 'input', 'input_repos', 'output_branch',
        'parallelism_constant', 'parallelism_coefficient', 'datum_tries',
        'jobs_running', 'jobs_success', 'jobs_failure',
        'created', 'state'
    ]])


def test_list_jobs(client_base):
    skip_if_pachyderm_unavailable(client_base)
    df = client_base._list_jobs()
    assert df.shape[1] == 14
    assert all([c in df.columns for c in [
        'job', 'pipeline', 'state', 'started', 'finished', 'restart',
        'data_processed', 'data_skipped', 'data_total',
        'download_time', 'process_time', 'upload_time',
        'download_bytes', 'upload_bytes'
    ]])


def test_create_update_delete_pipeline(client_base, pipeline_spec_1):
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


def test_stop_start_pipeline(client_base, pipeline_spec_1):
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


def test_create_commit_delete_repo(client_base):
    skip_if_pachyderm_unavailable(client_base)
    repo_name = 'test_repo_1'
    delete_repo_if_exists(client_base, repo_name)

    client_base._create_repo(repo_name)
    assert repo_name in set(client_base._list_repos().repo)

    client_base._commit_timestamp_file(repo_name)
    commits = client_base.pfs_client.list_commit(repo_name)
    assert len(commits) == 1
    assert commits[0].commit.repo.name == repo_name
    assert commits[0].size_bytes == 26

    client_base._delete_repo(repo_name)
    assert repo_name not in set(client_base._list_repos().repo)


def test_list_job_get_logs(client_base, pipeline_spec_2):
    skip_if_pachyderm_unavailable(client_base)
    pipeline_name = pipeline_spec_2['pipeline']['name']
    cron_input_name = pipeline_spec_2['input']['cron']['name']
    delete_pipeline_if_exists(client_base, pipeline_name)

    client_base._create_pipeline(pipeline_spec_2)
    assert await_pipeline_new_state(client_base, pipeline_name, initial_state='starting') == 'running'

    client_base._commit_timestamp_file(pipeline_name + '_' + cron_input_name, overwrite=True)
    assert await_job_completed_state(client_base, pipeline_name) == 'success'

    jobs = client_base._list_jobs(pipeline=pipeline_name)
    assert len(jobs) == 1
    assert (jobs['finished'] - jobs['started']).dt.total_seconds().iloc[0] > 0
    assert jobs['data_processed'].iloc[0] == jobs['data_total'].iloc[0] == 1
    assert jobs['data_skipped'].iloc[0] == 0

    logs = client_base._get_logs(pipeline=pipeline_name)
    logs = logs[logs['user']]
    assert logs.shape == (1, 7)
    assert logs['message'].iloc[0] == 'test'

    client_base._delete_pipeline(pipeline_name)
    assert pipeline_name not in set(client_base._list_pipelines().pipeline)


def test_delete_pipeline_exception(client_base):
    from pachypy.base import PachydermException
    with pytest.raises(PachydermException):
        client_base._delete_pipeline('pipeline_that_does_not_exist')
