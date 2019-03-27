import os
import time
import pytest
from unittest import mock


@pytest.fixture(scope='module')
def adapter():
    from pachypy.adapter import PachydermAdapter
    return PachydermAdapter()


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
                'cron': {
                    'name': 'tick',
                    'spec': '0 * * * *'
                }
            }, {
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


@pytest.fixture(scope='module')
def pipeline_spec_4():
    return {
        'pipeline': {'name': 'test_pipeline_4'},
        'transform': {
            'image': 'alpine:latest',
            'cmd': ['/bin/sh', '-c', 'cat /pfs/*/*']
        },
        'input': {
            'cross': [{
                'atom': {
                    'repo': 'test_pipeline_1',
                    'branch': 'test',
                    'glob': '*'
                }
            }, {
                'atom': {
                    'repo': 'test_pipeline_2',
                    'branch': 'test',
                    'glob': '*'
                }
            }]
        }
    }


def skip_if_pachyderm_unavailable(adapter):
    if not adapter.check_connectivity():
        pytest.skip('Pachyderm cluster is not available')


def delete_pipeline_if_exists(adapter, pipeline_name):
    from pachypy.adapter import PachydermException
    try:
        adapter.delete_pipeline(pipeline_name)
    except PachydermException:
        pass


def delete_repo_if_exists(adapter, repo_name):
    from pachypy.adapter import PachydermException
    try:
        adapter.delete_repo(repo_name)
    except PachydermException:
        pass


def await_pipeline_new_state(adapter, pipeline_name, initial_state='starting', timeout=30):
    start_time = time.time()
    state = initial_state
    while state == initial_state and time.time() - start_time < timeout:
        time.sleep(1)
        pipelines = adapter.list_pipelines()
        state = pipelines.loc[pipelines.pipeline == pipeline_name, 'state'].iloc[0]
    return state


def await_job_completed_state(adapter, pipeline_name, timeout=300):
    start_time = time.time()
    state = 'starting'
    while state in {'unknown', 'starting', 'running'} and time.time() - start_time < timeout:
        time.sleep(3)
        jobs = adapter.list_jobs(pipeline=pipeline_name, n=1)
        if len(jobs):
            state = jobs['state'].iloc[0]
    return state


def test_init():
    from pachypy.adapter import PachydermAdapter
    adapter = PachydermAdapter(host='test_host')
    assert adapter.host == 'test_host' and adapter.port == 30650
    with mock.patch.dict(os.environ, {'PACHD_ADDRESS': 'test_host:12345'}):
        adapter = PachydermAdapter()
        assert adapter.host == 'test_host' and adapter.port == 12345


def test_check_connectivity():
    from pachypy.adapter import PachydermAdapter
    adapter = PachydermAdapter(host='host_that_does_not_exist')
    assert adapter.check_connectivity() is False
    adapter = PachydermAdapter(host='google.com')
    assert adapter.check_connectivity() is False


def test_list_repos(adapter):
    skip_if_pachyderm_unavailable(adapter)
    df = adapter.list_repos()
    assert df.shape[1] == 4
    assert all([c in df.columns for c in ['repo', 'size_bytes', 'branches', 'created']])


def test_list_pipelines(adapter, pipeline_spec_1, pipeline_spec_2, pipeline_spec_3, pipeline_spec_4):
    skip_if_pachyderm_unavailable(adapter)
    pipeline_specs = [pipeline_spec_1, pipeline_spec_2, pipeline_spec_3, pipeline_spec_4]
    for pipeline_spec in pipeline_specs:
        delete_pipeline_if_exists(adapter, pipeline_spec['pipeline']['name'])
        adapter.create_pipeline(pipeline_spec)
    df = adapter.list_pipelines()
    for pipeline_spec in pipeline_specs[::-1]:
        delete_pipeline_if_exists(adapter, pipeline_spec['pipeline']['name'])
    assert df.shape[0] >= 4
    assert df.shape[1] == 15
    assert all([c in df.columns for c in [
        'pipeline', 'image', 'cron_spec', 'input', 'input_repos', 'output_branch',
        'parallelism_constant', 'parallelism_coefficient', 'datum_tries', 'max_queue_size',
        'jobs_running', 'jobs_success', 'jobs_failure',
        'created', 'state'
    ]])
    assert set(df.loc[df['pipeline'] == 'test_pipeline_3', 'input_repos'].iloc[0]) == {'test_pipeline_1', 'test_pipeline_2'}
    assert df.loc[df['pipeline'] == 'test_pipeline_3', 'input'].iloc[0] == '(tick ∪ test_pipeline_1:* ∪ test_pipeline_2:*)'
    assert df.loc[df['pipeline'] == 'test_pipeline_3', 'cron_spec'].iloc[0] == '0 * * * *'
    assert set(df.loc[df['pipeline'] == 'test_pipeline_4', 'input_repos'].iloc[0]) == {'test_pipeline_1', 'test_pipeline_2'}
    assert df.loc[df['pipeline'] == 'test_pipeline_4', 'input'].iloc[0] == '(test_pipeline_1/test:* ⨯ test_pipeline_2/test:*)'


def test_list_jobs(adapter):
    skip_if_pachyderm_unavailable(adapter)
    df = adapter.list_jobs()
    assert df.shape[1] == 15
    assert all([c in df.columns for c in [
        'job', 'pipeline', 'state', 'started', 'finished', 'restart',
        'data_processed', 'data_skipped', 'data_total',
        'download_time', 'process_time', 'upload_time',
        'download_bytes', 'upload_bytes', 'output_commit'
    ]])


def test_create_update_delete_pipeline(adapter, pipeline_spec_1):
    skip_if_pachyderm_unavailable(adapter)
    pipeline_name = pipeline_spec_1['pipeline']['name']
    delete_pipeline_if_exists(adapter, pipeline_name)

    adapter.create_pipeline(pipeline_spec_1)
    pipelines = adapter.list_pipelines()
    pipeline_names = adapter.list_pipeline_names()
    assert len(pipelines) == len(pipeline_names)
    assert pipeline_name in set(pipeline_names)
    assert pipeline_name in set(pipelines.pipeline)
    assert pipelines.loc[pipelines.pipeline == pipeline_name, 'image'].iloc[0] == pipeline_spec_1['transform']['image']
    assert pipelines.loc[pipelines.pipeline == pipeline_name, 'cron_spec'].iloc[0] == pipeline_spec_1['input']['cron']['spec']

    repos = adapter.list_repos()
    assert len(repos) > 0 and pipeline_name in set(repos.repo)

    pipeline_spec_1['transform']['image'] = 'alpine:edge'
    adapter.update_pipeline(pipeline_spec_1)
    pipelines = adapter.list_pipelines()
    assert pipelines.loc[pipelines.pipeline == pipeline_name, 'image'].iloc[0] == 'alpine:edge'

    adapter.delete_pipeline(pipeline_name)
    assert pipeline_name not in adapter.list_pipeline_names()


def test_stop_start_pipeline(adapter, pipeline_spec_1):
    skip_if_pachyderm_unavailable(adapter)
    pipeline_name = pipeline_spec_1['pipeline']['name']
    delete_pipeline_if_exists(adapter, pipeline_name)

    adapter.create_pipeline(pipeline_spec_1)
    assert await_pipeline_new_state(adapter, pipeline_name, initial_state='starting') == 'running'

    adapter.stop_pipeline(pipeline_name)
    assert await_pipeline_new_state(adapter, pipeline_name, initial_state='running') == 'paused'

    adapter.start_pipeline(pipeline_name)
    assert await_pipeline_new_state(adapter, pipeline_name, initial_state='paused') == 'running'

    adapter.delete_pipeline(pipeline_name)
    assert pipeline_name not in set(adapter.list_pipelines().pipeline)


def test_create_commit_delete_repo(adapter):
    skip_if_pachyderm_unavailable(adapter)
    repo_name = 'test_repo_1'
    delete_repo_if_exists(adapter, repo_name)

    adapter.create_repo(repo_name)
    assert repo_name in set(adapter.list_repos().repo)

    adapter.commit_timestamp_file(repo_name)
    commits = adapter.pfs_client.list_commit(repo_name)
    assert len(commits) == 1
    assert commits[0].commit.repo.name == repo_name
    assert commits[0].size_bytes == 26

    adapter.delete_repo(repo_name)
    assert repo_name not in set(adapter.list_repos().repo)


def test_list_job_get_logs(adapter, pipeline_spec_2):
    skip_if_pachyderm_unavailable(adapter)
    pipeline_name = pipeline_spec_2['pipeline']['name']
    cron_input_name = pipeline_spec_2['input']['cron']['name']
    delete_pipeline_if_exists(adapter, pipeline_name)

    adapter.create_pipeline(pipeline_spec_2)
    assert await_pipeline_new_state(adapter, pipeline_name, initial_state='starting') == 'running'

    adapter.commit_timestamp_file(pipeline_name + '_' + cron_input_name, overwrite=True)
    assert await_job_completed_state(adapter, pipeline_name) == 'success'

    jobs = adapter.list_jobs(pipeline=pipeline_name)
    assert len(jobs) == 1
    assert (jobs['finished'] - jobs['started']).dt.total_seconds().round().iloc[0] > -10
    assert jobs['data_processed'].iloc[0] == jobs['data_total'].iloc[0] == 1
    assert jobs['data_skipped'].iloc[0] == 0

    logs = adapter.get_logs(pipeline=pipeline_name)
    logs = logs[logs['user']]
    assert logs.shape == (1, 7)
    assert logs['message'].iloc[0] == 'test'

    adapter.delete_pipeline(pipeline_name)
    assert pipeline_name not in set(adapter.list_pipelines().pipeline)


def test_delete_pipeline_exception(adapter):
    from pachypy.adapter import PachydermException
    with pytest.raises(PachydermException):
        adapter.delete_pipeline('pipeline_that_does_not_exist')
