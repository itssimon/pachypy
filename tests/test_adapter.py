import os
import time
import random
import pytest
from unittest import mock

from pachypy.adapter import PachydermAdapter, PachydermCommitAdapter, PachydermError


@pytest.fixture(scope='module')
def adapter() -> PachydermAdapter:
    return PachydermAdapter('localhost', 30650)


@pytest.fixture(scope='function')
def repo(adapter: PachydermAdapter):
    repo_name = 'test_repo_{:05x}'.format(random.randrange(16**5))
    delete_repo_if_exists(adapter, repo_name)
    adapter.create_repo(repo_name)
    yield repo_name
    delete_repo_if_exists(adapter, repo_name)


@pytest.fixture(scope='function')
def pipeline_1(adapter: PachydermAdapter):
    yield from pipeline(adapter, {
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
    })


@pytest.fixture(scope='function')
def pipeline_2(adapter: PachydermAdapter):
    yield from pipeline(adapter, {
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
    })


@pytest.fixture(scope='function')
def pipeline_3(adapter: PachydermAdapter):
    yield from pipeline(adapter, {
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
    })


@pytest.fixture(scope='function')
def pipeline_4(adapter: PachydermAdapter):
    yield from pipeline(adapter, {
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
    })


@pytest.fixture(scope='function')
def pipeline_5(adapter: PachydermAdapter, repo):
    yield from pipeline(adapter, {
        'pipeline': {'name': 'test_pipeline_5'},
        'transform': {
            'image': 'alpine:latest',
            'cmd': ['/bin/sh', '-c', f"sleep 5s && cat /pfs/{repo}/*" + " | wc -w | awk '{$1=$1};1' > /pfs/out/cnt"]
        },
        'input': {
            'pfs': {
                'repo': repo,
                'glob': '/'
            }
        }
    })


@pytest.fixture(scope='function')
def pipeline_6(adapter: PachydermAdapter):
    yield from pipeline(adapter, {
        'pipeline': {'name': 'test_pipeline_6'},
        'transform': {
            'image': 'alpine:latest',
            'cmd': ['/bin/sh', '-c', 'cat /pfs/*/*']
        },
        'input': {
            'union': [{
                'cron': {
                    'name': 'tick1',
                    'spec': '0 * * * *'
                }
            }, {
                'cron': {
                    'name': 'tick2',
                    'spec': '30 * * * *'
                }
            }]
        }
    })


def skip_if_pachyderm_unavailable(adapter: PachydermAdapter):
    if adapter._connectable is None:
        adapter.check_connectivity()
    if not adapter._connectable:
        pytest.skip('Pachyderm cluster is not available')


def delete_pipeline_if_exists(adapter: PachydermAdapter, pipeline_name):
    try:
        adapter.delete_pipeline(pipeline_name)
    except PachydermError:
        pass


def delete_repo_if_exists(adapter: PachydermAdapter, repo_name):
    try:
        adapter.delete_repo(repo_name)
    except PachydermError:
        pass


def pipeline(adapter: PachydermAdapter, pipeline_spec):
    pipeline_name = pipeline_spec['pipeline']['name']
    delete_pipeline_if_exists(adapter, pipeline_name)
    adapter.create_pipeline(pipeline_spec)
    yield pipeline_spec
    delete_pipeline_if_exists(adapter, pipeline_name)


def await_pipeline_new_state(adapter: PachydermAdapter, pipeline_name, initial_state='starting', timeout=30):
    start_time = time.time()
    state = initial_state
    while state == initial_state and time.time() - start_time < timeout:
        time.sleep(1)
        pipelines = adapter.list_pipelines()
        state = pipelines.loc[pipelines.pipeline == pipeline_name, 'state'].iloc[0]
    return state


def await_job_completed_state(adapter: PachydermAdapter, pipeline_name, timeout=300):
    start_time = time.time()
    state = 'starting'
    while state in {'unknown', 'starting', 'running'} and time.time() - start_time < timeout:
        time.sleep(3)
        jobs = adapter.list_jobs(pipeline=pipeline_name, n=1)
        if len(jobs):
            state = jobs['state'].iloc[0]
    return state


def test_init(monkeypatch):
    monkeypatch.delenv('PACHD_ADDRESS', raising=False)
    adapter = PachydermAdapter()
    assert adapter.host == 'localhost' and adapter.port == 30650
    adapter = PachydermAdapter(host='test_host')
    assert adapter.host == 'test_host' and adapter.port == 30650
    with mock.patch.dict(os.environ, {'PACHD_ADDRESS': 'test_host:12345'}):
        adapter = PachydermAdapter()
        assert adapter.host == 'test_host' and adapter.port == 12345
    with mock.patch.dict(os.environ, {'PACHD_ADDRESS': 'another_test_host'}):
        adapter = PachydermAdapter()
        assert adapter.host == 'another_test_host' and adapter.port == 30650


def test_check_connectivity():
    adapter = PachydermAdapter(host='host_that_does_not_exist')
    assert adapter.check_connectivity() is False
    adapter = PachydermAdapter(host='google.com')
    assert adapter.check_connectivity() is False


def test_get_version(adapter: PachydermAdapter):
    version = adapter.get_version()
    assert len(version.split('.')) == 3


def test_create_delete_repo(adapter: PachydermAdapter):
    skip_if_pachyderm_unavailable(adapter)
    repo_name = 'test_repo_a1b2c'
    delete_repo_if_exists(adapter, repo_name)

    adapter.create_repo(repo_name)
    assert repo_name in set(adapter.list_repo_names())
    assert repo_name in set(adapter.list_repos().repo)

    adapter.delete_repo(repo_name)
    assert repo_name not in set(adapter.list_repo_names())


def test_create_update_delete_pipeline(adapter, pipeline_1):
    skip_if_pachyderm_unavailable(adapter)
    pipeline_name = pipeline_1['pipeline']['name']

    pipelines = adapter.list_pipelines()
    pipeline_names = adapter.list_pipeline_names()
    assert len(pipelines) == len(pipeline_names)
    assert pipeline_name in set(pipeline_names)
    assert pipeline_name in set(pipelines.pipeline)
    assert pipelines.loc[pipelines.pipeline == pipeline_name, 'image'].iloc[0] == pipeline_1['transform']['image']
    assert pipelines.loc[pipelines.pipeline == pipeline_name, 'cron_spec'].iloc[0] == pipeline_1['input']['cron']['spec']
    assert pipeline_name in set(adapter.list_repo_names())

    pipeline_1['transform']['image'] = 'alpine:edge'
    adapter.update_pipeline(pipeline_1)
    pipelines = adapter.list_pipelines()
    assert pipelines.loc[pipelines.pipeline == pipeline_name, 'image'].iloc[0] == 'alpine:edge'

    adapter.delete_pipeline(pipeline_name)
    assert pipeline_name not in adapter.list_pipeline_names()

    with pytest.raises(PachydermError):
        adapter.delete_pipeline('pipeline_that_does_not_exist')


def test_list_pipelines(adapter: PachydermAdapter, pipeline_1, pipeline_2, pipeline_3, pipeline_4):
    skip_if_pachyderm_unavailable(adapter)
    df = adapter.list_pipelines()
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


def test_stop_start_pipeline(adapter: PachydermAdapter, pipeline_1):
    skip_if_pachyderm_unavailable(adapter)

    pipeline_name = pipeline_1['pipeline']['name']
    assert await_pipeline_new_state(adapter, pipeline_name, initial_state='starting') == 'running'

    adapter.stop_pipeline(pipeline_name)
    assert await_pipeline_new_state(adapter, pipeline_name, initial_state='running') == 'paused'

    adapter.start_pipeline(pipeline_name)
    assert await_pipeline_new_state(adapter, pipeline_name, initial_state='paused') == 'running'


def test_invert_dict():
    from pachypy.adapter import _invert_dict
    assert _invert_dict({'a': '1'}) == {'1': ['a']}
    assert _invert_dict({'a': '1', 'b': '1'}) == {'1': ['a', 'b']}
    assert _invert_dict({'a': '1', 'b': '2'}) == {'1': ['a'], '2': ['b']}


def test_commit_context_manager(adapter: PachydermAdapter, repo):
    skip_if_pachyderm_unavailable(adapter)

    c = PachydermCommitAdapter(adapter, repo)
    assert c.commit is None and c.finished is False
    assert len(adapter.list_commits(repo)) == 0
    with c:
        pass
    assert c.commit is not None and c.finished is True
    assert len(adapter.list_commits(repo)) == 1
    assert adapter.list_branch_heads(repo)['master'] == c.commit
    with pytest.raises(PachydermError):
        with c:
            pass
    assert len(adapter.list_commits(repo)) == 1

    with pytest.raises(OSError):
        with PachydermCommitAdapter(adapter, repo) as c:
            raise OSError
    assert len(adapter.list_commits(repo)) == 1


def test_commit_put_file_bytes(adapter: PachydermAdapter, repo):
    skip_if_pachyderm_unavailable(adapter)

    with PachydermCommitAdapter(adapter, repo) as c:
        c.put_file_bytes('test_1', '/test_file_1')
        c.put_file_bytes('test_2', '/folder/test_file_2')
        c.put_file_bytes(b'test_3', 'test_file_3')
        c.put_file_bytes(b'test_4', 'test_file_4')
        assert set(c._list_file_paths('test*')) == {'/test_file_1', '/test_file_3', '/test_file_4'}
        c.delete_file('test_file_4')
        assert set(c._list_file_paths('test*')) == {'/test_file_1', '/test_file_3'}
        assert len(c._list_file_paths('**')) == 4
    files = adapter.list_files(repo, commit=c.commit)
    assert len(files) == 4
    assert '/test_file_1' in set(files.path)
    assert '/folder/test_file_2' in set(files.path)
    assert '/test_file_3' in set(files.path)
    assert '/test_file_4' not in set(files.path)

    with PachydermCommitAdapter(adapter, repo, branch='test') as c:
        c.put_file_bytes('test_5', '/test_file_5')
    files = adapter.list_files(repo, branch='test')
    assert len(files) == 1
    assert '/test_file_5' in set(files.path)
    assert adapter.list_branch_heads(repo)['test'] == c.commit

    commits = adapter.list_commits(repo)
    commits_branches = set(commits.branches.apply(', '.join))
    assert len(commits) == 2
    assert 'master' in commits_branches and 'test' in commits_branches
    assert c.commit in set(commits.commit)

    adapter.delete_commit(repo, c.commit)
    assert c.commit not in set(adapter.list_commits(repo).commit)

    adapter.delete_branch(repo, 'test')
    assert 'test' not in adapter.list_branch_heads(repo)


def test_commit_create_branch(adapter: PachydermAdapter, repo):
    skip_if_pachyderm_unavailable(adapter)
    with PachydermCommitAdapter(adapter, repo) as c:
        c.create_branch('test_branch_1')
    adapter.create_branch(repo, c.commit, 'test_branch_2')
    branch_heads = adapter.list_branch_heads(repo)
    assert 'test_branch_1' in branch_heads and 'test_branch_2' in branch_heads
    assert branch_heads['test_branch_1'] == branch_heads['master']
    assert branch_heads['test_branch_2'] == branch_heads['master']


def test_commit_put_file_url(adapter: PachydermAdapter, repo):
    skip_if_pachyderm_unavailable(adapter)
    with PachydermCommitAdapter(adapter, repo, branch=None) as c:
        c.put_file_url('https://raw.githubusercontent.com/itssimon/pachypy/master/tests/mock/get_logs.csv', 'get_logs.csv')
    files = adapter.list_files(repo, commit=c.commit)
    commits = adapter.list_commits(repo)
    assert len(files) == 1 and len(commits) == 1
    assert '/get_logs.csv' in set(files.path)
    assert c.commit in set(commits.commit)
    assert len(adapter.list_branch_heads(repo)) == 0


def test_commit_flush(adapter: PachydermAdapter, pipeline_5):
    skip_if_pachyderm_unavailable(adapter)
    pipeline = pipeline_5['pipeline']['name']
    repo = pipeline_5['input']['pfs']['repo']
    assert await_pipeline_new_state(adapter, pipeline) == 'running'
    with PachydermCommitAdapter(adapter, repo, flush=True) as c:
        c.put_file_bytes(b'a b c d e f g h i j\n', 'file1')
        c.put_file_bytes(b'k l m n o p q r s t\n', 'file2')
        t = time.time()
    assert (time.time() - t) > 5
    res = b''.join([c for c in adapter.get_file(pipeline, 'cnt')]).decode('utf-8')
    assert int(res) == 20


def test_list_commits_files(adapter: PachydermAdapter, repo):
    skip_if_pachyderm_unavailable(adapter)

    assert len(adapter.list_branch_heads(repo)) == 0
    assert len(adapter.list_files(repo)) == 0

    for _ in range(3):
        with PachydermCommitAdapter(adapter, repo) as c:
            c.put_file_bytes(b'test', 'test')
        with PachydermCommitAdapter(adapter, repo, branch='test') as c:
            c.put_file_bytes(b'test', 'test')

    branch_heads = adapter.list_branch_heads(repo)
    assert 'master' in branch_heads and 'test' in branch_heads

    commits = adapter.list_commits(repo, n=3)
    assert len(commits) == 3
    assert commits['repo'].iloc[0] == repo
    assert commits['size_bytes'].iloc[0] == 12
    assert commits['branches'].iloc[0] == ['test']
    assert commits['branches'].iloc[1] == ['master']
    assert commits['branches'].iloc[2] == []

    files = adapter.list_files(repo, branch='master')
    assert len(files) == 1
    assert files['repo'].iloc[0] == repo
    assert files['type'].iloc[0] == 'file'
    assert files['size_bytes'].iloc[0] == 12

    files = adapter.list_files(repo, branch='test')
    assert len(files) == 1

    with pytest.raises(ValueError):
        adapter.list_files(repo, branch=None, commit=None)


def test_list_jobs_get_logs(adapter: PachydermAdapter, pipeline_2):
    skip_if_pachyderm_unavailable(adapter)

    pipeline_name = pipeline_2['pipeline']['name']
    tick_repo_name = pipeline_name + '_' + pipeline_2['input']['cron']['name']
    assert await_pipeline_new_state(adapter, pipeline_name, initial_state='starting') == 'running'

    with PachydermCommitAdapter(adapter, tick_repo_name) as c:
        c.put_file_bytes(b'0', 'time')
    assert await_job_completed_state(adapter, pipeline_name) == 'success'

    jobs = adapter.list_jobs(pipeline=pipeline_name)
    assert len(jobs) == 1
    job = jobs['job'].iloc[0]
    assert (jobs['finished'] - jobs['started']).dt.total_seconds().round().iloc[0] > -10
    assert jobs['data_processed'].iloc[0] == jobs['data_total'].iloc[0] == 1
    assert jobs['data_skipped'].iloc[0] == 0

    datums = adapter.list_datums(job=job)
    assert len(datums) == 1
    assert datums['job'].iloc[0] == job
    assert datums['repo'].iloc[0] == tick_repo_name
    assert datums['size_bytes'].iloc[0] > 0

    logs = adapter.get_logs(pipeline=pipeline_name)
    logs = logs[logs['user']]
    assert logs.shape == (1, 7)
    assert logs['message'].iloc[0] == 'test'

    adapter.delete_job(job)
    assert len(adapter.list_jobs(pipeline=pipeline_name)) == 0


def test_get_file(adapter: PachydermAdapter, repo):
    skip_if_pachyderm_unavailable(adapter)
    with PachydermCommitAdapter(adapter, repo) as c:
        c.put_file_bytes(b'123', '/test_file_1')
    with PachydermCommitAdapter(adapter, repo, branch='test') as c:
        c.put_file_bytes(b'321', '/test_file_2')
    assert next(adapter.get_file(repo, '/test_file_1')) == b'123'
    assert next(adapter.get_file(repo, '/test_file_2', branch='test')) == b'321'
    assert next(adapter.get_file(repo, '/test_file_2', commit=c.commit)) == b'321'


def test_pipeline_input_cron_specs(adapter: PachydermAdapter, pipeline_5, pipeline_6):
    assert len(adapter.get_pipeline_cron_specs(pipeline_5['pipeline']['name'])) == 0
    assert len(adapter.get_pipeline_cron_specs(pipeline_6['pipeline']['name'])) == 2
