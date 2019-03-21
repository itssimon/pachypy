import os
import pytest
from unittest import mock


@pytest.fixture(scope='module')
def pachyderm_wrapper():
    from pachypy.base import PachydermWrapper
    return PachydermWrapper()


@mock.patch.dict(os.environ, {'PACHD_ADDRESS': 'test_host:12345'})
def test_pachyderm_wrapper_init_env():
    from pachypy.base import PachydermWrapper
    pachyderm_wrapper = PachydermWrapper()
    channel_target = pachyderm_wrapper.pps_client.channel._channel.target().decode()
    assert channel_target == 'test_host:12345'


def test_pachyderm_wrapper_init():
    from pachypy.base import PachydermWrapper
    pachyderm_wrapper = PachydermWrapper(host='test_host')
    channel_target = pachyderm_wrapper.pps_client.channel._channel.target().decode()
    assert channel_target == 'test_host:30650'


def test_pachyderm_wrapper_check_connectivity():
    from pachypy.base import PachydermWrapper
    pachyderm_wrapper = PachydermWrapper(host='host_that_does_not_exist')
    assert pachyderm_wrapper.check_connectivity() is False
    pachyderm_wrapper = PachydermWrapper(host='google.com')
    assert pachyderm_wrapper.check_connectivity() is False


def test_pachyderm_wrapper_create_update_delete_pipeline(pachyderm_wrapper):
    if not pachyderm_wrapper.check_connectivity():
        pytest.skip('Pachyderm cluster is not available')
    try:
        pachyderm_wrapper._delete_pipeline('test_pipeline')
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
    pachyderm_wrapper._create_pipeline(pipeline_spec)
    pipelines = pachyderm_wrapper._list_pipelines()
    assert len(pipelines) > 0 and 'test_pipeline' in set(pipelines.pipeline)
    assert pipelines.loc[pipelines.pipeline == 'test_pipeline', 'image'].iloc[0] == 'alpine:latest'
    assert pipelines.loc[pipelines.pipeline == 'test_pipeline', 'cron_input'].iloc[0]

    repos = pachyderm_wrapper._list_repos()
    assert len(repos) > 0 and 'test_pipeline' in set(repos.repo)

    pipeline_spec['transform']['image'] = 'alpine:edge'
    pachyderm_wrapper._update_pipeline(pipeline_spec)
    pipelines = pachyderm_wrapper._list_pipelines()
    assert pipelines.loc[pipelines.pipeline == 'test_pipeline', 'image'].iloc[0] == 'alpine:edge'

    pachyderm_wrapper._delete_pipeline('test_pipeline')
    pipelines = pachyderm_wrapper._list_pipelines()
    assert len(pipelines) == 0 or 'test_pipeline' not in set(pipelines.pipeline)


def test_pachyderm_wrapper_delete_pipeline_exception(pachyderm_wrapper):
    from pachypy.base import PachydermException
    with pytest.raises(PachydermException):
        pachyderm_wrapper._delete_pipeline('pipeline_that_does_not_exist')
