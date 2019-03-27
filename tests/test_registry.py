import pytest
from unittest.mock import patch, mock_open, Mock


@pytest.fixture(scope='module')
def docker_registry():
    from pachypy.registry import DockerRegistryAdapter
    return DockerRegistryAdapter(registry_host='index.docker.io')


def test_docker_load_auth_from_file(docker_registry):
    import json
    from base64 import b64encode
    auth1 = b64encode(b'username1:password1').decode('utf-8')
    auth2 = b64encode(b'username2:password2').decode('utf-8')
    config_data = json.dumps({'auths': {docker_registry.registry_url: {'auth': auth1}}})
    config_data_cred_store = json.dumps({'auths': {}, 'credsStore': 'test'})
    cred_store_data = json.dumps({'Username': 'username2', 'Secret': 'password2'})
    with patch('pachypy.registry.open', mock_open(read_data=config_data), create=True):
        assert docker_registry.load_auth_from_file() == auth1
    with patch('pachypy.registry.open', mock_open(read_data=config_data_cred_store), create=True):
        with patch('subprocess.Popen') as popen_mock:
            popen_mock.return_value = Mock()
            popen_mock.return_value.configure_mock(**{'communicate.return_value': (cred_store_data, '')})
            assert docker_registry.load_auth_from_file() == auth2


def test_docker_load_auth_cred_store(docker_registry):
    with pytest.raises(ValueError):
        docker_registry.load_auth_from_cred_store('!@#$%')
    with pytest.raises(RuntimeError):
        docker_registry.load_auth_from_cred_store('non-existing-cred-store')


def test_docker_get_image_digest(docker_registry):
    digest = docker_registry.get_image_digest('alpine', 'latest')
    assert digest.startswith('sha256:')
    assert docker_registry.get_image_digest.cache_info().currsize > 0
    docker_registry.clear_cache()
    assert docker_registry.get_image_digest.cache_info().currsize == 0


def test_docker_get_image_digest_exception1(docker_registry):
    from pachypy.registry import RegistryImageNotFoundException
    with pytest.raises(RegistryImageNotFoundException):
        docker_registry.get_image_digest('repository/that_doesnt_exist', 'latest')


def test_docker_get_image_digest_exception2():
    from pachypy.registry import DockerRegistryAdapter, RegistryAuthorizationException
    with pytest.raises(RegistryAuthorizationException):
        docker_registry = DockerRegistryAdapter(registry_host='index.docker.io', auth='Zm9vOmJhcg==')
        docker_registry.get_image_digest('repository/that_doesnt_exist', 'latest')
