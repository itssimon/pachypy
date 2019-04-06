import pytest


@pytest.fixture(scope='module')
def docker_registry():
    from docker import DockerClient
    from pachypy.registry import DockerRegistryAdapter
    return DockerRegistryAdapter(DockerClient())


def test_docker_get_image_digest(docker_registry):
    digest = docker_registry.get_image_digest('alpine:latest')
    assert digest.startswith('sha256:')


def test_split_image_string():
    from pachypy.registry import AmazonECRAdapter
    assert AmazonECRAdapter._split_image_string('a.b/c:d@sha1:e') == ('a.b', 'c', 'd')
    assert AmazonECRAdapter._split_image_string('a.b/c:d') == ('a.b', 'c', 'd')
    assert AmazonECRAdapter._split_image_string('a.b/c') == ('a.b', 'c', None)
    assert AmazonECRAdapter._split_image_string('a.b/c/d') == ('a.b', 'c/d', None)
    assert AmazonECRAdapter._split_image_string('a.b/c/d:e') == ('a.b', 'c/d', 'e')
    assert AmazonECRAdapter._split_image_string('a/b') == ('docker.io', 'a/b', None)
    assert AmazonECRAdapter._split_image_string('a/b:c') == ('docker.io', 'a/b', 'c')
    assert AmazonECRAdapter._split_image_string('a:b') == ('docker.io', 'a', 'b')
