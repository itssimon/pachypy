def test_init():
    from pachypy.client import PachydermClient
    from pachypy.registry import DockerRegistry, AmazonECRRegistry
    assert isinstance(PachydermClient(container_registry='docker').container_registry, DockerRegistry)
    assert isinstance(PachydermClient(container_registry='ecr').container_registry, AmazonECRRegistry)
    assert isinstance(PachydermClient(container_registry='registry.example.com').container_registry, DockerRegistry)
    assert PachydermClient(container_registry=None).container_registry is None


def test_split_image_string():
    from pachypy.client import _split_image_string
    assert _split_image_string('a.b/c:d@sha1:e') == ('a.b/c', 'd', 'sha1:e')
    assert _split_image_string('a.b/c:d') == ('a.b/c', 'd', None)
    assert _split_image_string('a.b/c') == ('a.b/c', None, None)


def test_wildcard_filter():
    from pachypy.client import _wildcard_filter
    x = ['a', 'ab', 'b']
    assert _wildcard_filter(x, '*') == set(x)
    assert _wildcard_filter(x, None) == set(x)
    assert _wildcard_filter(x, 'a') == {'a'}
    assert _wildcard_filter(x, ['a']) == {'a'}
    assert _wildcard_filter(x, [['a']]) == {'a'}
    assert _wildcard_filter(x, 'a*') == {'a', 'ab'}
    assert _wildcard_filter(x, 'a?') == {'ab'}
    assert _wildcard_filter(x, [['a*'], 'b']) == set(x)
    assert _wildcard_filter(x, ['*a', '*b']) == set(x)
    assert _wildcard_filter(x, ['a', 'b']) == {'a', 'b'}
