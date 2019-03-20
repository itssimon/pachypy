__all__ = [
    'DockerRegistry', 'AmazonECRRegistry'
]

import os
import json
from functools import lru_cache
from typing import Optional


class RegistryAuthorizationException(Exception):
    pass


class RegistryImageNotFoundException(Exception):
    pass


class ContainerRegistry:

    """Container registry handler base class."""

    @lru_cache()
    def get_image_digest(self, repository: str, tag: str) -> str:
        """Retrieve the latest image digest.

        Args:
            repository: Repository.
            tag: Tag.
        """
        raise NotImplementedError

    def clear_cache(self) -> None:
        """Clear image digest cache."""
        self.get_image_digest.cache_clear()


class DockerRegistry(ContainerRegistry):

    """Docker registry handler.

    Args:
        registry_host: Hostname of Docker registry. Defaults to Docker Hub.
        auth: Docker auth token. Will try to read this from ``~/.docker/config.json`` if not specified. Run ``docker login`` before relying on it.
    """

    def __init__(self, registry_host: str = 'index.docker.io', auth: Optional[str] = None):
        self.registry_host = registry_host
        self.auth = auth
        if self.auth is None:
            self.auth = self.load_auth_from_file()

    @lru_cache()
    def get_image_digest(self, repository: str, tag: str) -> str:
        from dxf import DXF
        from dxf.exceptions import DXFUnauthorizedError
        try:
            repository = f'library/{repository}' if '/' not in repository else repository
            auth = 'Basic ' + self.auth if self.auth is not None else None
            dxf = DXF(self.registry_host, repo=repository)
            dxf.authenticate(authorization=auth, actions=['pull'])
        except DXFUnauthorizedError:
            raise RegistryAuthorizationException(f'Authentication with Docker registry {self.index_url} failed. Run `docker login` first?')
        try:
            manifest = json.loads(dxf.get_manifest(tag))
            return manifest['config']['digest']
        except DXFUnauthorizedError:
            raise RegistryImageNotFoundException(f'Image {repository}:{tag} not found in registry {self.index_url}')

    def load_auth_from_file(self, file: str = '~/.docker/config.json') -> str:
        try:
            with open(os.path.expanduser(file)) as config_file:
                data = json.load(config_file)
        except (FileNotFoundError, json.JSONDecodeError):
            return None
        if 'credsStore' in data:
            raise NotImplementedError('Credentials store not currently supported')
        else:
            hub_index = 'https://' + self.registry_host + '/v1/'
            return data.get('auths', {}).get(hub_index, {}).get('auth', None)


class AmazonECRRegistry(ContainerRegistry):

    """Amazon Elastic Container Registry (ECR) handler using boto3.

    Args:
        aws_access_key_id: AWS access key ID.
        aws_secret_access_key: AWS secret access key.
    """

    def __init__(self, aws_access_key_id: Optional[str] = None, aws_secret_access_key: Optional[str] = None):
        import boto3
        self.ecr_client = boto3.client('ecr', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

    @lru_cache()
    def _get_image_digest_from_ecr(self, repository: str, tag: str) -> str:
        try:
            res = self.ecr_client.batch_get_image(imageIds=[{'imageTag': tag}], repositoryName=repository)
            return res['images'][0]['imageId']['imageDigest']
        except KeyError:
            raise RegistryImageNotFoundException(f'Image {repository}:{tag} not found in Amazon ECR')
