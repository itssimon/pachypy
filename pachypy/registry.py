__all__ = [
    'DockerRegistryAdapter', 'AmazonECRAdapter'
]

import re
from base64 import b64decode
from requests.exceptions import HTTPError
from typing import Tuple, Optional

import boto3
import docker


class RegistryException(Exception):
    pass


class DockerRegistryAdapter:

    """Docker registry adapter."""

    def __init__(self, docker_client: docker.DockerClient):
        self.docker_client = docker_client

    def get_image_digest(self, image: str) -> str:
        # TODO: This can be simplified by using docker.api.APIClient.inspect_distribution()
        # once a fixed version is released.

        api = self.docker_client.api
        registry, _ = docker.auth.resolve_repository_name(image)
        header = docker.auth.get_config_header(api, registry)

        try:
            url = api._url("/distribution/{0}/json", image)
            headers = {'X-Registry-Auth': header} if header else {}
            result = api._result(api._get(url, headers=headers), True)
            return str(result['Descriptor']['digest'])
        except HTTPError:
            raise RegistryException(f"Image '{image}' not found in Docker registry '{registry}'")

    def push_image(self, image) -> str:
        digest = None
        push_stream = self.docker_client.api.push(repository=image, stream=True, decode=True)
        for chunk in push_stream:
            if 'error' in chunk:
                raise RegistryException(chunk['error'])
            if 'aux' in chunk:
                digest = str(chunk['aux']['Digest'])
        if not digest:
            raise RegistryException('did not retrieve image digest from Docker after pushing image')
        return digest


class AmazonECRAdapter(DockerRegistryAdapter):

    """Amazon Elastic Container Registry (ECR) adapter using boto3.

    Getting image digests via boto3 is faster than using the DockerRegistryAdapter,
    which otherwise works fine for ECR too.
    When pushing images, this class also handles logging in to ECR by getting an
    authorization token using boto3 and passing it to Docker to log in.

    Args:
        aws_access_key_id: AWS access key ID.
        aws_secret_access_key: AWS secret access key.
    """

    def __init__(self, docker_client: docker.DockerClient):
        self.docker_client = docker_client
        self.ecr_client = boto3.client('ecr')

    def set_credentials(self, aws_access_key_id: str, aws_secret_access_key: str):
        self.ecr_client = boto3.client('ecr', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

    def login(self, registry: str) -> None:
        response = self.ecr_client.get_authorization_token(registryIds=[registry[:12]])
        auth_token = response['authorizationData'][0]['authorizationToken']
        username, password = b64decode(auth_token).decode('utf-8').split(':')
        self.docker_client.login(username=username, password=password, registry=registry, reauth=True)

    def get_image_digest(self, image: str) -> str:
        registry, repository, tag = self._split_image_string(image)
        if not tag:
            raise ValueError('tag is required for images in Amazon ECR')
        try:
            res = self.ecr_client.batch_get_image(registryId=registry[:12], repositoryName=repository, imageIds=[{'imageTag': tag}])
            return str(res['images'][0]['imageId']['imageDigest'])
        except (self.ecr_client.exceptions.RepositoryNotFoundException, KeyError):
            raise RegistryException(f"Image '{repository}:{tag}' not found in Amazon ECR registry '{registry[:12]}'")

    def push_image(self, image: str) -> str:
        try:
            return super().push_image(image)
        except RegistryException as e:
            if 'denied' in str(e):
                registry = self._split_image_string(image)[0]
                self.login(registry)
                return super().push_image(image)
            else:
                raise e

    @staticmethod
    def _split_image_string(image) -> Tuple[str, Optional[str], Optional[str]]:
        match = re.match('(^[^:@]+)(?::([^:@]+))?(?:@(.+))?$', image)
        if match:
            repository, tag, _ = match.groups()
            registry, repository = docker.auth.resolve_repository_name(repository)
            return registry, repository, tag
        else:
            raise ValueError(f"'{image}' is not a valid image string")
