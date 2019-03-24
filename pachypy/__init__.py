# flake8: noqa

from .client import PachydermClient
from .registry import DockerRegistry, AmazonECRRegistry

try:
    from IPython import get_ipython
    if get_ipython().__class__.__name__ == 'ZMQInteractiveShell':
        from .pretty import PrettyPachydermClient
    del get_ipython
except ModuleNotFoundError:
    pass
