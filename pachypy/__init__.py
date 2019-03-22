# flake8: noqa

try:
    from IPython import get_ipython
    notebook_mode = get_ipython().__class__.__name__ == 'ZMQInteractiveShell'
    del get_ipython
except ModuleNotFoundError:
    notebook_mode = False

if notebook_mode:
    from .pretty import PrettyPachydermClient as PachydermClient
else:
    from .client import PachydermClient

from .registry import DockerRegistry, AmazonECRRegistry

del notebook_mode
