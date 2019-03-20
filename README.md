# pachypy - A Python client library and alternative CLI for Pachyderm

This package aims to make interactions with a [Pachyderm](https://www.pachyderm.io) cluster more efficient and user-friendly.

It is primarily intended to be used in a Jupyter notebook environment. A command-line interface is also provided.

Current functionality includes:

- Get info about repos, pipelines and jobs as (styled) pandas DataFrames
- Retrieve and pretty print logs
- Create, update and delete pipelines in batch using shell-style wildcards
- Read pipeline specs from YAML files, supporting multiple pipelines per file
- Automatically add image digests when creating/updating pipelines to ensure Kubernetes pulls the latest version of images used in pipelines
- Custom transformation of pipeline specs (e.g. programmatically add fields) before creating/updating pipelines

## Installation

pachpy relies internally on the official [python_pachyderm](https://github.com/pachyderm/python-pachyderm) package, which for the moment is best installed directly from GitHub.

```bash
pip install --upgrade git+https://github.com/pachyderm/python-pachyderm.git
pip install git+https://github.com/itssimon/pachypy.git
```

## Basic usage

### Initialise client

```python
from pachypy import PachydermClient

pachy = PachydermClient(host='localhost')
```

### Get info about repos, pipelines and jobs

```python
# Get list of repos
pachy.list_repo()

# Get list of pipelines
pachy.list_pipeline()

# Get list of jobs
pachy.list_job()

# Get list of jobs for pipelines starting with 'test'
pachy.list_job('test*')
```

### Get logs

```python
# Print logs of last job of pipelines starting with 'test'
pachy.get_logs('test*')

# Print logs of all jobs of pipelines starting with 'test'
pachy.get_logs('test*', last_job_only=False)
```

### Create, update and delete pipelines

```python
```
