# pachypy - A Python client library and CLI for Pachyderm

[![Python 3.5](https://img.shields.io/badge/python-3.5+-blue.svg)](#)
[![Documentation Status](https://readthedocs.org/projects/pachypy/badge/?version=latest)](https://pachypy.readthedocs.io/en/latest/?badge=latest)
[![Coverage Status](https://raw.githubusercontent.com/itssimon/pachypy/master/coverage.svg?sanitize=true)](#)
[![Stability Experimental](https://img.shields.io/badge/stability-alpha-yellow.svg)](#)
[![Requirements Status](https://requires.io/github/itssimon/pachypy/requirements.svg?branch=master)](https://requires.io/github/itssimon/pachypy/requirements/?branch=master)

This package aims to make interactions with a [Pachyderm](https://github.com/pachyderm/pachyderm) cluster more efficient and user-friendly.

It is primarily intended to be used in a Jupyter notebook environment. A command-line interface is also provided.

Current functionality includes:

- Get info about repos, pipelines and jobs as (styled) pandas DataFrames
- Retrieve and pretty print logs
- Create, update and delete pipelines in batch using shell-style wildcards
- Read pipeline specs from YAML files, supporting multiple pipelines per file
- Automatically add image digests when creating/updating pipelines to ensure Kubernetes pulls the latest version of images used in pipelines
- Custom transformation of pipeline specs (e.g. programmatically add fields) before creating/updating pipelines

pachypy builds on top of the official [python_pachyderm](https://github.com/pachyderm/python-pachyderm) package.

*Requires Python 3.5 or higher*

## Installation

```bash
pip install git+https://github.com/itssimon/pachypy.git
```

pachypy will be pushed to PyPI and conda-forge soon.

## Usage in notebook

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
# Print logs for last job of pipelines starting with 'test'
pachy.get_logs('test*')

# Print logs for all jobs of pipelines starting with 'test'
pachy.get_logs('test*', last_job_only=False)
```

### Create, update and delete pipelines

```python
...
```

*Work in progress...*
