# pachypy - A Python client library for Pachyderm

[![Python 3.5](https://img.shields.io/badge/python-3.5+-blue.svg)](#)
[![Documentation Status](https://readthedocs.org/projects/pachypy/badge/?version=latest)](https://pachypy.readthedocs.io/en/latest/?badge=latest)
[![Coverage Status](https://img.shields.io/codecov/c/github/itssimon/pachypy.svg)](https://codecov.io/gh/itssimon/pachypy)
[![Codacy Badge](https://img.shields.io/codacy/grade/889241976fca40a18591be7db43698fe.svg)](https://app.codacy.com/app/itssimon/pachypy)
[![Requirements Status](https://requires.io/github/itssimon/pachypy/requirements.svg?branch=master)](https://requires.io/github/itssimon/pachypy/requirements/?branch=master)
[![Stability Status](https://img.shields.io/badge/stability-alpha-yellow.svg)](#)
[![Gitter](https://badges.gitter.im/pachypy/community.svg)](https://gitter.im/pachypy/community)

This package aims to make interactions with [Pachyderm](https://github.com/pachyderm/pachyderm) more pythonic and user-friendly.

It is primarily intended to be used interactively in a Jupyter notebook environment.

Key features include:

- Show Pachyderm objects (such as repositories, pipelines, jobs, etc.) as nicely formatted tables, backed by [pandas](https://github.com/pandas-dev/pandas) DataFrames
- Batch operations using shell-style wildcards on Pachyderm objects
- Create and update pipelines from specifications in YAML format, supporting multiple pipelines per file
- Build and push Docker images before creating and updating pipelines
- Add image digests when creating and updating pipelines to ensure the latest images are used in pipelines without requiring a tag change

pachypy uses the protobufs from the official [python_pachyderm](https://github.com/pachyderm/python-pachyderm) package to communicate with Pachyderm.

*Requires Python 3.5 or higher*

## Installation

```bash
pip install pachypy
```

## Usage examples

- [Example notebook](https://github.com/itssimon/pachypy/blob/master/examples/usage.ipynb)
- [Example pipeline specifications in YAML format](https://github.com/itssimon/pachypy/blob/master/examples/pipelines.yaml)

## Getting help

Let's [chat on Gitter](https://gitter.im/pachypy/community) or open an issue on GitHub.