# Contributing to pachypy

## Report issues

Please submit an issue on GitHub and I will be happy to help out.

## Submit pull requests

Pull requests for bugfixes and new features are always welcome! Please also add tests for any new functionality.

## Run tests

In order to run the full suite of tests, a running Pachyderm instance is required. The easiest way to achieve that is to deploy it on a local Minikube cluster using `pachctl`.

Example: To install `minikube` and `pachctl` on macOS via `brew` run:

```bash
brew cask install virtualbox minikube
brew tap pachyderm/tap && brew install pachyderm/tap/pachctl@1.8
```

A script is provided to start a local Minikube cluster and deploy Pachyderm on it. You also need to forward ports so that tests can connect to it.

```bash
. tests/deploy_pachyderm.sh
pachctl port-forward
```

To run tests do:

```bash
make test
```

If Pachyderm is not currently available, the tests relying on it will be skipped automatically.

The Minikube cluster can subsequently be deleted by running:

```bash
minikube delete
```
