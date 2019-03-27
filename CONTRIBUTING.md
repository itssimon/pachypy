# Contributing to pachypy

## Report issues

Please submit an issue on GitHub and I will be happy to help out.

## Submit pull requests

Pull requests for bugfixes and new features are always welcome! Please also add tests for any new functionality.

## Run tests

In order to run the full suite of tests, a running Pachyderm instance is required. A shell script is provided which starts a local Minikube cluster, deploys Pachyderm on it and forwards ports.

```bash
. tests/deploy_pachyderm.sh
```

The script requires `minkube`, `kubectl` and `pachctl` to be available.

You can install these requiresments on macOS via `brew`:

```bash
brew cask install virtualbox minikube
brew install kubernetes-cli
brew tap pachyderm/tap && brew install pachyderm/tap/pachctl@1.8
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
