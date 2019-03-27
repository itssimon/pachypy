# Contributing to pachypy

## Report issues

Please submit an issue on GitHub and I will be happy to help out.

## Submit pull requests

Pull requests for bugfixes and new features are always welcome! Please also add tests for any new functionality.

## Prerequisites for testing

In order to run the full suite of tests, a running Pachyderm instance is required. A shell script is provided that automates the process of setting things up locally:

- Uses a Kubernetes on Docker Desktop cluster or Minikube and sets the `kubectl` context accordingly
- If using Minikube, starts a Minikube cluster if not currently running
- Deploys Pachyderm using `pachctl deploy local` if not already deployed and waits for the deployment to complete
- Forwards Pachyderm ports using `pachctl port-forward`
- Provides teardown instructions

```bash
helper/deploy_pachyderm.sh
```

The script requires `kubectl`, `pachctl` and `minikube` (if not using Kubernetes on Docker Desktop) to be available.

## Run tests

```bash
make test
```

If Pachyderm is not currently available, the tests relying on it will be skipped automatically.
