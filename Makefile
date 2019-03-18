.PHONY: deploy-pachyderm undeploy-pachyderm

deploy-pachyderm:
	minikube start
	export PACHD_ADDRESS = $(minikube ip):30650
	export ADDRESS = $(minikube ip):30650
	pachctl deploy local

undeploy-pachyderm:
	minikube delete