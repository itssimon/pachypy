#!/bin/bash

NC="\033[0m"
RED="\033[0;31m"
GREEN="\033[0;32m"
YELLOW="\033[0;33m"
BLUE_BOLD="\033[1;34m"
CYAN="\033[0;36m"

REQUIRE=("kubectl" "pachctl")

for c in "${REQUIRE[@]}"; do
    if ! command -v "$c" >/dev/null 2>&1; then
        echo -e "${RED}${c} is not available. Exiting.${NC}"
        exit
    fi
done

if kubectl config get-contexts | grep -q "docker-for-desktop"; then
    CONTEXT="docker-for-desktop"
    echo -e "${BLUE_BOLD}Using Kubernetes on Docker for Desktop...${NC}"

    EXITMSG="\n${CYAN}Done testing? You can reset the local Kubernetes cluster in the Docker Desktop preferences under Reset > Reset Kubernetes cluster!${NC}"
else
    CONTEXT="minikube"
    echo -e "${BLUE_BOLD}Using Minikube...${NC}"

    if ! command -v minikube >/dev/null 2>&1; then
        echo -e "${RED}minikube is not available. Exiting.${NC}"
        exit
    fi

    if ! minikube status | grep -q "Running"; then
        echo -e "${YELLOW}Starting Minikube cluster...${NC}"
        minikube start
    fi

    EXITMSG="\n${CYAN}Done testing? You can delete the Minikube cluster by running 'minikube delete'!${NC}"
fi

if ! kubectl config current-context | grep -q "${CONTEXT}"; then
    echo -e "${YELLOW}Setting kubectl context to ${CONTEXT}...${NC}"
    kubectl config use-context "${CONTEXT}"
fi

if ! kubectl wait --for=condition=available --timeout=600s deployment/pachd > /dev/null 2>&1; then
    echo -e "${YELLOW}Deploying Pachyderm...${NC}"
    pachctl deploy local --no-guaranteed --no-dashboard
    echo -e "${YELLOW}Waiting for Pachyderm to become available...${NC}"
    kubectl wait --for=condition=available --timeout=600s deployment/pachd
else
    echo -e "${GREEN}Pachyderm is already deployed.${NC}"
fi

if ! kubectl config view --minify -o jsonpath="{.clusters[0].cluster.server}" | grep -q "localhost"; then
    echo -e "${YELLOW}Forwarding ports... (hit Ctrl-C to stop)${NC}"
    trap 'echo -e "${EXITMSG}"; exit' SIGHUP SIGINT SIGTERM
    pachctl port-forward
else
    echo -e "${GREEN}Port forwarding not necessary. Cluster is already reachable on localhost.${NC}"
    echo -e "${EXITMSG}"
fi
