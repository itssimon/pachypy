#!/bin/bash

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

if ! command -v minikube >/dev/null 2>&1; then
    echo -e "${RED}minikube is not available. Exiting.${NC}"
    exit
fi

if ! command -v kubectl >/dev/null 2>&1; then
    echo -e "${RED}kubectl is not available. Exiting.${NC}"
    exit
fi

if ! command -v pachctl >/dev/null 2>&1; then
    echo -e "${RED}pachctl is not available. Exiting.${NC}"
    exit
fi

if ! minikube status | grep -q "Running"; then
    echo -e "${YELLOW}Starting Minikube cluster...${NC}"
    minikube start
else
    echo -e "${GREEN}Minikube is already running${NC}"
fi

export PACHD_ADDRESS=$(minikube ip):30650

if ! kubectl config current-context | grep -q "minikube"; then
    echo -e "${YELLOW}Setting kubectl context to minikube...${NC}"
    kubectl config use-context minikube
fi

if ! kubectl wait --for=condition=available --timeout=600s deployment/pachd > /dev/null; then
    echo -e "${YELLOW}Deploying Pachyderm...${NC}"
    pachctl deploy local --no-guaranteed --no-dashboard
    kubectl wait --for=condition=available --timeout=600s deployment/pachd
else
    echo -e "${GREEN}Pachyderm is already deployed${NC}"
fi

EXITMSG="\n${GREEN}Done testing? You can delete the Minikube cluster by running 'minikube delete'!${NC}"
trap "echo -e \"${EXITMSG}\"; exit" SIGHUP SIGINT SIGTERM

echo -e "${YELLOW}Forwarding ports... (hit Ctrl-C to stop)${NC}"
pachctl port-forward
