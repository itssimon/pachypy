#!/bin/bash
# Run this using dot-space syntax, like `. dep[oy_pachyderm.sh`

minikube start
export PACHD_ADDRESS=$(minikube ip):30650
export ADDRESS=$(minikube ip):30650
pachctl deploy local --no-guaranteed --no-dashboard