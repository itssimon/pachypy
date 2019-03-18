#!/bin/bash

minikube start
export PACHD_ADDRESS=$(minikube ip):30650
export ADDRESS=$(minikube ip):30650
pachctl deploy local