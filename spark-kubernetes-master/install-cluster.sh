#!/bin/bash
## Config memory and CPUs from local resources
minikube start --memory 8192 --cpus 4
## Create docker spark image
docker build -f docker/Dockerfile -t spark-hadoop:3.2.0 ./docker
## Deploy master
kubectl create -f ./kubernetes/spark-master-deployment.yaml
kubectl create -f ./kubernetes/spark-master-service.yaml
kubectl get deployments
## Deploy worker
kubectl create -f ./kubernetes/spark-worker-deployment.yaml
kubectl get pods
## add ingress
minikube addons enable ingress
kubectl apply -f ./kubernetes/minikube-ingress.yaml
echo "$(minikube ip) spark-kubernetes"
## get nodes to connect master
kubectl get pods -o wide
## Create dashboard
minikube dashboard
minikube dashboard -url 
