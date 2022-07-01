#!/bin/bash
## Config memory and CPUs from local resources
minikube start --cpus 4 --memory 8192
# kubectl cluster-info
# kubectl cluster view
kubectl get pods -A
eval $(minikube -p minikube docker-env)
## Point the shell to minikube's Docker daemon.
eval $(minikube docker-env)
## Create docker spark image
docker build -f docker/Dockerfile -t spark:3.3.0 ./docker
kubectl create ns spark
kubectl config set-context --current --namespace=spark
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
## get nodes to connect worker
kubectl get pods -o wide
# kubectl exec spark-worker-fc4d55c87-8rrbd -it -- pyspark 
## --conf spark.driver.bindAddress=172.17.0.9 --conf spark.driver.host=172.17.0.9
## Create dashboard
minikube dashboard
