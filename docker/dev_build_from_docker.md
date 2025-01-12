# Use Docker Image

We provide docker images of different operating systems to support compilation and development.including

- ubuntu18.04
- ubuntu22.04
- centos7
- centos8
- rocky8.8
- rocky9.2

## Build docker image

For different operating systems, we can refer to the following commands to build

``````
cd dingo-store 

docker build docker/ubuntu18/  -t dingo-store-ubuntu-18.04-dev

docker build docker/ubuntu22/  -t dingo-store-ubuntu-22.04-dev:eureka

docker build docker/centos7/  -t dingo-store-centos-7-dev

docker build docker/centos8/  -t dingo-store-centos-8-dev

docker build docker/rocky8/  -t dingo-store-rocky-8-dev:eureka

docker build docker/rocky9/  -t dingo-store-rocky-9-dev:eureka
``````

If you want to start quickly, we can refer to the following commands to pull container

``````
docker pull dingodatabase/dingo-store-ubuntu-18.04-dev

docker pull dingodatabase/dingo-store-ubuntu-22.04-dev:eureka

docker pull dingodatabase/dingo-store-centos-7-dev

docker pull dingodatabase/dingo-store-centos-8-dev

docker pull dingodatabase/dingo-store-rocky-8-dev:eureka

docker pull dingodatabase/dingo-store-rocky-9-dev:eureka
``````

## Use docker container

After building the docker image succeed, we can use docker run to launch the docker service. rocky9 docker image for example:

``````
docker run  -it dingo-store-rocky-9-dev:eureka
``````

