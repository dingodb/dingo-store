# Use Docker Image

We provide docker images of different operating systems to support compilation and development.including

- ubuntu18.04
- ubuntu22.04
- centos7
- centos8
- rocky8.6

## Build docker image

For different operating systems, we can refer to the following commands to build

``````
cd dingo-store 

docker build docker/ubuntu18/  -t u18-dingo-dev

docker build docker/ubuntu22/  -t u22-dingo-dev

docker build docker/centos7/  -t centos7-dingo-dev

docker build docker/centos8/  -t centos8-dingo-dev

docker build docker/rocky86/  -t rocky86-dingo-dev
``````

If you want to start quickly, we can refer to the following commands to pull container

``````
docker pull dingodatabase/u18-dingo-dev

docker pull dingodatabase/u22-dingo-dev

docker pull dingodatabase/centos7-dingo-dev

docker pull dingodatabase/centos8-dingo-dev

docker pull dingodatabase/rocky86-dingo-dev
``````

## Use docker container

After building the docker image succeed, we can use docker run to launch the docker service. centos8 docker image for example:

``````
docker run  -it centos8-dingo-dev
``````

