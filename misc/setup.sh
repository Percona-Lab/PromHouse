#!/bin/bash

# setup steps for Ubuntu 16.04

set -ex

sudo apt update
sudo apt upgrade -y
sudo apt install -y mc make apt-transport-https ca-certificates curl software-properties-common

curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
sudo add-apt-repository -u "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"
sudo apt install -y docker-ce
sudo docker --version

sudo curl -L https://github.com/docker/compose/releases/download/1.15.0/docker-compose-`uname -s`-`uname -m` -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose
sudo docker-compose --version

curl -O https://storage.googleapis.com/golang/go1.9.linux-amd64.tar.gz
tar xzf go1.9.linux-amd64.tar.gz
sudo mv go /usr/local/
rm go1.9.linux-amd64.tar.gz
sudo ln -vs /usr/local/go/bin/* /usr/local/bin

go get -u -v github.com/Percona-Lab/PromHouse/cmd/...
