# master project: "Recommendation System for emulation environments using OpenSearch"
This projecct implements a search engine instance of opensearch creating a recommendation system for emulation environments. 


## setup:
1. install [Docker](https://docs.docker.com/engine/install/ubuntu/)
2. install [opensearch](https://opensearch.org/docs/latest/opensearch/install/docker/) (v:1.3.1)

## run:
1. start docker opensearch instance.
   sudo docker run -p 9200:9200 -p 9600:9600 -e "discovery.type=single-node" opensearchproject/opensearch:1.3.1

2. run python.
   python recommend_envs.py
