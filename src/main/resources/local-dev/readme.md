# How to use "docker compose" for this project

*All commands in the following section should be run in the current folder (local-dev)*

## What is being deployed

* Kafka
* Kafdrop - https://localhost:9000
* PostgreSQL
* PGAdmin - https://localhost:5480

## Updating a image version

1. Edit version name in etc postgres/docker-compose.yaml

    docker compose up -d

## Stopping running solution

    # Stop running instances, but keep data / volume
    docker compose down

## Removing volume, either including stopping containers

    docker compose down -v

## Using Kafka / Kafdrop locally from some other local installation



## NOTE Using separate directories will create separate volumes

Entering a folder like f.eks. "postgres", and running "docker compose up -d", will create a totally separate database
