# How to use "docker compose" for this project

*All commands in the following section should be run in the current folder (local-dev)*

## What is being deployed

* Kafka
* Kafdrop - https://localhost:9000
* PostgreSQL
* PGAdmin - https://localhost:5480

## Files

* **docker-compose.yaml** - Main docker compose file
* **kafka.yaml** - kafka / kafdrop
* **postgres.yaml** - postgresql / pgadmin

## Updating a image version

*Note: The image versions are pinned. Version pinning is important to avoid potential security issues.*

1. Edit version string in etc postgres.yaml. 

| Operation   | data                    |
|:------------|:------------------------|
| Change from | `image: postgres:17.9`  |
| Change to   | `image: postgres:17.10` |

2. Re-run the following command, to make docker download and deploy the new image.

    
    docker compose up -d

## Stopping running solution

    # Stop running instances, but keep data / volume
    docker compose down

## Removing volume, either including stopping containers

    docker compose down -v
