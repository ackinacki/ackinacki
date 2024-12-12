help:
    just --list

stop:
    #!/bin/bash
    set -ex
    export COMPOSE_FILE="./docker/lite-gossip-3.compose.yaml"
    docker compose stop

litenode: stop
    #!/bin/bash
    set -ex
    export COMPOSE_FILE="./docker/lite-gossip-3.compose.yaml"
    docker compose build
    docker compose up -d

logs:
    #!/bin/bash
    set -ex
    export COMPOSE_FILE="./docker/lite-gossip-3.compose.yaml"
    docker compose logs -f -n 100 litenode

# runs `docker compose {{task}}` with litenode's compose file
litedc +TASK:
    #!/bin/bash
    set -ex
    export COMPOSE_FILE="./docker/lite-gossip-3.compose.yaml"
    docker compose {{TASK}}
