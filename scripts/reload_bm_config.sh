#!/bin/bash

container="local_gossip_nodes-block_manager-1"

if docker ps --format '{{.Names}}' | grep -q "^$container$"; then
    echo "[$container] processing"

    pid=$(docker exec "$container" pgrep -f "^block-manager$")

    if [[ -n "$pid" ]]; then
        docker exec "$container" kill -SIGUSR1 "$pid"
        echo "[$container] SIGUSR1 sent to $pid"
    else
        echo "[$container] block-manager process not found"
    fi
else
    echo "[$container] not found"
fi
